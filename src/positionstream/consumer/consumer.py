import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from prometheus_client import Counter
from redis import Redis

from positionstream.consumer.processor import process_raw_message
from positionstream.storage.postgres import PostgresStorage

logger = logging.getLogger(__name__)

RAW_TOPIC = "market.raw"
CLEAN_TOPIC = "market.clean"
DLQ_TOPIC = "market.dlq"

IDEMPOTENCY_PREFIX = "positionstream:processed"
IDEMPOTENCY_TTL_SECONDS = 7 * 24 * 60 * 60

QUALITY_PREFIX = "positionstream:quality"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 0.5

EVENTS_PROCESSED = Counter(
    "positionstream_events_processed_total",
    "Successfully normalized events published to the clean topic.",
    ["event_type"],
)
EVENTS_REJECTED = Counter(
    "positionstream_events_rejected_total",
    "Events routed to the dead-letter topic.",
    ["reason_code"],
)
PROCESSING_ERRORS = Counter(
    "positionstream_processing_errors_total",
    "Unexpected processing and infrastructure errors.",
    ["stage"],
)
BATCHES_PROCESSED = Counter(
    "positionstream_batches_processed_total",
    "Kafka batches successfully committed.",
)


class KafkaBatchConsumer:
    """Consume Kafka batches and persist clean events before committing offsets."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        group_id: str = "positionstream-consumer",
        batch_size: int = 100,
        allowed_lateness_seconds: int = 300,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if allowed_lateness_seconds < 0:
            raise ValueError("allowed_lateness_seconds cannot be negative")

        servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.batch_size = batch_size
        self.allowed_lateness_seconds = allowed_lateness_seconds
        self._stopping = False

        self._consumer = Consumer(
            {
                "bootstrap.servers": servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._producer = Producer(
            {
                "bootstrap.servers": servers,
                "client.id": "positionstream-clean-dlq-producer",
            }
        )
        self._redis: Redis[str] = Redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        self.storage = PostgresStorage(
            os.getenv(
                "POSITIONSTREAM_DATABASE_URL",
                "postgresql://positionstream:local_dev_password@127.0.0.1:5432/positionstream",
            )
        )

        self._delivery_errors: list[str] = []
        self._max_event_time: dict[tuple[str, str], datetime] = {}
        self._consumer.subscribe([RAW_TOPIC])

    def _on_delivery(self, error: KafkaError | None, message: Any) -> None:
        if error is not None:
            self._delivery_errors.append(str(error))
            logger.error("Output delivery failed: %s", error)

    def _publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        payload = json.dumps(value)

        for attempt in range(MAX_RETRIES + 1):
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    callback=self._on_delivery,
                )
                self._producer.poll(0)
                return
            except BufferError:
                if attempt == MAX_RETRIES:
                    raise

                delay = RETRY_BACKOFF_SECONDS * (2**attempt)
                logger.warning(
                    "Kafka producer queue full; retry %d/%d in %.1fs",
                    attempt + 1,
                    MAX_RETRIES,
                    delay,
                )
                self._producer.poll(delay)

        raise RuntimeError("Kafka publish retries exhausted")

    def _deliver_outputs(self, outputs: list[tuple[str, str, dict[str, Any]]]) -> None:
        """Retry output delivery; replaying a batch can produce duplicate outputs."""
        last_error: Exception | None = None

        for attempt in range(MAX_RETRIES + 1):
            self._delivery_errors.clear()

            try:
                for topic, key, value in outputs:
                    self._publish(topic, key, value)

                self._flush_outputs()
                errors = self._delivery_errors.copy()

                if errors:
                    raise RuntimeError(
                        f"Output delivery failed; errors={errors}"
                    )
                return

            except (RuntimeError, KafkaException) as exc:
                last_error = exc
                if attempt == MAX_RETRIES:
                    break

                delay = RETRY_BACKOFF_SECONDS * (2**attempt)
                logger.warning(
                    "Output delivery retry %d/%d in %.1fs: %s",
                    attempt + 1,
                    MAX_RETRIES,
                    delay,
                    exc,
                )
                time.sleep(delay)

        raise RuntimeError(
            "Output delivery retries exhausted; offsets not committed"
        ) from last_error

    @staticmethod
    def _event_timestamp(event: dict[str, Any]) -> datetime:
        value = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        if value.tzinfo is None:
            raise ValueError("Event timestamp must include a timezone")
        return value

    @staticmethod
    def _idempotency_key(event_type: str, event: dict[str, Any]) -> str:
        event_id = event.get("trade_id") or event.get("event_id")
        return f"{IDEMPOTENCY_PREFIX}:{event_type}:{event['source']}:{event_id}"

    def _check_event(
        self,
        event_type: str,
        event: dict[str, Any],
        pending_keys: set[str],
    ) -> tuple[str, str] | None:
        key = self._idempotency_key(event_type, event)

        if key in pending_keys or self._redis.exists(key):
            return "DUPLICATE", f"Duplicate event: {key}"

        timestamp = self._event_timestamp(event)
        watermark_key = (event_type, event["symbol"])
        latest = self._max_event_time.get(watermark_key)

        if (
            latest is not None
            and (latest - timestamp).total_seconds() > self.allowed_lateness_seconds
        ):
            return (
                "LATE",
                f"Event timestamp {timestamp.isoformat()} is too late; "
                f"latest observed timestamp for {event['symbol']} is "
                f"{latest.isoformat()}",
            )

        pending_keys.add(key)

        if latest is None or timestamp > latest:
            self._max_event_time[watermark_key] = timestamp

        return None

    def _publish_dlq(
        self,
        message: Any,
        reason_code: str,
        error: str,
        outputs: list[tuple[str, str, dict[str, Any]]] | None = None,
        quality_deltas: dict[str, int] | None = None,
    ) -> None:
        """Route rejected messages to the batch DLQ or publish standalone."""
        raw_value = message.value()

        try:
            original = json.loads(raw_value)
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
            original = {"raw": repr(raw_value)}

        correlation_id = (
            original.get("correlation_id")
            if isinstance(original, dict)
            else None
        )
        raw_key = message.key()
        output_key = (
            raw_key.decode("utf-8", errors="replace")
            if raw_key
            else "invalid-event"
        )
        payload = {
            "reason_code": reason_code,
            "error": error,
            "correlation_id": correlation_id,
            "original": original,
        }

        if outputs is None and getattr(self, "_processing_batch", False):
            outputs = self._active_dlq_outputs
        if quality_deltas is None and getattr(self, "_processing_batch", False):
            quality_deltas = self._active_quality_deltas

        if outputs is None:
            self._publish(DLQ_TOPIC, output_key, payload)
        else:
            outputs.append((DLQ_TOPIC, output_key, payload))

        EVENTS_REJECTED.labels(reason_code=reason_code).inc()
        if quality_deltas is not None:
            counter = f"rejected_{reason_code.lower()}"
            quality_deltas[counter] = quality_deltas.get(counter, 0) + 1

    def _persist_events(
        self,
        trades: list[dict[str, Any]],
        prices: list[dict[str, Any]],
    ) -> None:
        """Persist trades and prices atomically in one database transaction."""
        if trades or prices:
            self.storage.write_batch(trades=trades, prices=prices)

    def _persist_latest_positions(self, trades: list[dict[str, Any]]) -> None:
        """Refresh cached portfolio positions after portfolio trades are stored."""
        if not any(trade.get("portfolio_id") and trade.get("side") for trade in trades):
            return

        positions = self.storage.fetch_portfolio_positions()
        if not positions:
            return

        with self._redis.pipeline(transaction=True) as pipeline:
            for position in positions:
                key = (
                    f"positionstream:position:{position['portfolio_id']}:"
                    f"{position['symbol']}"
                )
                value = {
                    "portfolio_id": position["portfolio_id"],
                    "symbol": position["symbol"],
                    "quantity": float(position["quantity"]),
                    "market_value": (
                        float(position["market_value"])
                        if position["market_value"] is not None
                        else None
                    ),
                    "quality": position.get("quality") or "OK",
                }
                pipeline.set(key, json.dumps(value))
            pipeline.execute()

    def _persist_redis_state(
        self,
        pending_keys: set[str],
        quality_deltas: dict[str, int],
    ) -> None:
        """Update idempotency keys and quality counters in one Redis transaction."""
        if not pending_keys and not quality_deltas:
            return

        with self._redis.pipeline(transaction=True) as pipeline:
            for key in pending_keys:
                pipeline.set(key, "1", ex=IDEMPOTENCY_TTL_SECONDS)

            for name, amount in quality_deltas.items():
                pipeline.incrby(f"{QUALITY_PREFIX}:{name}", amount)

            pipeline.execute()

    def _cache_latest_positions(self) -> None:
        """Refresh Redis portfolio positions from the PostgreSQL view."""
        storage = getattr(self, "storage", None)
        if storage is None:
            return

        positions = storage.fetch_portfolio_positions()
        with self._redis.pipeline(transaction=True) as pipeline:
            for position in positions:
                key = (
                    f"positionstream:position:"
                    f"{position['portfolio_id']}:{position['symbol']}"
                )
                pipeline.set(key, json.dumps(position))
            pipeline.execute()

    def process_batch(self, timeout: float = 1.0) -> int:
        """Deliver outputs, persist events, update Redis, then commit offsets."""
        messages = self._consumer.consume(
            num_messages=self.batch_size,
            timeout=timeout,
        )

        if not messages:
            return 0

        pending_keys: set[str] = set()
        outputs: list[tuple[str, str, dict[str, Any]]] = []
        trades: list[dict[str, Any]] = []
        prices: list[dict[str, Any]] = []
        quality_deltas: dict[str, int] = {}
        watermark_snapshot = self._max_event_time.copy()
        self._processing_batch = True
        self._active_dlq_outputs = outputs
        self._active_quality_deltas = quality_deltas

        try:
            for message in messages:
                if message.error():
                    raise RuntimeError(f"Kafka consumption failed: {message.error()}")

                result = process_raw_message(message.value())

                if result["status"] == "rejected":
                    self._publish_dlq(
                        message,
                        result["reason_code"],
                        result["error"],
                    )
                    continue

                event = result["event"]
                event_type = result["event_type"]

                try:
                    rejection = self._check_event(
                        event_type,
                        event,
                        pending_keys,
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    rejection = ("SCHEMA_INVALID", str(exc))

                if rejection is not None:
                    reason_code, error = rejection
                    self._publish_dlq(
                        message,
                        reason_code,
                        error,
                        outputs,
                        quality_deltas,
                    )
                    continue

                event_id = event.get("trade_id") or event.get("event_id")
                outputs.append(
                    (
                        CLEAN_TOPIC,
                        str(event_id),
                        {
                            "event_type": event_type,
                            "correlation_id": result["correlation_id"],
                            "event": event,
                        },
                    )
                )

                if event_type == "trade":
                    trades.append(event)
                elif event_type == "price":
                    prices.append(event)
                else:
                    raise ValueError(f"Unsupported event type: {event_type}")

                EVENTS_PROCESSED.labels(event_type=event_type).inc()
                quality_deltas[f"processed_{event_type}"] = (
                    quality_deltas.get(f"processed_{event_type}", 0) + 1
                )

            # Output delivery must succeed before storage work proceeds.
            self._processing_batch = False
            self._deliver_outputs(outputs)

            # PostgreSQL persistence must succeed before Redis keys or offsets.
            self._persist_events(trades, prices)

            # Refresh positions only for portfolio trades; DB must succeed first.
            self._persist_latest_positions(trades)

            # Redis failure also prevents offset commits.
            self._persist_redis_state(pending_keys, quality_deltas)
            self._cache_latest_positions()

            self._consumer.commit(asynchronous=False)
            BATCHES_PROCESSED.inc()

            logger.info(
                "Committed %d messages; persisted %d trades, %d prices, %d idempotency keys",
                len(messages),
                len(trades),
                len(prices),
                len(pending_keys),
            )
            return len(messages)

        except Exception:
            # Prevent an uncommitted batch's in-memory watermark from rejecting
            # its own events as late when Kafka replays them.
            self._max_event_time = watermark_snapshot
            PROCESSING_ERRORS.labels(stage="batch").inc()
            logger.exception("Batch failed; Kafka offsets were not committed")
            raise

    def run_forever(self) -> None:
        """Consume until interrupted, then close clients gracefully."""
        try:
            while not self._stopping:
                self.process_batch()
        except KeyboardInterrupt:
            logger.info("Consumer shutdown requested")
            self._stopping = True
        finally:
            try:
                self._producer.flush(10.0)
            finally:
                self._consumer.close()
                self._redis.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    KafkaBatchConsumer().run_forever()
