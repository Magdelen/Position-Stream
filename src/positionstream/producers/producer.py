import json
import logging
import os
from datetime import date, datetime
from typing import Any

from confluent_kafka import KafkaError, KafkaException, Producer

logger = logging.getLogger(__name__)

RAW_TOPIC = "market.raw"


def _json_default(value: Any) -> str:
    """Convert date and datetime objects into ISO 8601 strings."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _event_key(event_type: str, event: dict[str, Any]) -> str:
    """Extract a stable Kafka key from a raw trade or price event."""
    if event_type == "trade":
        payload = event["payload"]
        source = event["source"]

        id_fields = {
            "vendor": "tradeRef",
            "oms": "order_id",
            "custodian": "executionId",
        }

        if source not in id_fields:
            raise ValueError(f"Unsupported trade source: {source}")

        return str(payload[id_fields[source]])

    if event_type == "price":
        return str(event["event_id"])

    raise ValueError(f"Unsupported event type: {event_type}")


class RawEventProducer:
    """Publish unvalidated source events to the raw Kafka topic."""

    def __init__(self, bootstrap_servers: str | None = None) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": (
                    bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
                ),
                "client.id": "positionstream-producer",
            }
        )
        self._delivery_errors: list[str] = []

    def _on_delivery(
        self,
        error: KafkaError | None,
        message: Any,
    ) -> None:
        """Record whether Kafka accepted a published message."""
        if error is not None:
            description = str(error)
            self._delivery_errors.append(description)
            logger.error("Kafka delivery failed: %s", description)
            return

        logger.debug(
            "Delivered event to %s [%s] at offset %s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    def publish(
        self,
        event_type: str,
        event: dict[str, Any],
        correlation_id: str,
    ) -> None:
        """Publish one raw event, preserving its original source payload."""
        envelope = {
            "event_type": event_type,
            "source": (event["source"] if event_type == "trade" else event.get("source")),
            "correlation_id": correlation_id,
            "payload": event["payload"] if event_type == "trade" else event,
        }

        key = _event_key(event_type, event)
        value = json.dumps(envelope, default=_json_default)

        self._producer.produce(
            topic=RAW_TOPIC,
            key=key,
            value=value,
            callback=self._on_delivery,
        )

        # Serve delivery callbacks without blocking for every event.
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        """Wait for queued messages and fail if delivery was unsuccessful."""
        remaining = self._producer.flush(timeout)

        if remaining:
            raise TimeoutError(f"{remaining} Kafka message(s) remain undelivered after {timeout}s")

        if self._delivery_errors:
            errors = self._delivery_errors.copy()
            self._delivery_errors.clear()
            raise KafkaException(
                KafkaError(KafkaError._FAIL),
                f"{len(errors)} Kafka delivery failure(s): {'; '.join(errors)}",
            )
