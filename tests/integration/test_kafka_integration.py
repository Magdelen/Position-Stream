import json
import time
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition

from positionstream.producers.producer import RAW_TOPIC, RawEventProducer


def test_kafka_raw_event_round_trip() -> None:
    suffix = uuid4().hex
    group_id = f"positionstream-it-{suffix}"
    event_id = f"it-{suffix}"

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    producer = RawEventProducer("localhost:9092")

    try:
        consumer.subscribe([RAW_TOPIC])

        # Allow the consumer to join its group before publishing.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            consumer.poll(0.2)
            if consumer.assignment():
                break
        else:
            raise AssertionError("Kafka consumer did not receive a partition assignment")

        producer.publish(
            "trade",
            {
                "source": "vendor",
                "payload": {
                    "tradeRef": event_id,
                    "ticker": "AAPL",
                    "eventTime": "2026-10-21T10:00:00+00:00",
                    "shares": 2,
                    "price": 150.0,
                },
            },
            correlation_id=f"corr-{suffix}",
        )
        producer.flush()

        deadline = time.monotonic() + 10
        message = None

        while time.monotonic() < deadline:
            candidate = consumer.poll(0.5)
            if candidate is None:
                continue
            if candidate.error():
                raise AssertionError(f"Kafka consume error: {candidate.error()}")

            envelope = json.loads(candidate.value())
            if envelope.get("correlation_id") == f"corr-{suffix}":
                message = envelope
                break

        assert message is not None, "Published Kafka event was not received"
        assert message["event_type"] == "trade"
        assert message["source"] == "vendor"
        assert message["payload"]["tradeRef"] == event_id
        assert message["payload"]["ticker"] == "AAPL"

    finally:
        producer.flush()
        consumer.close()


def test_kafka_redelivery_after_consumer_restart() -> None:
    """A restarted consumer re-reads an uncommitted message from Kafka."""
    from confluent_kafka import KafkaException

    suffix = uuid4().hex
    topic = RAW_TOPIC
    group_id = f"positionstream-recovery-{suffix}"
    event_id = f"recovery-{suffix}"

    producer = RawEventProducer("localhost:9092")
    first = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    try:
        first.subscribe([topic])

        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            first.poll(0.2)
            if first.assignment():
                break
        else:
            raise AssertionError("First consumer did not receive partition assignment")

        producer.publish(
            "trade",
            {
                "source": "vendor",
                "payload": {
                    "tradeRef": event_id,
                    "ticker": "AAPL",
                    "eventTime": "2026-10-21T10:00:00+00:00",
                    "shares": 2,
                    "price": 150.0,
                },
            },
            correlation_id=f"recovery-{suffix}",
        )
        producer.flush()

        first_message = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            message = first.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            envelope = json.loads(message.value())
            if envelope.get("payload", {}).get("tradeRef") == event_id:
                first_message = message
                break

        assert first_message is not None, "First consumer did not receive event"
        partition = first_message.partition()
        offset = first_message.offset()

        # Simulate a crash: close without committing the received message.
        first.close()
        first = None

        restarted = Consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        try:
            restarted.assign([TopicPartition(topic, partition, offset)])
            redelivered = restarted.poll(10)
            assert redelivered is not None, "Uncommitted event was not replayed"
            assert redelivered.error() is None
            replay = json.loads(redelivered.value())
            assert replay["payload"]["tradeRef"] == event_id
        finally:
            restarted.close()

    finally:
        if first is not None:
            first.close()
        producer.flush()
