import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError

from positionstream.producers.producer import (
    RAW_TOPIC,
    RawEventProducer,
    _event_key,
    _json_default,
)


def test_json_default_serializes_datetime() -> None:
    value = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)

    assert _json_default(value) == "2026-01-05T14:30:00+00:00"


def test_json_default_rejects_unsupported_type() -> None:
    with pytest.raises(TypeError, match="not JSON serializable"):
        _json_default(object())


def test_trade_event_key_uses_source_trade_id() -> None:
    event = {
        "source": "oms",
        "payload": {"order_id": "TRD-000001"},
    }

    assert _event_key("trade", event) == "TRD-000001"


def test_price_event_key_uses_event_id() -> None:
    event = {"event_id": "PX-000001"}

    assert _event_key("price", event) == "PX-000001"


def test_trade_event_key_rejects_unknown_source() -> None:
    event = {"source": "unknown", "payload": {}}

    with pytest.raises(ValueError, match="Unsupported trade source"):
        _event_key("trade", event)


def test_event_key_rejects_unknown_event_type() -> None:
    with pytest.raises(ValueError, match="Unsupported event type"):
        _event_key("unknown", {})


@patch("positionstream.producers.producer.Producer")
def test_publish_serializes_and_queues_trade(
    producer_class: MagicMock,
) -> None:
    kafka = producer_class.return_value
    raw_producer = RawEventProducer("localhost:9092")

    event = {
        "source": "vendor",
        "payload": {
            "tradeRef": "TRD-000001",
            "eventTime": datetime(2026, 1, 5, tzinfo=UTC),
            "ticker": "AAPL",
        },
    }

    raw_producer.publish("trade", event, "batch-001")

    kwargs = kafka.produce.call_args.kwargs
    envelope = json.loads(kwargs["value"])

    assert kwargs["topic"] == RAW_TOPIC
    assert kwargs["key"] == "TRD-000001"
    assert envelope["event_type"] == "trade"
    assert envelope["source"] == "vendor"
    assert envelope["correlation_id"] == "batch-001"
    assert envelope["payload"]["eventTime"] == "2026-01-05T00:00:00+00:00"
    kafka.poll.assert_called_once_with(0)


@patch("positionstream.producers.producer.Producer")
def test_publish_queues_price_event(
    producer_class: MagicMock,
) -> None:
    kafka = producer_class.return_value
    raw_producer = RawEventProducer("localhost:9092")

    event = {
        "event_id": "PX-000001",
        "symbol": "AAPL",
        "timestamp": datetime(2026, 1, 5, tzinfo=UTC),
        "price": 200.5,
        "source": "vendor",
    }

    raw_producer.publish("price", event, "batch-002")

    kwargs = kafka.produce.call_args.kwargs
    envelope = json.loads(kwargs["value"])

    assert kwargs["topic"] == RAW_TOPIC
    assert kwargs["key"] == "PX-000001"
    assert envelope["event_type"] == "price"
    assert envelope["payload"]["symbol"] == "AAPL"


@patch("positionstream.producers.producer.Producer")
def test_publish_rejects_unknown_event_type(
    producer_class: MagicMock,
) -> None:
    raw_producer = RawEventProducer("localhost:9092")

    with pytest.raises(ValueError, match="Unsupported event type"):
        raw_producer.publish("unknown", {}, "batch-003")

    producer_class.return_value.produce.assert_not_called()


@patch("positionstream.producers.producer.Producer")
def test_flush_succeeds_when_all_messages_are_delivered(
    producer_class: MagicMock,
) -> None:
    kafka = producer_class.return_value
    kafka.flush.return_value = 0
    raw_producer = RawEventProducer("localhost:9092")

    raw_producer.flush()

    kafka.flush.assert_called_once_with(10.0)


@patch("positionstream.producers.producer.Producer")
def test_flush_raises_if_messages_remain_queued(
    producer_class: MagicMock,
) -> None:
    kafka = producer_class.return_value
    kafka.flush.return_value = 2
    raw_producer = RawEventProducer("localhost:9092")

    with pytest.raises(TimeoutError, match="2 Kafka message"):
        raw_producer.flush()


@patch("positionstream.producers.producer.Producer")
def test_delivery_callback_records_failure(
    producer_class: MagicMock,
) -> None:
    raw_producer = RawEventProducer("localhost:9092")
    error = KafkaError(KafkaError._FAIL)

    raw_producer._on_delivery(error, MagicMock())

    assert raw_producer._delivery_errors
    assert str(error) in raw_producer._delivery_errors[0]
