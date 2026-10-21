from datetime import UTC

import pytest

from positionstream.consumer import consumer as consumer_module
from positionstream.consumer.consumer import KafkaBatchConsumer


class FakeMessage:
    def __init__(self, value=b"{}"):
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value

    def key(self):
        return b"test-key"


class FakeKafkaConsumer:
    def __init__(self, messages):
        self.messages = messages
        self.commit_calls = 0
        self.consume_args = None

    def consume(self, num_messages, timeout):
        self.consume_args = (num_messages, timeout)
        return self.messages

    def commit(self, asynchronous):
        self.commit_calls += 1

    def close(self):
        pass


class FakeRedis:
    def __init__(self):
        self.saved = {}

    def exists(self, key):
        return int(key in self.saved)

    def set(self, key, value, ex):
        self.saved[key] = (value, ex)

    def close(self):
        pass


def make_consumer(messages, batch_size=10):
    instance = KafkaBatchConsumer.__new__(KafkaBatchConsumer)
    instance.batch_size = batch_size
    instance.allowed_lateness_seconds = 300
    instance._stopping = False
    instance._consumer = FakeKafkaConsumer(messages)
    instance._redis = FakeRedis()
    instance._delivery_errors = []
    instance._max_event_time = {}
    instance._producer = None
    return instance


def reject_message(monkeypatch):
    monkeypatch.setattr(
        consumer_module,
        "process_raw_message",
        lambda value: {
            "status": "rejected",
            "reason_code": "SCHEMA_INVALID",
            "error": "Invalid test payload",
        },
    )


def test_consumes_bounded_batch_and_commits_after_delivery(monkeypatch):
    instance = make_consumer([FakeMessage()], batch_size=5)
    reject_message(monkeypatch)

    order = []
    instance._publish_dlq = lambda *args: order.append("publish")
    instance._flush_outputs = lambda: order.append("flush")

    original_commit = instance._consumer.commit

    def record_commit(asynchronous):
        order.append("commit")
        original_commit(asynchronous)

    instance._consumer.commit = record_commit

    processed = instance.process_batch(timeout=0.25)

    assert processed == 1
    assert instance._consumer.consume_args == (5, 0.25)
    assert order == ["publish", "flush", "commit"]
    assert instance._consumer.commit_calls == 1


def test_retries_output_flush_before_committing(monkeypatch):
    instance = make_consumer([FakeMessage()])
    reject_message(monkeypatch)
    instance._publish_dlq = lambda *args: None

    attempts = []

    def flaky_flush():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("Temporary delivery failure")

    instance._flush_outputs = flaky_flush
    monkeypatch.setattr(consumer_module.time, "sleep", lambda _: None)

    assert instance.process_batch() == 1
    assert len(attempts) == 2
    assert instance._consumer.commit_calls == 1


def test_does_not_commit_when_delivery_retries_exhaust(monkeypatch):
    instance = make_consumer([FakeMessage()])
    reject_message(monkeypatch)
    instance._publish_dlq = lambda *args: None

    attempts = []

    def failing_flush():
        attempts.append(1)
        raise RuntimeError("Delivery unavailable")

    instance._flush_outputs = failing_flush
    monkeypatch.setattr(consumer_module.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="offsets not committed"):
        instance.process_batch()

    assert len(attempts) == consumer_module.MAX_RETRIES + 1
    assert instance._consumer.commit_calls == 0


def test_empty_batch_does_not_commit():
    instance = make_consumer([])

    assert instance.process_batch() == 0
    assert instance._consumer.commit_calls == 0


def test_duplicate_event_is_rejected():
    instance = make_consumer([])
    event = {
        "trade_id": "trade-001",
        "source": "vendor",
        "symbol": "AAPL",
        "timestamp": "2026-01-05T14:30:00+00:00",
    }

    pending_keys = set()

    first = instance._check_event("trade", event, pending_keys)
    second = instance._check_event("trade", event, pending_keys)

    assert first is None
    assert second is not None
    assert second[0] == "DUPLICATE"


def test_late_event_is_rejected():
    from datetime import datetime

    instance = make_consumer([])
    instance._max_event_time[("trade", "AAPL")] = datetime(2026, 1, 5, 14, 40, tzinfo=UTC)

    event = {
        "trade_id": "trade-old",
        "source": "vendor",
        "symbol": "AAPL",
        "timestamp": "2026-01-05T14:30:00+00:00",
    }

    result = instance._check_event("trade", event, set())

    assert result is not None
    assert result[0] == "LATE"


def test_malformed_event_is_routed_to_dlq(monkeypatch):
    instance = make_consumer([FakeMessage(b"not-valid-json")])

    captured = []
    instance._publish_dlq = lambda message, reason_code, error: captured.append(
        (reason_code, error)
    )
    instance._flush_outputs = lambda: None

    monkeypatch.setattr(
        consumer_module,
        "process_raw_message",
        lambda value: {
            "status": "rejected",
            "reason_code": "SCHEMA_INVALID",
            "error": "Malformed JSON",
        },
    )

    processed = instance.process_batch()

    assert processed == 1
    assert captured[0][0] == "SCHEMA_INVALID"
    assert "Malformed JSON" in captured[0][1]
    assert instance._consumer.commit_calls == 1


def test_dlq_preserves_correlation_id(monkeypatch):
    import json

    instance = make_consumer([])
    raw = {
        "correlation_id": "corr-test-123",
        "event_type": "trade",
        "payload": {"invalid": True},
    }
    message = FakeMessage(json.dumps(raw).encode())
    captured = []

    instance._publish = lambda topic, key, value: captured.append((topic, key, value))

    instance._publish_dlq(message, "SCHEMA_INVALID", "Invalid payload")

    topic, _, value = captured[0]

    assert topic == consumer_module.DLQ_TOPIC
    assert value["correlation_id"] == "corr-test-123"
    assert value["reason_code"] == "SCHEMA_INVALID"
    assert value["original"] == raw
