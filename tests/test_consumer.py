import pytest

from positionstream.consumer import consumer as consumer_module
from positionstream.consumer.consumer import KafkaBatchConsumer


class FakeMessage:
    def __init__(self, value: bytes = b"{}") -> None:
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
    message = FakeMessage()
    instance = make_consumer([message], batch_size=5)
    reject_message(monkeypatch)

    delivery_order = []
    instance._publish_dlq = lambda *args: delivery_order.append("publish")
    instance._flush_outputs = lambda: delivery_order.append("flush")

    original_commit = instance._consumer.commit

    def record_commit(asynchronous):
        delivery_order.append("commit")
        original_commit(asynchronous)

    instance._consumer.commit = record_commit

    processed = instance.process_batch(timeout=0.25)

    assert processed == 1
    assert instance._consumer.consume_args == (5, 0.25)
    assert delivery_order == ["publish", "flush", "commit"]
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

    processed = instance.process_batch()

    assert processed == 1
    assert len(attempts) == 2
    assert instance._consumer.commit_calls == 1


def test_does_not_commit_when_output_delivery_retries_exhaust(monkeypatch):
    instance = make_consumer([FakeMessage()])
    reject_message(monkeypatch)

    instance._publish_dlq = lambda *args: None
    flush_attempts = []

    def failing_flush():
        flush_attempts.append(1)
        raise RuntimeError("Delivery unavailable")

    instance._flush_outputs = failing_flush
    monkeypatch.setattr(consumer_module.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="offsets not committed"):
        instance.process_batch()

    assert len(flush_attempts) == consumer_module.MAX_RETRIES + 1
    assert instance._consumer.commit_calls == 0


def test_empty_batch_does_not_commit():
    instance = make_consumer([])

    processed = instance.process_batch()

    assert processed == 0
    assert instance._consumer.commit_calls == 0


def test_process_batch_skips_duplicate_event():
    from unittest.mock import MagicMock

    consumer = KafkaBatchConsumer.__new__(KafkaBatchConsumer)
    consumer._consumer = MagicMock()
    consumer._consumer.consume.return_value = [
        MagicMock(
            error=MagicMock(return_value=None),
            value=MagicMock(
                return_value=(
                    b'{"event_type":"trade","source":"vendor",'
                    b'"payload":{"tradeRef":"duplicate-1","ticker":"AAPL",'
                    b'"eventTime":"2026-10-21T10:00:00+00:00",'
                    b'"shares":2,"price":150.0},'
                    b'"correlation_id":"duplicate-test"}'
                )
            ),
        )
    ]
    consumer.batch_size = 10
    consumer._max_event_time = {}
    consumer._redis = MagicMock()
    consumer.allowed_lateness_seconds = 300
