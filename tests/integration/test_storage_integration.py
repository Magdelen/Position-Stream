import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from positionstream.consumer.consumer import (
    IDEMPOTENCY_PREFIX,
    IDEMPOTENCY_TTL_SECONDS,
    KafkaBatchConsumer,
)
from positionstream.storage.postgres import PostgresStorage
from positionstream.storage.redis_store import RedisStorage


@pytest.fixture
def postgres() -> PostgresStorage:
    dsn = os.getenv(
        "POSITIONSTREAM_DATABASE_URL",
        "postgresql://positionstream:local_dev_password@localhost:15432/positionstream",
    )
    storage = PostgresStorage(dsn)
    if not storage.health_check():
        pytest.fail("TimescaleDB health check failed")
    return storage


@pytest.fixture
def redis_store() -> RedisStorage:
    storage = RedisStorage(os.getenv("POSITIONSTREAM_REDIS_URL", "redis://localhost:6379/0"))
    if not storage.health_check():
        pytest.fail("Redis health check failed")
    yield storage
    storage.close()


def test_timescaledb_health(postgres: PostgresStorage) -> None:
    assert postgres.health_check()


def test_trade_write_is_idempotent(postgres: PostgresStorage) -> None:
    suffix = uuid4().hex
    event_time = datetime.now(UTC)
    trade = {
        "timestamp": event_time,
        "trade_id": f"it-{suffix}",
        "symbol": "AAPL",
        "quantity": 2,
        "price": 150.0,
        "source": "vendor",
        "portfolio_id": f"it-{suffix}",
        "side": "BUY",
    }

    first = postgres.write_trades([trade])
    second = postgres.write_trades([trade])

    assert first == 1
    assert second == 0


def test_price_write_is_idempotent(postgres: PostgresStorage) -> None:
    suffix = uuid4().hex
    price = {
        "timestamp": datetime.now(UTC),
        "event_id": f"it-{suffix}",
        "symbol": "AAPL",
        "price": 150.0,
        "source": "vendor",
    }

    first = postgres.write_prices([price])
    second = postgres.write_prices([price])

    assert first == 1
    assert second == 0


def test_redis_position_round_trip(redis_store: RedisStorage) -> None:
    portfolio_id = f"it-{uuid4().hex}"

    redis_store.set_latest_position(
        portfolio_id=portfolio_id,
        symbol="AAPL",
        quantity=2,
        market_value=300.0,
        quality="OK",
    )

    result = redis_store.get_latest_position(portfolio_id, "AAPL")

    assert result is not None
    assert result["portfolio_id"] == portfolio_id
    assert result["symbol"] == "AAPL"
    assert result["quantity"] == 2
    assert result["market_value"] == 300.0
    assert result["quality"] == "OK"


def test_redis_quality_counter_increments(redis_store: RedisStorage) -> None:
    counter = f"it_{uuid4().hex}"

    assert redis_store.increment_quality_counter(counter) == 1
    assert redis_store.increment_quality_counter(counter, amount=2) == 3


def test_redis_idempotency_key_survives_new_consumer(
    redis_store: RedisStorage,
) -> None:
    event_id = f"it-{uuid4().hex}"
    event = {
        "trade_id": event_id,
        "source": "vendor",
        "symbol": "AAPL",
        "timestamp": datetime.now(UTC).isoformat(),
    }
    key = f"{IDEMPOTENCY_PREFIX}:trade:vendor:{event_id}"

    # Simulate the first consumer recording a processed event.
    redis_store._client.set(key, "1", ex=IDEMPOTENCY_TTL_SECONDS)

    # A fresh consumer instance must recognize the event as a duplicate.
    consumer = KafkaBatchConsumer.__new__(KafkaBatchConsumer)
    consumer._redis = redis_store._client
    consumer._max_event_time = {}
    consumer.allowed_lateness_seconds = 300

    result = consumer._check_event("trade", event, set())

    assert result is not None
    assert result[0] == "DUPLICATE"


def test_late_event_is_rejected_after_newer_event(
    redis_store: RedisStorage,
) -> None:
    consumer = KafkaBatchConsumer.__new__(KafkaBatchConsumer)
    consumer._max_event_time = {}
    consumer.allowed_lateness_seconds = 300
    consumer._redis = redis_store._client

    now = datetime.now(UTC)
    newer_event = {
        "trade_id": f"new-{uuid4().hex}",
        "source": "vendor",
        "symbol": "AAPL",
        "timestamp": now.isoformat(),
    }
    late_event = {
        "trade_id": f"old-{uuid4().hex}",
        "source": "vendor",
        "symbol": "AAPL",
        "timestamp": (now - timedelta(minutes=10)).isoformat(),
    }

    assert consumer._check_event("trade", newer_event, set()) is None

    result = consumer._check_event("trade", late_event, set())

    assert result is not None
    assert result[0] == "LATE"


def test_portfolio_position_reconciles_with_signed_trades(
    postgres: PostgresStorage,
) -> None:
    suffix = uuid4().hex
    portfolio_id = f"it-{suffix}"
    event_time = datetime.now(UTC)

    trades = [
        {
            "timestamp": event_time,
            "trade_id": f"buy-{suffix}",
            "symbol": "AAPL",
            "quantity": 10,
            "price": 150.0,
            "source": "vendor",
            "portfolio_id": portfolio_id,
            "side": "BUY",
        },
        {
            "timestamp": event_time,
            "trade_id": f"sell-{suffix}",
            "symbol": "AAPL",
            "quantity": 3,
            "price": 155.0,
            "source": "vendor",
            "portfolio_id": portfolio_id,
            "side": "SELL",
        },
    ]

    assert postgres.write_trades(trades) == 2

    positions = postgres.fetch_portfolio_positions()
    matching = [
        position
        for position in positions
        if position["portfolio_id"] == portfolio_id and position["symbol"] == "AAPL"
    ]

    assert len(matching) == 1
    assert matching[0]["quantity"] == 7
    assert matching[0]["trade_count"] == 2
