import json
from typing import Any

import redis


class RedisStorage:
    """Redis cache for latest market prices, positions, and quality counters."""

    def __init__(self, url: str = "redis://localhost:6379/0") -> None:
        self._client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )

    def health_check(self) -> bool:
        return bool(self._client.ping())

    def set_latest_position(
        self,
        portfolio_id: str,
        symbol: str,
        quantity: float,
        market_value: float,
        quality: str = "OK",
    ) -> None:
        key = f"positionstream:position:{portfolio_id}:{symbol}"
        value = {
            "portfolio_id": portfolio_id,
            "symbol": symbol,
            "quantity": quantity,
            "market_value": market_value,
            "quality": quality,
        }
        self._client.set(key, json.dumps(value))

    def get_latest_position(
        self, portfolio_id: str, symbol: str
    ) -> dict[str, Any] | None:
        key = f"positionstream:position:{portfolio_id}:{symbol}"
        value = self._client.get(key)
        return json.loads(value) if value is not None else None

    def increment_quality_counter(
        self, counter_name: str, amount: int = 1
    ) -> int:
        if not counter_name or ":" in counter_name:
            raise ValueError("counter_name must be non-empty and contain no ':'")
        key = f"positionstream:quality:{counter_name}"
        return int(self._client.incrby(key, amount))

    def get_quality_counters(self) -> dict[str, int]:
        counters: dict[str, int] = {}
        for key in self._client.scan_iter(match="positionstream:quality:*"):
            name = key.rsplit(":", 1)[-1]
            counters[name] = int(self._client.get(key) or 0)
        return counters

    def close(self) -> None:
        self._client.close()