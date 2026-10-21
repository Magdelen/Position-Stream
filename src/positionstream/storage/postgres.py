from collections.abc import Iterable, Mapping
from typing import Any

import psycopg
from psycopg.rows import dict_row

TradeRecord = Mapping[str, Any]
PriceRecord = Mapping[str, Any]

_INSERT_TRADE = """
    INSERT INTO trades (
        event_time, trade_id, symbol, quantity, price, source,
        portfolio_id, side
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_time, trade_id) DO NOTHING
"""

_INSERT_PRICE = """
    INSERT INTO prices (
        event_time, event_id, symbol, price, source
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (event_time, event_id) DO NOTHING
"""


class PostgresStorage:
    """Persist market events with transactional, idempotent writes."""

    def __init__(self, dsn: str) -> None:
        if not dsn.strip():
            raise ValueError("Database connection string cannot be empty")
        self._dsn = dsn

    def health_check(self) -> bool:
        with psycopg.connect(self._dsn) as connection:
            connection.execute("SELECT 1")
        return True

    def write_batch(
        self,
        trades: Iterable[TradeRecord] = (),
        prices: Iterable[PriceRecord] = (),
    ) -> dict[str, int]:
        """Insert trades and prices atomically in one transaction."""
        trade_rows = [
            (
                event["timestamp"],
                event["trade_id"],
                event["symbol"],
                event["quantity"],
                event["price"],
                event["source"],
                event.get("portfolio_id"),
                event.get("side"),
            )
            for event in trades
        ]

        price_rows = [
            (
                event["timestamp"],
                event["event_id"],
                event["symbol"],
                event["price"],
                event["source"],
            )
            for event in prices
        ]

        inserted_trades = 0
        inserted_prices = 0

        with psycopg.connect(self._dsn) as connection:
            with connection.cursor() as cursor:
                for row in trade_rows:
                    cursor.execute(_INSERT_TRADE, row)
                    inserted_trades += cursor.rowcount

                for row in price_rows:
                    cursor.execute(_INSERT_PRICE, row)
                    inserted_prices += cursor.rowcount

        return {"trades": inserted_trades, "prices": inserted_prices}

    def fetch_portfolio_positions(self) -> list[dict[str, Any]]:
        """Read current portfolio holdings from the database view."""
        query = """
            SELECT portfolio_id, symbol, quantity, latest_price,
                   market_value, price_time, trade_count, quality
            FROM portfolio_positions
        """
        with psycopg.connect(self._dsn, row_factory=dict_row) as connection:
            rows = connection.execute(query).fetchall()
        return [dict(row) for row in rows]

    def write_trades(self, events: Iterable[TradeRecord]) -> int:
        return self.write_batch(trades=events)["trades"]

    def write_prices(self, events: Iterable[PriceRecord]) -> int:
        return self.write_batch(prices=events)["prices"]
