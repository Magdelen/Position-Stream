import random
from datetime import UTC, datetime, timedelta
from typing import Any

SYMBOLS = ("AAPL", "MSFT")
SOURCES = ("vendor", "oms", "custodian")


def generate_events(
    event_count: int = 100,
    seed: int = 42,
) -> dict[str, list[dict[str, Any]]]:
    """Generate raw trade and price payloads, including intentional defects.

    event_count controls the number of trade events. The same number of
    price events is generated. Reusing a seed reproduces the same data.
    """
    if event_count < 0:
        raise ValueError("event_count must be zero or greater")

    rng = random.Random(seed)
    start = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    trades: list[dict[str, Any]] = []
    prices: list[dict[str, Any]] = []

    for index in range(event_count):
        source = SOURCES[index % len(SOURCES)]
        symbol = rng.choice(SYMBOLS)
        timestamp = start + timedelta(seconds=index * 5)
        quantity = rng.randint(1, 100)
        price = round(rng.uniform(150, 300), 2)
        trade_id = f"TRD-{index:06d}"

        if source == "vendor":
            payload = {
                "tradeRef": trade_id,
                "ticker": symbol,
                "eventTime": timestamp,
                "shares": quantity,
                "price": price,
            }
        elif source == "oms":
            payload = {
                "order_id": trade_id,
                "instrument": symbol,
                "executed_at": timestamp,
                "filled_qty": quantity,
                "avg_price": price,
            }
        else:
            payload = {
                "executionId": trade_id,
                "security": symbol,
                "settlement_timestamp": timestamp,
                "units": quantity,
                "exec_price": price,
            }

        trades.append({"source": source, "payload": payload})
        prices.append(
            {
                "event_id": f"PX-{index:06d}",
                "symbol": symbol,
                "timestamp": timestamp,
                "price": price,
                "source": source,
            }
        )

    # Inject defects when there are enough events to keep them distinct.
    if event_count >= 1:
        trades[0]["payload"][
            "ticker"
            if trades[0]["source"] == "vendor"
            else "instrument"
            if trades[0]["source"] == "oms"
            else "security"
        ] = None
        prices[0]["price"] = None

    if event_count >= 2:
        trades[1] = {
            "source": trades[0]["source"],
            "payload": trades[0]["payload"].copy(),
        }
        prices[1] = prices[0].copy()

    if event_count >= 3:
        late_time = start - timedelta(days=2)
        item = trades[2]
        timestamp_field = {
            "vendor": "eventTime",
            "oms": "executed_at",
            "custodian": "settlement_timestamp",
        }[item["source"]]
        item["payload"][timestamp_field] = late_time
        prices[2]["timestamp"] = late_time

    if event_count >= 4:
        item = trades[3]
        symbol_field = {
            "vendor": "ticker",
            "oms": "instrument",
            "custodian": "security",
        }[item["source"]]
        item["payload"][symbol_field] = "BAD$"
        prices[3]["symbol"] = "BAD$"

    return {"trades": trades, "prices": prices}
