import pytest
from pydantic import ValidationError

from positionstream.common.models import PriceEvent, TradeEvent
from positionstream.producers.sources import transform_trade
from positionstream.producers.synthetic import generate_events


def test_trade_model_normalizes_symbol() -> None:
    trade = TradeEvent(
        trade_id="TRD-1",
        symbol=" aapl ",
        timestamp="2026-01-05T14:30:00Z",
        quantity=10,
        price=200.50,
        source="vendor",
    )
    assert trade.symbol == "AAPL"


def test_trade_model_rejects_invalid_symbol() -> None:
    with pytest.raises(ValidationError):
        TradeEvent(
            trade_id="TRD-1",
            symbol="BAD$",
            timestamp="2026-01-05T14:30:00Z",
            quantity=10,
            price=200,
            source="vendor",
        )


def test_trade_model_rejects_non_positive_quantity() -> None:
    with pytest.raises(ValidationError):
        TradeEvent(
            trade_id="TRD-1",
            symbol="AAPL",
            timestamp="2026-01-05T14:30:00Z",
            quantity=0,
            price=200,
            source="vendor",
        )


def test_price_model_rejects_null_price() -> None:
    with pytest.raises(ValidationError):
        PriceEvent.model_validate(
            {
                "event_id": "PX-1",
                "symbol": "AAPL",
                "timestamp": "2026-01-05T14:30:00Z",
                "price": None,
                "source": "vendor",
            }
        )


def test_all_three_sources_transform_to_canonical_model() -> None:
    events = generate_events(event_count=10, seed=42)

    # Use indexes 4, 5 and 6, which are valid vendor, OMS and custodian events.
    transformed = [
        transform_trade(item["source"], item["payload"]) for item in events["trades"][4:7]
    ]

    assert {trade.source for trade in transformed} == {
        "vendor",
        "oms",
        "custodian",
    }
    assert all(isinstance(trade, TradeEvent) for trade in transformed)
    assert all(trade.symbol in {"AAPL", "MSFT"} for trade in transformed)


def test_generator_is_reproducible() -> None:
    assert generate_events(20, seed=123) == generate_events(20, seed=123)


def test_different_seeds_change_generated_data() -> None:
    assert generate_events(20, seed=1) != generate_events(20, seed=2)


def test_generator_injects_required_defects() -> None:
    events = generate_events(event_count=10, seed=42)

    # Null fields
    assert (
        events["trades"][0]["payload"][
            {"vendor": "ticker", "oms": "instrument", "custodian": "security"}[
                events["trades"][0]["source"]
            ]
        ]
        is None
    )
    assert events["prices"][0]["price"] is None

    # Duplicate trade and price event
    assert events["trades"][1] == events["trades"][0]
    assert events["prices"][1] == events["prices"][0]

    # Late events
    assert events["prices"][2]["timestamp"] < events["prices"][0]["timestamp"]

    # Invalid symbols
    assert events["prices"][3]["symbol"] == "BAD$"


def test_invalid_source_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported trade source"):
        transform_trade("unknown", {})
