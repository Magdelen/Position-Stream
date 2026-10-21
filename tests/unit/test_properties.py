import string

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from positionstream.common.models import TradeEvent

VALID_SYMBOLS = st.text(
    alphabet=string.ascii_letters,
    min_size=1,
    max_size=5,
)


@given(symbol=VALID_SYMBOLS)
def test_valid_symbols_are_normalized(symbol: str) -> None:
    trade = TradeEvent(
        trade_id="TRD-1",
        symbol=f" {symbol} ",
        timestamp="2026-01-05T14:30:00Z",
        quantity=1,
        price=1.0,
        source="vendor",
    )
    assert trade.symbol == symbol.upper()


@given(symbol=st.text(alphabet=string.ascii_letters, min_size=1, max_size=5))
def test_symbols_with_special_characters_are_rejected(symbol: str) -> None:
    with pytest.raises(ValidationError):
        TradeEvent(
            trade_id="TRD-1",
            symbol=f"{symbol}$",
            timestamp="2026-01-05T14:30:00Z",
            quantity=1,
            price=1.0,
            source="vendor",
        )


@given(
    quantity=st.integers(min_value=1, max_value=1_000_000),
    price=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False, allow_infinity=False),
)
def test_positive_quantity_and_price_are_accepted(quantity: int, price: float) -> None:
    trade = TradeEvent(
        trade_id="TRD-1",
        symbol="AAPL",
        timestamp="2026-01-05T14:30:00Z",
        quantity=quantity,
        price=price,
        source="vendor",
    )
    assert trade.quantity == quantity
    assert trade.price == price
