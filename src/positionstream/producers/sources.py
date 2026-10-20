from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from positionstream.common.models import TradeEvent


class VendorTrade(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tradeRef: str = Field(min_length=1)
    ticker: str
    eventTime: datetime
    shares: int = Field(gt=0)
    price: float = Field(gt=0, allow_inf_nan=False)


class OMSTrade(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    instrument: str
    executed_at: datetime
    filled_qty: int = Field(gt=0)
    avg_price: float = Field(gt=0, allow_inf_nan=False)


class CustodianTrade(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executionId: str = Field(min_length=1)
    security: str
    settlement_timestamp: datetime
    units: int = Field(gt=0)
    exec_price: float = Field(gt=0, allow_inf_nan=False)


def transform_trade(source: str, payload: dict[str, Any]) -> TradeEvent:
    """Validate a source payload and normalize it to TradeEvent."""

    if source == "vendor":
        raw = VendorTrade.model_validate(payload)
        values = {
            "trade_id": raw.tradeRef,
            "symbol": raw.ticker,
            "timestamp": raw.eventTime,
            "quantity": raw.shares,
            "price": raw.price,
        }
    elif source == "oms":
        raw = OMSTrade.model_validate(payload)
        values = {
            "trade_id": raw.order_id,
            "symbol": raw.instrument,
            "timestamp": raw.executed_at,
            "quantity": raw.filled_qty,
            "price": raw.avg_price,
        }
    elif source == "custodian":
        raw = CustodianTrade.model_validate(payload)
        values = {
            "trade_id": raw.executionId,
            "symbol": raw.security,
            "timestamp": raw.settlement_timestamp,
            "quantity": raw.units,
            "price": raw.exec_price,
        }
    else:
        raise ValueError(f"Unsupported trade source: {source}")

    return TradeEvent(**values, source=source)
