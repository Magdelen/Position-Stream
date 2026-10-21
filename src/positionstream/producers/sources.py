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
        vendor = VendorTrade.model_validate(payload)
        return TradeEvent(
            trade_id=vendor.tradeRef,
            symbol=vendor.ticker,
            timestamp=vendor.eventTime,
            quantity=vendor.shares,
            price=vendor.price,
            source="vendor",
        )

    elif source == "oms":
        oms = OMSTrade.model_validate(payload)
        return TradeEvent(
            trade_id=oms.order_id,
            symbol=oms.instrument,
            timestamp=oms.executed_at,
            quantity=oms.filled_qty,
            price=oms.avg_price,
            source="oms",
        )

    elif source == "custodian":
        custodian = CustodianTrade.model_validate(payload)
        return TradeEvent(
            trade_id=custodian.executionId,
            symbol=custodian.security,
            timestamp=custodian.settlement_timestamp,
            quantity=custodian.units,
            price=custodian.exec_price,
            source="custodian",
        )

    raise ValueError(f"Unsupported trade source: {source}")
