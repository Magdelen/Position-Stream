from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class TradeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    trade_id: str = Field(min_length=1)
    symbol: str
    timestamp: datetime
    quantity: int = Field(gt=0)
    price: float = Field(gt=0, allow_inf_nan=False)
    source: Literal["vendor", "oms", "custodian"]

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        value = value.strip().upper()
        if not value.isalpha() or not value.isascii() or not 1 <= len(value) <= 5:
            raise ValueError("symbol must contain 1-5 ASCII letters")
        return value


class PriceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    symbol: str
    timestamp: datetime
    price: float = Field(gt=0, allow_inf_nan=False)
    source: Literal["vendor", "oms", "custodian"]

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        value = value.strip().upper()
        if not value.isalpha() or not value.isascii() or not 1 <= len(value) <= 5:
            raise ValueError("symbol must contain 1-5 ASCII letters")
        return value
