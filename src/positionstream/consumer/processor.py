import json
from typing import Any

from pydantic import ValidationError

from positionstream.common.models import PriceEvent
from positionstream.producers.sources import transform_trade


def process_raw_message(value: bytes | str) -> dict[str, Any]:
    """Parse a raw Kafka message and normalize its event."""
    try:
        envelope = json.loads(value)
        event_type = envelope["event_type"]
        source = envelope.get("source")
        payload = envelope["payload"]

        if event_type == "trade":
            event = transform_trade(source, payload)
        elif event_type == "price":
            event = PriceEvent.model_validate(payload)
        else:
            raise ValueError(f"Unsupported event type: {event_type}")

        return {
            "status": "clean",
            "event_type": event_type,
            "correlation_id": envelope.get("correlation_id"),
            "event": event.model_dump(mode="json"),
        }

    except (
        json.JSONDecodeError,
        UnicodeDecodeError,
        KeyError,
        TypeError,
        ValueError,
        ValidationError,
    ) as exc:
        return {
            "status": "rejected",
            "reason_code": "SCHEMA_INVALID",
            "error": str(exc),
        }
