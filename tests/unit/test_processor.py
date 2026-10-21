import json

from positionstream.consumer.processor import process_raw_message


def make_trade(source, payload):
    return {
        "event_type": "trade",
        "source": source,
        "correlation_id": "test-001",
        "payload": payload,
    }


def test_valid_vendor_trade_is_normalized():
    message = make_trade(
        "vendor",
        {
            "tradeRef": "TRD-1",
            "ticker": "aapl",
            "eventTime": "2026-01-05T14:30:00+00:00",
            "shares": 10,
            "price": 150.5,
        },
    )

    result = process_raw_message(json.dumps(message))

    assert result["status"] == "clean"
    assert result["event"]["trade_id"] == "TRD-1"
    assert result["event"]["symbol"] == "AAPL"
    assert result["event"]["quantity"] == 10
    assert result["correlation_id"] == "test-001"


def test_valid_price_event_is_accepted():
    message = {
        "event_type": "price",
        "source": "vendor",
        "correlation_id": "test-002",
        "payload": {
            "event_id": "PX-1",
            "symbol": "msft",
            "timestamp": "2026-01-05T14:30:00+00:00",
            "price": 420.5,
            "source": "vendor",
        },
    }

    result = process_raw_message(json.dumps(message))

    assert result["status"] == "clean"
    assert result["event"]["symbol"] == "MSFT"
    assert result["event"]["price"] == 420.5


def test_invalid_trade_is_rejected():
    message = make_trade(
        "vendor",
        {
            "tradeRef": "TRD-2",
            "ticker": "BAD$",
            "eventTime": "2026-01-05T14:30:00+00:00",
            "shares": 10,
            "price": 150.5,
        },
    )

    result = process_raw_message(json.dumps(message))

    assert result["status"] == "rejected"
    assert result["reason_code"] == "SCHEMA_INVALID"


def test_malformed_json_is_rejected():
    result = process_raw_message("{not valid json")

    assert result["status"] == "rejected"
    assert result["reason_code"] == "SCHEMA_INVALID"


def test_unsupported_event_type_is_rejected():
    message = {
        "event_type": "unknown",
        "source": "vendor",
        "correlation_id": "test-003",
        "payload": {},
    }

    result = process_raw_message(json.dumps(message))

    assert result["status"] == "rejected"
    assert result["reason_code"] == "SCHEMA_INVALID"


def test_missing_required_field_is_rejected():
    message = {
        "event_type": "trade",
        "source": "vendor",
        "correlation_id": "test-004",
        "payload": {"tradeRef": "TRD-3"},
    }

    result = process_raw_message(json.dumps(message))

    assert result["status"] == "rejected"
    assert result["reason_code"] == "SCHEMA_INVALID"
