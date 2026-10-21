
-- Migration: 001_market_tables.sql
-- Purpose: Create the canonical trade and price storage tables.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS trades (
    event_time TIMESTAMPTZ NOT NULL,
    trade_id TEXT NOT NULL,
    symbol VARCHAR(5) NOT NULL,
    quantity BIGINT NOT NULL CHECK (quantity > 0),
    price NUMERIC(20, 8) NOT NULL CHECK (price > 0),
    source TEXT NOT NULL CHECK (source IN ('vendor', 'oms', 'custodian')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_trades_time_id UNIQUE (event_time, trade_id)
);

SELECT create_hypertable(
    'trades',
    'event_time',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
    ON trades (symbol, event_time DESC);


CREATE TABLE IF NOT EXISTS prices (
    event_time TIMESTAMPTZ NOT NULL,
    event_id TEXT NOT NULL,
    symbol VARCHAR(5) NOT NULL,
    price NUMERIC(20, 8) NOT NULL CHECK (price > 0),
    source TEXT NOT NULL CHECK (source IN ('vendor', 'oms', 'custodian')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_prices_time_id UNIQUE (event_time, event_id)
);

SELECT create_hypertable(
    'prices',
    'event_time',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_prices_symbol_time
    ON prices (symbol, event_time DESC);
    