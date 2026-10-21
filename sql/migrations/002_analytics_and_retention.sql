
-- Migration: 002_analytics_and_retention.sql
-- Purpose: Compress historical data, retain one year, and aggregate prices.

-- Compress older chunks, organized by symbol and event time.
ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);

ALTER TABLE prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'event_time DESC'
);

SELECT add_compression_policy(
    'trades',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT add_compression_policy(
    'prices',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Keep one year of raw events.
SELECT add_retention_policy(
    'trades',
    INTERVAL '365 days',
    if_not_exists => TRUE
);

SELECT add_retention_policy(
    'prices',
    INTERVAL '365 days',
    if_not_exists => TRUE
);

-- One-minute OHLC derived from price events.
CREATE MATERIALIZED VIEW IF NOT EXISTS price_ohlc_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 minute', event_time) AS bucket,
    symbol,
    first(price, event_time) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    last(price, event_time) AS close,
    COUNT(*) AS event_count
FROM prices
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'price_ohlc_1m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);
-- One-minute OHLC aggregate for market prices.
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_ohlc_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 minute', event_time) AS bucket,
    symbol,
    first(price, event_time) AS open,
    MAX(price) AS high,
    MIN(price) AS low,
    last(price, event_time) AS close,
    COUNT(*) AS tick_count
FROM prices
GROUP BY bucket, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'prices_ohlc_1m',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- Daily position summary derived from trade events.
CREATE OR REPLACE VIEW daily_position_summary AS
SELECT
    (event_time AT TIME ZONE 'UTC')::date AS trading_date,
    symbol,
    SUM(quantity) AS total_quantity,
    SUM(quantity * price) AS traded_value,
    COUNT(*) AS trade_count
FROM trades
GROUP BY (event_time AT TIME ZONE 'UTC')::date, symbol;