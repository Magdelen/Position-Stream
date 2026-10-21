-- Migration: 003_portfolio_positions.sql

ALTER TABLE trades
    ADD COLUMN IF NOT EXISTS portfolio_id TEXT,
    ADD COLUMN IF NOT EXISTS side TEXT;

CREATE INDEX IF NOT EXISTS idx_trades_portfolio_symbol_time
    ON trades (portfolio_id, symbol, event_time DESC);

DROP VIEW IF EXISTS daily_position_summary;

CREATE VIEW daily_position_summary AS
SELECT
    (event_time AT TIME ZONE 'UTC')::date AS trading_date,
    portfolio_id,
    symbol,
    SUM(
        CASE side
            WHEN 'BUY' THEN quantity
            WHEN 'SELL' THEN -quantity
        END
    ) AS net_quantity,
    SUM(quantity * price) AS traded_value,
    COUNT(*) AS trade_count
FROM trades
WHERE portfolio_id IS NOT NULL
  AND side IS NOT NULL
GROUP BY
    (event_time AT TIME ZONE 'UTC')::date,
    portfolio_id,
    symbol;
