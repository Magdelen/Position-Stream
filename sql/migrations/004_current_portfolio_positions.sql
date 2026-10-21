-- Migration: 004_current_portfolio_positions.sql
-- Calculate current holdings from signed trades and value them at the latest price.

CREATE OR REPLACE VIEW portfolio_positions AS
WITH holdings AS (
    SELECT
        portfolio_id,
        symbol,
        SUM(
            CASE side
                WHEN 'BUY' THEN quantity
                WHEN 'SELL' THEN -quantity
            END
        ) AS quantity,
        COUNT(*) AS trade_count
    FROM trades
    WHERE portfolio_id IS NOT NULL
      AND side IS NOT NULL
    GROUP BY portfolio_id, symbol
),
latest_prices AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        price AS latest_price,
        event_time AS price_time
    FROM prices
    ORDER BY symbol, event_time DESC
)
SELECT
    h.portfolio_id,
    h.symbol,
    h.quantity,
    lp.latest_price,
    h.quantity * lp.latest_price AS market_value,
    lp.price_time,
    h.trade_count,
    CASE
        WHEN lp.latest_price IS NULL THEN 'MISSING_PRICE'
        WHEN h.quantity = 0 THEN 'ZERO_POSITION'
        ELSE 'OK'
    END AS quality
FROM holdings h
LEFT JOIN latest_prices lp ON lp.symbol = h.symbol;
