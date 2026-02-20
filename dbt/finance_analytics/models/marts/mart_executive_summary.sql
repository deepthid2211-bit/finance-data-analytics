{{ config(materialized='view') }}

WITH daily AS (
    SELECT * FROM {{ ref('gold_stock_daily_summary') }}
)

SELECT
    trade_date,
    COUNT(DISTINCT ticker) AS total_stocks_tracked,
    ROUND(AVG(daily_return_pct), 4) AS market_avg_return,
    MAX(daily_return_pct) AS top_gainer_return,
    MAX_BY(ticker, daily_return_pct) AS top_gainer_ticker,
    MIN(daily_return_pct) AS top_loser_return,
    MIN_BY(ticker, daily_return_pct) AS top_loser_ticker,
    SUM(volume) AS total_market_volume,
    ROUND(AVG(volatility_30d), 4) AS avg_market_volatility
FROM daily
GROUP BY trade_date
ORDER BY trade_date DESC
