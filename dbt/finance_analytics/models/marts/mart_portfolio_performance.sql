{{ config(materialized='view') }}

WITH stocks AS (
    SELECT * FROM {{ ref('gold_stock_daily_summary') }}
),

sentiment AS (
    SELECT * FROM {{ ref('gold_news_sentiment_daily') }}
)

SELECT
    s.trade_date,
    s.ticker,
    s.company_name,
    s.sector,
    s.open_price,
    s.high_price,
    s.low_price,
    s.close_price,
    s.volume,
    s.daily_return_pct,
    s.cumulative_return_pct,
    s.moving_avg_7d,
    s.moving_avg_30d,
    s.volatility_30d,
    s.volume_avg_20d,
    s.price_vs_52w_high,
    s.price_vs_52w_low,
    ns.avg_sentiment_score,
    ns.total_articles AS news_count,
    ns.sentiment_trend
FROM stocks s
LEFT JOIN sentiment ns
    ON s.ticker = ns.ticker AND s.trade_date = ns.trade_date
