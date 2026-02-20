{{ config(materialized='table') }}

WITH daily_stocks AS (
    SELECT * FROM {{ ref('gold_stock_daily_summary') }}
    WHERE sector IS NOT NULL
),

sector_agg AS (
    SELECT
        trade_date,
        sector,
        COUNT(DISTINCT ticker) AS total_stocks,
        AVG(daily_return_pct) AS avg_daily_return,
        SUM(volume) AS sector_volume,
        MAX_BY(ticker, daily_return_pct) AS best_performer,
        MIN_BY(ticker, daily_return_pct) AS worst_performer,
        CURRENT_TIMESTAMP() AS processed_at
    FROM daily_stocks
    GROUP BY trade_date, sector
)

SELECT * FROM sector_agg
