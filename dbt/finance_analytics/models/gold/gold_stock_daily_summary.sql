{{ config(materialized='table') }}

WITH stock_data AS (
    SELECT * FROM {{ ref('silver_stock_prices') }}
),

company_info AS (
    SELECT
        ticker,
        raw_data:shortName::VARCHAR AS company_name,
        raw_data:sector::VARCHAR AS sector,
        raw_data:industry::VARCHAR AS industry,
        raw_data:fiftyTwoWeekHigh::FLOAT AS fifty_two_week_high,
        raw_data:fiftyTwoWeekLow::FLOAT AS fifty_two_week_low
    FROM {{ source('bronze', 'raw_company_fundamentals') }}
    WHERE data_type = 'company_info'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingestion_timestamp DESC) = 1
),

enriched AS (
    SELECT
        s.trade_date::DATE AS trade_date,
        s.ticker,
        COALESCE(c.company_name, s.ticker) AS company_name,
        c.sector,
        s.open_price,
        s.high_price,
        s.low_price,
        s.close_price,
        s.volume,
        s.daily_return_pct,
        -- Cumulative return from start
        (s.close_price - FIRST_VALUE(s.close_price) OVER (PARTITION BY s.ticker ORDER BY s.trade_date))
            / NULLIF(FIRST_VALUE(s.close_price) OVER (PARTITION BY s.ticker ORDER BY s.trade_date), 0) * 100
            AS cumulative_return_pct,
        s.moving_avg_7d,
        s.moving_avg_30d,
        s.volatility_30d,
        s.volume_avg_20d,
        -- Price vs 52-week range
        CASE WHEN c.fifty_two_week_high > 0
            THEN (s.close_price / c.fifty_two_week_high) * 100
        END AS price_vs_52w_high,
        CASE WHEN c.fifty_two_week_low > 0
            THEN (s.close_price / c.fifty_two_week_low) * 100
        END AS price_vs_52w_low,
        CURRENT_TIMESTAMP() AS processed_at
    FROM stock_data s
    LEFT JOIN company_info c ON s.ticker = c.ticker
)

SELECT * FROM enriched
