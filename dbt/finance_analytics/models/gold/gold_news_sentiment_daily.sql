{{ config(materialized='table') }}

WITH news AS (
    SELECT * FROM {{ source('silver', 'news_enriched') }}
),

company_info AS (
    SELECT
        ticker,
        raw_data:shortName::VARCHAR AS company_name
    FROM {{ source('bronze', 'raw_company_fundamentals') }}
    WHERE data_type = 'company_info'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY ingestion_timestamp DESC) = 1
),

daily_sentiment AS (
    SELECT
        published_at::DATE AS trade_date,
        n.ticker,
        COALESCE(c.company_name, n.ticker) AS company_name,
        COUNT(*) AS total_articles,
        AVG(sentiment_score) AS avg_sentiment_score,
        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_articles,
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_articles,
        SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_articles,
        CASE
            WHEN AVG(sentiment_score) > 0.1 THEN 'bullish'
            WHEN AVG(sentiment_score) < -0.1 THEN 'bearish'
            ELSE 'neutral'
        END AS sentiment_trend,
        CURRENT_TIMESTAMP() AS processed_at
    FROM news n
    LEFT JOIN company_info c ON n.ticker = c.ticker
    WHERE published_at IS NOT NULL
    GROUP BY published_at::DATE, n.ticker, COALESCE(c.company_name, n.ticker)
)

SELECT * FROM daily_sentiment
