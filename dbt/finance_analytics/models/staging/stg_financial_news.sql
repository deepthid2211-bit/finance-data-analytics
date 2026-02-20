{{ config(materialized='view') }}

WITH raw_news AS (
    SELECT
        ingestion_timestamp,
        source,
        ticker,
        headline,
        article_text,
        article_url,
        published_at,
        raw_data,
        _loaded_at
    FROM {{ source('bronze', 'raw_financial_news') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'headline', 'published_at']) }} AS news_id,
    ticker,
    headline,
    article_text AS summary,
    article_url,
    published_at,
    raw_data:provider::VARCHAR AS source_provider,
    source,
    _loaded_at
FROM raw_news
WHERE headline IS NOT NULL
  AND headline != ''
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, headline ORDER BY _loaded_at DESC) = 1
