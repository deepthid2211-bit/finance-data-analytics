{{ config(materialized='view') }}

WITH raw_stocks AS (
    SELECT
        ingestion_timestamp,
        source,
        ticker,
        raw_data,
        raw_data:date::TIMESTAMP_NTZ AS trade_date,
        raw_data:open::FLOAT AS open_price,
        raw_data:high::FLOAT AS high_price,
        raw_data:low::FLOAT AS low_price,
        raw_data:close::FLOAT AS close_price,
        raw_data:volume::INTEGER AS volume,
        _loaded_at
    FROM {{ source('bronze', 'raw_stock_prices') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ticker', 'trade_date']) }} AS surrogate_key,
    trade_date,
    ticker,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    source,
    _loaded_at
FROM raw_stocks
WHERE trade_date IS NOT NULL
  AND close_price IS NOT NULL
  AND close_price > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, trade_date ORDER BY _loaded_at DESC) = 1
