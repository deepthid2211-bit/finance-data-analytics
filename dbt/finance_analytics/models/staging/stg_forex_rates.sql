{{ config(materialized='view') }}

WITH raw_forex AS (
    SELECT
        ingestion_timestamp,
        source,
        pair,
        raw_data,
        raw_data:date::TIMESTAMP_NTZ AS trade_date,
        raw_data:open::FLOAT AS open_rate,
        raw_data:high::FLOAT AS high_rate,
        raw_data:low::FLOAT AS low_rate,
        raw_data:close::FLOAT AS close_rate,
        _loaded_at
    FROM {{ source('bronze', 'raw_forex_rates') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['pair', 'trade_date']) }} AS surrogate_key,
    trade_date,
    pair,
    LEFT(REPLACE(pair, '=X', ''), 3) AS base_currency,
    RIGHT(REPLACE(pair, '=X', ''), 3) AS quote_currency,
    open_rate,
    high_rate,
    low_rate,
    close_rate,
    source,
    _loaded_at
FROM raw_forex
WHERE trade_date IS NOT NULL
  AND close_rate IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY pair, trade_date ORDER BY _loaded_at DESC) = 1
