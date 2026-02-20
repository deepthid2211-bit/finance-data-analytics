{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_forex_rates') }}
),

with_analytics AS (
    SELECT
        surrogate_key,
        trade_date,
        pair,
        base_currency,
        quote_currency,
        open_rate,
        high_rate,
        low_rate,
        close_rate,
        (close_rate - LAG(close_rate) OVER (PARTITION BY pair ORDER BY trade_date))
            / NULLIF(LAG(close_rate) OVER (PARTITION BY pair ORDER BY trade_date), 0) * 100
            AS daily_change_pct,
        AVG(close_rate) OVER (
            PARTITION BY pair ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d,
        AVG(close_rate) OVER (
            PARTITION BY pair ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS moving_avg_30d,
        source,
        _loaded_at,
        CURRENT_TIMESTAMP() AS processed_at
    FROM staged
)

SELECT * FROM with_analytics

{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
