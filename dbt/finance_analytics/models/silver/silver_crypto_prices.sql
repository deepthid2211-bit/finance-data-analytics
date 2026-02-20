{{ config(
    materialized='incremental',
    unique_key='surrogate_key',
    on_schema_change='sync_all_columns'
) }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
),

-- Step 1: Compute daily returns first (can't nest LAG inside STDDEV)
with_returns AS (
    SELECT
        surrogate_key,
        trade_date,
        ticker,
        crypto_symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date))
            / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY trade_date), 0) * 100
            AS daily_return_pct,
        source,
        _loaded_at
    FROM staged
),

-- Step 2: Compute moving averages and volatility on pre-computed returns
with_technicals AS (
    SELECT
        surrogate_key,
        trade_date,
        ticker,
        crypto_symbol,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        daily_return_pct,
        AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS moving_avg_7d,
        AVG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS moving_avg_30d,
        STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        source,
        _loaded_at,
        CURRENT_TIMESTAMP() AS processed_at
    FROM with_returns
)

SELECT * FROM with_technicals

{% if is_incremental() %}
WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }})
{% endif %}
