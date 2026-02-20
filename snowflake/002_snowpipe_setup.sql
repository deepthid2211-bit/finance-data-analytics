-- ============================================================
-- SNOWPIPE SETUP FOR CONTINUOUS DATA LOADING
-- This enables near real-time ingestion from external stages
-- ============================================================

USE DATABASE FINANCE_DB;

-- Create a file format for JSON data
CREATE FILE FORMAT IF NOT EXISTS FINANCE_DB.BRONZE.JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Create a file format for CSV data
CREATE FILE FORMAT IF NOT EXISTS FINANCE_DB.BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('NULL', 'null', '');

-- ============================================================
-- INTERNAL STAGES (for local file uploads / Kafka connector)
-- ============================================================

CREATE STAGE IF NOT EXISTS FINANCE_DB.BRONZE.STOCK_PRICES_STAGE
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

CREATE STAGE IF NOT EXISTS FINANCE_DB.BRONZE.CRYPTO_PRICES_STAGE
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

CREATE STAGE IF NOT EXISTS FINANCE_DB.BRONZE.NEWS_STAGE
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

-- ============================================================
-- SNOWPIPE DEFINITIONS
-- Auto-ingest from stages into Bronze tables
-- ============================================================

CREATE PIPE IF NOT EXISTS FINANCE_DB.BRONZE.STOCK_PRICES_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO FINANCE_DB.BRONZE.RAW_STOCK_PRICES (source, ticker, raw_data)
    FROM (
        SELECT
            'snowpipe',
            $1:ticker::VARCHAR,
            $1
        FROM @FINANCE_DB.BRONZE.STOCK_PRICES_STAGE
    )
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

CREATE PIPE IF NOT EXISTS FINANCE_DB.BRONZE.CRYPTO_PRICES_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO FINANCE_DB.BRONZE.RAW_CRYPTO_PRICES (source, ticker, raw_data)
    FROM (
        SELECT
            'snowpipe',
            $1:ticker::VARCHAR,
            $1
        FROM @FINANCE_DB.BRONZE.CRYPTO_PRICES_STAGE
    )
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

CREATE PIPE IF NOT EXISTS FINANCE_DB.BRONZE.NEWS_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO FINANCE_DB.BRONZE.RAW_FINANCIAL_NEWS
        (source, ticker, headline, article_text, article_url, published_at, raw_data)
    FROM (
        SELECT
            'snowpipe',
            $1:ticker::VARCHAR,
            $1:headline::VARCHAR,
            $1:article_text::VARCHAR,
            $1:url::VARCHAR,
            TRY_TO_TIMESTAMP($1:published_at::VARCHAR),
            $1
        FROM @FINANCE_DB.BRONZE.NEWS_STAGE
    )
    FILE_FORMAT = FINANCE_DB.BRONZE.JSON_FORMAT;

-- ============================================================
-- SNOWFLAKE TASKS (Scheduled Transformations - Bronze to Silver)
-- ============================================================

CREATE TASK IF NOT EXISTS FINANCE_DB.BRONZE.PROCESS_STOCK_STREAM
    WAREHOUSE = FINANCE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.STOCK_PRICES_STREAM')
    AS
    INSERT INTO FINANCE_DB.SILVER.STOCK_PRICES_CLEAN
    SELECT
        raw_data:date::TIMESTAMP_NTZ AS trade_date,
        ticker,
        raw_data:open::FLOAT AS open_price,
        raw_data:high::FLOAT AS high_price,
        raw_data:low::FLOAT AS low_price,
        raw_data:close::FLOAT AS close_price,
        raw_data:volume::INTEGER AS volume,
        source,
        CURRENT_TIMESTAMP() AS processed_at
    FROM BRONZE.STOCK_PRICES_STREAM
    WHERE METADATA$ACTION = 'INSERT';

-- Resume tasks (run manually after setup)
-- ALTER TASK FINANCE_DB.BRONZE.PROCESS_STOCK_STREAM RESUME;
