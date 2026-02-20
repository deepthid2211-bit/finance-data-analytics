-- ============================================================
-- FINANCE DATA PROJECT - SNOWFLAKE DATABASE SETUP
-- Medallion Architecture: Bronze -> Silver -> Gold -> Marts
-- ============================================================

-- Create the warehouse
CREATE WAREHOUSE IF NOT EXISTS FINANCE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Create the database
CREATE DATABASE IF NOT EXISTS FINANCE_DB;

USE DATABASE FINANCE_DB;

-- ============================================================
-- BRONZE LAYER (Raw Data - As-Is from Sources)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- Raw stock prices (structured)
CREATE TABLE IF NOT EXISTS BRONZE.RAW_STOCK_PRICES (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(50),
    ticker VARCHAR(20),
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw cryptocurrency prices (structured)
CREATE TABLE IF NOT EXISTS BRONZE.RAW_CRYPTO_PRICES (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(50),
    ticker VARCHAR(20),
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw forex rates (structured)
CREATE TABLE IF NOT EXISTS BRONZE.RAW_FOREX_RATES (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(50),
    pair VARCHAR(20),
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw company fundamentals (structured)
CREATE TABLE IF NOT EXISTS BRONZE.RAW_COMPANY_FUNDAMENTALS (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(50),
    ticker VARCHAR(20),
    data_type VARCHAR(50),
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw financial news (unstructured)
CREATE TABLE IF NOT EXISTS BRONZE.RAW_FINANCIAL_NEWS (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(100),
    ticker VARCHAR(20),
    headline VARCHAR(1000),
    article_text VARCHAR,
    article_url VARCHAR(2000),
    published_at TIMESTAMP_NTZ,
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Raw economic indicators
CREATE TABLE IF NOT EXISTS BRONZE.RAW_ECONOMIC_INDICATORS (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source VARCHAR(50),
    indicator_name VARCHAR(100),
    raw_data VARIANT,
    _loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- SILVER LAYER (Cleansed, Validated, Enriched)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS SILVER;

-- ============================================================
-- GOLD LAYER (Business-Level Aggregations)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS GOLD;

-- ============================================================
-- DATA MARTS (Domain-Specific Views for Tableau)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS MARTS;

-- ============================================================
-- STAGING SCHEMA (for dbt intermediate models)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS STAGING;

-- ============================================================
-- STREAMS & TASKS (for Real-Time / CDC)
-- ============================================================

-- Stream on bronze stock prices for CDC
CREATE STREAM IF NOT EXISTS BRONZE.STOCK_PRICES_STREAM
    ON TABLE BRONZE.RAW_STOCK_PRICES
    SHOW_INITIAL_ROWS = TRUE;

-- Stream on bronze news for CDC
CREATE STREAM IF NOT EXISTS BRONZE.NEWS_STREAM
    ON TABLE BRONZE.RAW_FINANCIAL_NEWS
    SHOW_INITIAL_ROWS = TRUE;

-- Stream on bronze crypto for CDC
CREATE STREAM IF NOT EXISTS BRONZE.CRYPTO_PRICES_STREAM
    ON TABLE BRONZE.RAW_CRYPTO_PRICES
    SHOW_INITIAL_ROWS = TRUE;
