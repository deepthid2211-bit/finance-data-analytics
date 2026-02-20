-- ============================================================
-- SILVER LAYER - Cleansed, Validated, Typed Data
-- ============================================================

USE DATABASE FINANCE_DB;
USE SCHEMA SILVER;

-- Clean stock prices
CREATE TABLE IF NOT EXISTS SILVER.STOCK_PRICES_CLEAN (
    trade_date TIMESTAMP_NTZ,
    ticker VARCHAR(20),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume INTEGER,
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stock_prices PRIMARY KEY (ticker, trade_date)
);

-- Clean crypto prices
CREATE TABLE IF NOT EXISTS SILVER.CRYPTO_PRICES_CLEAN (
    trade_date TIMESTAMP_NTZ,
    ticker VARCHAR(20),
    crypto_name VARCHAR(50),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume INTEGER,
    market_cap FLOAT,
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_crypto_prices PRIMARY KEY (ticker, trade_date)
);

-- Clean forex rates
CREATE TABLE IF NOT EXISTS SILVER.FOREX_RATES_CLEAN (
    trade_date TIMESTAMP_NTZ,
    base_currency VARCHAR(3),
    quote_currency VARCHAR(3),
    pair VARCHAR(20),
    open_rate FLOAT,
    high_rate FLOAT,
    low_rate FLOAT,
    close_rate FLOAT,
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_forex_rates PRIMARY KEY (pair, trade_date)
);

-- Clean company fundamentals
CREATE TABLE IF NOT EXISTS SILVER.COMPANY_PROFILES (
    ticker VARCHAR(20),
    company_name VARCHAR(200),
    sector VARCHAR(100),
    industry VARCHAR(200),
    market_cap FLOAT,
    pe_ratio FLOAT,
    forward_pe FLOAT,
    dividend_yield FLOAT,
    beta FLOAT,
    fifty_two_week_high FLOAT,
    fifty_two_week_low FLOAT,
    avg_volume INTEGER,
    currency VARCHAR(10),
    exchange VARCHAR(50),
    country VARCHAR(50),
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_company_profile PRIMARY KEY (ticker)
);

-- Clean financial statements
CREATE TABLE IF NOT EXISTS SILVER.FINANCIAL_STATEMENTS (
    ticker VARCHAR(20),
    statement_type VARCHAR(50),
    period_date DATE,
    metric_name VARCHAR(200),
    metric_value FLOAT,
    currency VARCHAR(10),
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Clean news with sentiment (from unstructured processing)
CREATE TABLE IF NOT EXISTS SILVER.NEWS_ENRICHED (
    news_id VARCHAR(100),
    ticker VARCHAR(20),
    headline VARCHAR(1000),
    summary VARCHAR,
    article_url VARCHAR(2000),
    published_at TIMESTAMP_NTZ,
    source_provider VARCHAR(100),
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    subjectivity_score FLOAT,
    key_entities VARIANT,
    source VARCHAR(50),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
