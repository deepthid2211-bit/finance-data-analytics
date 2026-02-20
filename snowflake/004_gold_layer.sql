-- ============================================================
-- GOLD LAYER - Business-Level Aggregations & Metrics
-- ============================================================

USE DATABASE FINANCE_DB;
USE SCHEMA GOLD;

-- Daily stock summary with technical indicators
CREATE TABLE IF NOT EXISTS GOLD.STOCK_DAILY_SUMMARY (
    trade_date DATE,
    ticker VARCHAR(20),
    company_name VARCHAR(200),
    sector VARCHAR(100),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume INTEGER,
    daily_return_pct FLOAT,
    cumulative_return_pct FLOAT,
    moving_avg_7d FLOAT,
    moving_avg_30d FLOAT,
    volatility_30d FLOAT,
    rsi_14 FLOAT,
    volume_avg_20d FLOAT,
    price_vs_52w_high FLOAT,
    price_vs_52w_low FLOAT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Crypto daily summary
CREATE TABLE IF NOT EXISTS GOLD.CRYPTO_DAILY_SUMMARY (
    trade_date DATE,
    ticker VARCHAR(20),
    crypto_name VARCHAR(50),
    close_price FLOAT,
    volume INTEGER,
    daily_return_pct FLOAT,
    moving_avg_7d FLOAT,
    moving_avg_30d FLOAT,
    volatility_30d FLOAT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Forex daily summary
CREATE TABLE IF NOT EXISTS GOLD.FOREX_DAILY_SUMMARY (
    trade_date DATE,
    pair VARCHAR(20),
    base_currency VARCHAR(3),
    quote_currency VARCHAR(3),
    close_rate FLOAT,
    daily_change_pct FLOAT,
    moving_avg_7d FLOAT,
    moving_avg_30d FLOAT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- News sentiment aggregation
CREATE TABLE IF NOT EXISTS GOLD.NEWS_SENTIMENT_DAILY (
    trade_date DATE,
    ticker VARCHAR(20),
    company_name VARCHAR(200),
    total_articles INTEGER,
    avg_sentiment_score FLOAT,
    positive_articles INTEGER,
    negative_articles INTEGER,
    neutral_articles INTEGER,
    sentiment_trend VARCHAR(20),
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sector performance
CREATE TABLE IF NOT EXISTS GOLD.SECTOR_PERFORMANCE (
    trade_date DATE,
    sector VARCHAR(100),
    total_stocks INTEGER,
    avg_daily_return FLOAT,
    best_performer VARCHAR(20),
    worst_performer VARCHAR(20),
    sector_volume BIGINT,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
