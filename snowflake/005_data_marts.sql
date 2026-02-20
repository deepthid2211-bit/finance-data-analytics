-- ============================================================
-- DATA MARTS - Domain-Specific Views for Tableau
-- ============================================================

USE DATABASE FINANCE_DB;
USE SCHEMA MARTS;

-- ============================================================
-- MART 1: Portfolio Performance Mart
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_PORTFOLIO_PERFORMANCE AS
SELECT
    s.trade_date,
    s.ticker,
    s.company_name,
    s.sector,
    s.close_price,
    s.daily_return_pct,
    s.cumulative_return_pct,
    s.moving_avg_7d,
    s.moving_avg_30d,
    s.volatility_30d,
    s.volume,
    cp.market_cap,
    cp.pe_ratio,
    cp.dividend_yield,
    cp.beta,
    ns.avg_sentiment_score,
    ns.total_articles AS news_count
FROM GOLD.STOCK_DAILY_SUMMARY s
LEFT JOIN SILVER.COMPANY_PROFILES cp ON s.ticker = cp.ticker
LEFT JOIN GOLD.NEWS_SENTIMENT_DAILY ns
    ON s.ticker = ns.ticker AND s.trade_date = ns.trade_date;

-- ============================================================
-- MART 2: Crypto Market Mart
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_CRYPTO_MARKET AS
SELECT
    trade_date,
    ticker,
    crypto_name,
    close_price,
    volume,
    daily_return_pct,
    moving_avg_7d,
    moving_avg_30d,
    volatility_30d
FROM GOLD.CRYPTO_DAILY_SUMMARY;

-- ============================================================
-- MART 3: Forex Overview Mart
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_FOREX_OVERVIEW AS
SELECT
    trade_date,
    pair,
    base_currency,
    quote_currency,
    close_rate,
    daily_change_pct,
    moving_avg_7d,
    moving_avg_30d
FROM GOLD.FOREX_DAILY_SUMMARY;

-- ============================================================
-- MART 4: Sector Analysis Mart
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_SECTOR_ANALYSIS AS
SELECT
    sp.trade_date,
    sp.sector,
    sp.total_stocks,
    sp.avg_daily_return,
    sp.best_performer,
    sp.worst_performer,
    sp.sector_volume,
    bp.company_name AS best_performer_name,
    bp.close_price AS best_performer_price,
    wp.company_name AS worst_performer_name,
    wp.close_price AS worst_performer_price
FROM GOLD.SECTOR_PERFORMANCE sp
LEFT JOIN GOLD.STOCK_DAILY_SUMMARY bp
    ON sp.best_performer = bp.ticker AND sp.trade_date = bp.trade_date
LEFT JOIN GOLD.STOCK_DAILY_SUMMARY wp
    ON sp.worst_performer = wp.ticker AND sp.trade_date = wp.trade_date;

-- ============================================================
-- MART 5: News Sentiment Mart
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_NEWS_SENTIMENT AS
SELECT
    n.trade_date,
    n.ticker,
    n.company_name,
    n.total_articles,
    n.avg_sentiment_score,
    n.positive_articles,
    n.negative_articles,
    n.neutral_articles,
    n.sentiment_trend,
    s.close_price,
    s.daily_return_pct,
    cp.sector,
    cp.industry
FROM GOLD.NEWS_SENTIMENT_DAILY n
LEFT JOIN GOLD.STOCK_DAILY_SUMMARY s
    ON n.ticker = s.ticker AND n.trade_date = s.trade_date
LEFT JOIN SILVER.COMPANY_PROFILES cp
    ON n.ticker = cp.ticker;

-- ============================================================
-- MART 6: Executive Summary Mart (KPIs)
-- ============================================================
CREATE OR REPLACE VIEW MARTS.VW_EXECUTIVE_SUMMARY AS
SELECT
    trade_date,
    COUNT(DISTINCT ticker) AS total_stocks_tracked,
    AVG(daily_return_pct) AS market_avg_return,
    MAX(daily_return_pct) AS top_gainer_return,
    MIN(daily_return_pct) AS top_loser_return,
    SUM(volume) AS total_market_volume,
    AVG(volatility_30d) AS avg_market_volatility
FROM GOLD.STOCK_DAILY_SUMMARY
GROUP BY trade_date;
