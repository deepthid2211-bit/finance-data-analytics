-- ============================================================
-- TABLEAU CONNECTION GUIDE
-- Connect Tableau to Snowflake Data Marts
-- ============================================================

-- STEP 1: In Tableau Desktop -> Connect -> Snowflake
-- Server:    VYUVIGG-RVB11850.snowflakecomputing.com
-- Warehouse: FINANCE_WH
-- Database:  FINANCE_DB
-- Schema:    MARTS
-- Auth:      Username/Password

-- ============================================================
-- RECOMMENDED DATA SOURCES FOR TABLEAU DASHBOARDS
-- ============================================================

-- DASHBOARD 1: Portfolio Performance Dashboard
-- Data Source: MARTS.VW_PORTFOLIO_PERFORMANCE
-- Charts:
--   - Line chart: Stock price trends over time
--   - Bar chart: Daily returns by ticker
--   - Heatmap: Sector performance
--   - KPI cards: Top gainer, top loser, avg return
--   - Scatter plot: Return vs Volatility

-- DASHBOARD 2: Crypto Market Dashboard
-- Data Source: MARTS.VW_CRYPTO_MARKET
-- Charts:
--   - Candlestick chart: Crypto price movements
--   - Area chart: Volume trends
--   - Line chart: Moving averages (7d vs 30d)
--   - Table: Daily crypto summary

-- DASHBOARD 3: Forex Overview Dashboard
-- Data Source: MARTS.VW_FOREX_OVERVIEW
-- Charts:
--   - Line chart: Currency pair trends
--   - Bar chart: Daily change percentages
--   - Table: Exchange rate summary

-- DASHBOARD 4: Sector Analysis Dashboard
-- Data Source: MARTS.VW_SECTOR_ANALYSIS
-- Charts:
--   - Treemap: Sector by volume
--   - Bar chart: Average daily return by sector
--   - Highlight table: Best/worst performers

-- DASHBOARD 5: News Sentiment Dashboard
-- Data Source: MARTS.VW_NEWS_SENTIMENT
-- Charts:
--   - Dual axis: Sentiment score vs stock price
--   - Stacked bar: Positive/Negative/Neutral articles
--   - Bubble chart: Sentiment by ticker and article count
--   - Word cloud: Key entities

-- DASHBOARD 6: Executive Summary
-- Data Source: MARTS.VW_EXECUTIVE_SUMMARY
-- Charts:
--   - KPI cards: Total stocks, avg return, volatility
--   - Sparklines: Market trend
--   - Top/Bottom 5 tables

-- ============================================================
-- SAMPLE QUERIES TO TEST IN TABLEAU
-- ============================================================

-- Test portfolio performance
SELECT * FROM FINANCE_DB.MARTS.VW_PORTFOLIO_PERFORMANCE
ORDER BY trade_date DESC, ticker
LIMIT 100;

-- Test sector analysis
SELECT * FROM FINANCE_DB.MARTS.VW_SECTOR_ANALYSIS
ORDER BY trade_date DESC
LIMIT 50;

-- Test executive summary
SELECT * FROM FINANCE_DB.MARTS.VW_EXECUTIVE_SUMMARY
ORDER BY trade_date DESC
LIMIT 30;
