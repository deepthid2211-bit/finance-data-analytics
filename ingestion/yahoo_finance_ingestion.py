"""
Yahoo Finance Data Ingestion Module
Fetches stock prices, crypto, forex, company fundamentals, and news
and loads into Snowflake Bronze layer.
"""

import os
import json
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
from dotenv import load_dotenv
from loguru import logger

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.snowflake_config import get_snowflake_connection, execute_query_no_fetch

load_dotenv()

STOCK_TICKERS = os.getenv("YAHOO_STOCK_TICKERS", "AAPL,MSFT,GOOGL").split(",")
CRYPTO_TICKERS = os.getenv("YAHOO_CRYPTO_TICKERS", "BTC-USD,ETH-USD").split(",")
FOREX_PAIRS = os.getenv("YAHOO_FOREX_PAIRS", "EURUSD=X").split(",")


class YahooFinanceIngestion:
    """Handles all Yahoo Finance data extraction and loading to Snowflake Bronze layer."""

    def __init__(self):
        self.conn = get_snowflake_connection()
        self.conn.cursor().execute("USE DATABASE FINANCE_DB")
        self.conn.cursor().execute("USE SCHEMA BRONZE")

    def close(self):
        if self.conn:
            self.conn.close()

    # ----------------------------------------------------------
    # STOCK PRICES
    # ----------------------------------------------------------
    def ingest_stock_prices(self, period="5d", interval="1d"):
        """Fetch historical stock prices and load to Bronze."""
        logger.info(f"Ingesting stock prices for {len(STOCK_TICKERS)} tickers")
        for ticker in STOCK_TICKERS:
            try:
                stock = yf.Ticker(ticker.strip())
                hist = stock.history(period=period, interval=interval)

                if hist.empty:
                    logger.warning(f"No data for {ticker}")
                    continue

                for idx, row in hist.iterrows():
                    raw_data = {
                        "date": idx.isoformat(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row["Volume"]),
                    }
                    self._insert_stock_price(ticker.strip(), raw_data)

                logger.info(f"Loaded {len(hist)} records for {ticker}")
            except Exception as e:
                logger.error(f"Error ingesting {ticker}: {e}")

    def _insert_stock_price(self, ticker, raw_data):
        query = f"""
            INSERT INTO BRONZE.RAW_STOCK_PRICES (source, ticker, raw_data)
            SELECT 'yahoo_finance', '{ticker}', PARSE_JSON('{json.dumps(raw_data)}')
        """
        execute_query_no_fetch(self.conn, query)

    # ----------------------------------------------------------
    # CRYPTOCURRENCY PRICES
    # ----------------------------------------------------------
    def ingest_crypto_prices(self, period="5d", interval="1d"):
        """Fetch crypto prices and load to Bronze."""
        logger.info(f"Ingesting crypto prices for {len(CRYPTO_TICKERS)} tickers")
        for ticker in CRYPTO_TICKERS:
            try:
                crypto = yf.Ticker(ticker.strip())
                hist = crypto.history(period=period, interval=interval)

                if hist.empty:
                    logger.warning(f"No data for {ticker}")
                    continue

                for idx, row in hist.iterrows():
                    raw_data = {
                        "date": idx.isoformat(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row["Volume"]),
                    }
                    self._insert_crypto_price(ticker.strip(), raw_data)

                logger.info(f"Loaded {len(hist)} records for {ticker}")
            except Exception as e:
                logger.error(f"Error ingesting crypto {ticker}: {e}")

    def _insert_crypto_price(self, ticker, raw_data):
        query = f"""
            INSERT INTO BRONZE.RAW_CRYPTO_PRICES (source, ticker, raw_data)
            SELECT 'yahoo_finance', '{ticker}', PARSE_JSON('{json.dumps(raw_data)}')
        """
        execute_query_no_fetch(self.conn, query)

    # ----------------------------------------------------------
    # FOREX RATES
    # ----------------------------------------------------------
    def ingest_forex_rates(self, period="5d", interval="1d"):
        """Fetch forex exchange rates and load to Bronze."""
        logger.info(f"Ingesting forex rates for {len(FOREX_PAIRS)} pairs")
        for pair in FOREX_PAIRS:
            try:
                forex = yf.Ticker(pair.strip())
                hist = forex.history(period=period, interval=interval)

                if hist.empty:
                    logger.warning(f"No data for {pair}")
                    continue

                for idx, row in hist.iterrows():
                    raw_data = {
                        "date": idx.isoformat(),
                        "open": float(row["Open"]),
                        "high": float(row["High"]),
                        "low": float(row["Low"]),
                        "close": float(row["Close"]),
                        "volume": int(row.get("Volume", 0)),
                    }
                    self._insert_forex_rate(pair.strip(), raw_data)

                logger.info(f"Loaded {len(hist)} records for {pair}")
            except Exception as e:
                logger.error(f"Error ingesting forex {pair}: {e}")

    def _insert_forex_rate(self, pair, raw_data):
        query = f"""
            INSERT INTO BRONZE.RAW_FOREX_RATES (source, pair, raw_data)
            SELECT 'yahoo_finance', '{pair}', PARSE_JSON('{json.dumps(raw_data)}')
        """
        execute_query_no_fetch(self.conn, query)

    # ----------------------------------------------------------
    # COMPANY FUNDAMENTALS
    # ----------------------------------------------------------
    def ingest_company_fundamentals(self):
        """Fetch company info, financials, balance sheet, and cashflow."""
        logger.info(f"Ingesting fundamentals for {len(STOCK_TICKERS)} companies")
        for ticker in STOCK_TICKERS:
            try:
                stock = yf.Ticker(ticker.strip())

                # Company info
                info = stock.info
                if info:
                    safe_info = {k: str(v) for k, v in info.items() if v is not None}
                    self._insert_fundamental(ticker.strip(), "company_info", safe_info)

                # Income statement
                income = stock.financials
                if income is not None and not income.empty:
                    self._insert_fundamental(
                        ticker.strip(), "income_statement",
                        json.loads(income.to_json())
                    )

                # Balance sheet
                balance = stock.balance_sheet
                if balance is not None and not balance.empty:
                    self._insert_fundamental(
                        ticker.strip(), "balance_sheet",
                        json.loads(balance.to_json())
                    )

                # Cash flow
                cashflow = stock.cashflow
                if cashflow is not None and not cashflow.empty:
                    self._insert_fundamental(
                        ticker.strip(), "cash_flow",
                        json.loads(cashflow.to_json())
                    )

                logger.info(f"Loaded fundamentals for {ticker}")
            except Exception as e:
                logger.error(f"Error ingesting fundamentals for {ticker}: {e}")

    def _insert_fundamental(self, ticker, data_type, raw_data):
        escaped = json.dumps(raw_data).replace("'", "\\'")
        query = f"""
            INSERT INTO BRONZE.RAW_COMPANY_FUNDAMENTALS (source, ticker, data_type, raw_data)
            SELECT 'yahoo_finance', '{ticker}', '{data_type}', PARSE_JSON('{escaped}')
        """
        execute_query_no_fetch(self.conn, query)

    # ----------------------------------------------------------
    # FINANCIAL NEWS (Unstructured Data)
    # ----------------------------------------------------------
    def ingest_financial_news(self):
        """Fetch news headlines for each stock ticker."""
        logger.info(f"Ingesting news for {len(STOCK_TICKERS)} tickers")
        for ticker in STOCK_TICKERS:
            try:
                stock = yf.Ticker(ticker.strip())
                news = stock.news

                if not news:
                    logger.warning(f"No news for {ticker}")
                    continue

                for article in news:
                    content = article.get("content", {})
                    headline = content.get("title", "")
                    summary = content.get("summary", "")
                    pub_date = content.get("pubDate", "")
                    url = content.get("canonicalUrl", {}).get("url", "")

                    raw_data = {
                        "headline": headline,
                        "summary": summary,
                        "published_at": pub_date,
                        "url": url,
                        "provider": content.get("provider", {}).get("displayName", ""),
                        "ticker": ticker.strip(),
                    }
                    self._insert_news(ticker.strip(), raw_data)

                logger.info(f"Loaded {len(news)} news articles for {ticker}")
            except Exception as e:
                logger.error(f"Error ingesting news for {ticker}: {e}")

    def _insert_news(self, ticker, raw_data):
        headline = raw_data["headline"].replace("'", "''")
        summary = raw_data.get("summary", "").replace("'", "''")
        url = raw_data.get("url", "")
        pub_at = raw_data.get("published_at", "")
        escaped = json.dumps(raw_data).replace("'", "\\'")

        query = f"""
            INSERT INTO BRONZE.RAW_FINANCIAL_NEWS
                (source, ticker, headline, article_text, article_url, published_at, raw_data)
            SELECT
                'yahoo_finance',
                '{ticker}',
                '{headline}',
                '{summary}',
                '{url}',
                TRY_TO_TIMESTAMP('{pub_at}'),
                PARSE_JSON('{escaped}')
        """
        execute_query_no_fetch(self.conn, query)

    # ----------------------------------------------------------
    # FULL INGESTION
    # ----------------------------------------------------------
    def run_full_ingestion(self):
        """Run all ingestion pipelines."""
        logger.info("Starting full Yahoo Finance ingestion...")
        self.ingest_stock_prices(period="1mo")
        self.ingest_crypto_prices(period="1mo")
        self.ingest_forex_rates(period="1mo")
        self.ingest_company_fundamentals()
        self.ingest_financial_news()
        logger.info("Full ingestion complete.")


if __name__ == "__main__":
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.run_full_ingestion()
    finally:
        ingestion.close()
