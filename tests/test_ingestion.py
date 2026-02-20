"""Tests for the Yahoo Finance ingestion module."""

import os
import sys

# Ensure the project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def test_yahoo_finance_ingestion_class_exists():
    """Verify that YahooFinanceIngestion class exists in the ingestion module."""
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion

    assert YahooFinanceIngestion is not None


def test_yahoo_finance_ingestion_has_key_methods():
    """Verify that the ingestion class exposes expected methods."""
    expected_methods = [
        "ingest_stock_prices",
        "ingest_crypto_prices",
        "ingest_forex_rates",
        "ingest_company_fundamentals",
        "ingest_financial_news",
        "run_full_ingestion",
    ]
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion

    for method_name in expected_methods:
        assert hasattr(YahooFinanceIngestion, method_name), (
            f"YahooFinanceIngestion should have method '{method_name}'"
        )
