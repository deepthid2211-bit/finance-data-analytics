"""
Full Pipeline Runner
Executes the complete data pipeline end-to-end:
1. Ingest data from Yahoo Finance -> Bronze
2. Process unstructured data (sentiment) -> Silver
3. Run dbt transformations -> Silver/Gold/Marts
"""

import os
import subprocess
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from loguru import logger

load_dotenv()

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.path.join(PROJECT_DIR, "dbt", "finance_analytics")


def step_1_ingest():
    """Run Yahoo Finance ingestion."""
    logger.info("=" * 60)
    logger.info("STEP 1: Data Ingestion (Yahoo Finance -> Bronze)")
    logger.info("=" * 60)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion

    ingestion = YahooFinanceIngestion()
    try:
        ingestion.run_full_ingestion()
    finally:
        ingestion.close()


def step_2_process_unstructured():
    """Run NLP processing on news articles."""
    logger.info("=" * 60)
    logger.info("STEP 2: Unstructured Data Processing (NLP Sentiment)")
    logger.info("=" * 60)
    from unstructured.news_sentiment_processor import NewsSentimentProcessor

    processor = NewsSentimentProcessor()
    try:
        count = processor.process_and_load()
        logger.info(f"Processed {count} news articles")
    finally:
        processor.close()


def step_3_dbt_transform():
    """Run dbt transformations."""
    logger.info("=" * 60)
    logger.info("STEP 3: dbt Transformations (Silver -> Gold -> Marts)")
    logger.info("=" * 60)

    commands = [
        f"cd '{DBT_DIR}' && dbt deps --profiles-dir '{DBT_DIR}'",
        f"cd '{DBT_DIR}' && dbt run --profiles-dir '{DBT_DIR}'",
        f"cd '{DBT_DIR}' && dbt test --profiles-dir '{DBT_DIR}'",
    ]

    for cmd in commands:
        logger.info(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"dbt command failed: {result.stderr}")
        else:
            logger.info(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)


def main():
    logger.info("Starting Full Finance Data Pipeline")
    logger.info("=" * 60)

    step_1_ingest()
    step_2_process_unstructured()
    step_3_dbt_transform()

    logger.info("=" * 60)
    logger.info("Pipeline complete! Data is ready for Tableau.")
    logger.info("Connect Tableau to: FINANCE_DB.MARTS schema")


if __name__ == "__main__":
    main()
