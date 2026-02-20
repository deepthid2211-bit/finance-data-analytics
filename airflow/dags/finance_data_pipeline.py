"""
Apache Airflow DAG - Finance Data Pipeline
Orchestrates the full medallion architecture:
1. Ingest from Yahoo Finance -> Bronze
2. Process unstructured data (NLP) -> Silver
3. Run dbt transformations -> Silver/Gold/Marts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# ----------------------------------------------------------
# Default Args
# ----------------------------------------------------------
default_args = {
    "owner": "deepthi",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}

PROJECT_DIR = "/Users/deepthi/Streaming project"
DBT_DIR = f"{PROJECT_DIR}/dbt/finance_analytics"


# ----------------------------------------------------------
# Python Callables
# ----------------------------------------------------------
def run_stock_ingestion():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.ingest_stock_prices(period="1d")
    finally:
        ingestion.close()


def run_crypto_ingestion():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.ingest_crypto_prices(period="1d")
    finally:
        ingestion.close()


def run_forex_ingestion():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.ingest_forex_rates(period="1d")
    finally:
        ingestion.close()


def run_fundamentals_ingestion():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.ingest_company_fundamentals()
    finally:
        ingestion.close()


def run_news_ingestion():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from ingestion.yahoo_finance_ingestion import YahooFinanceIngestion
    ingestion = YahooFinanceIngestion()
    try:
        ingestion.ingest_financial_news()
    finally:
        ingestion.close()


def run_sentiment_processing():
    import sys
    sys.path.insert(0, PROJECT_DIR)
    from unstructured.news_sentiment_processor import NewsSentimentProcessor
    processor = NewsSentimentProcessor()
    try:
        processor.process_and_load()
    finally:
        processor.close()


# ----------------------------------------------------------
# DAG: Daily Finance Pipeline
# ----------------------------------------------------------
with DAG(
    dag_id="finance_daily_pipeline",
    default_args=default_args,
    description="Daily finance data pipeline: Ingest -> Process -> Transform",
    schedule_interval="0 6 * * 1-5",  # Weekdays at 6 AM
    catchup=False,
    tags=["finance", "medallion", "dbt"],
    max_active_runs=1,
) as dag:

    # ---- STEP 1: Data Ingestion (Bronze Layer) ----
    with TaskGroup("bronze_ingestion") as ingestion_group:
        ingest_stocks = PythonOperator(
            task_id="ingest_stock_prices",
            python_callable=run_stock_ingestion,
        )
        ingest_crypto = PythonOperator(
            task_id="ingest_crypto_prices",
            python_callable=run_crypto_ingestion,
        )
        ingest_forex = PythonOperator(
            task_id="ingest_forex_rates",
            python_callable=run_forex_ingestion,
        )
        ingest_fundamentals = PythonOperator(
            task_id="ingest_fundamentals",
            python_callable=run_fundamentals_ingestion,
        )
        ingest_news = PythonOperator(
            task_id="ingest_news",
            python_callable=run_news_ingestion,
        )

    # ---- STEP 2: Unstructured Data Processing ----
    process_sentiment = PythonOperator(
        task_id="process_news_sentiment",
        python_callable=run_sentiment_processing,
    )

    # ---- STEP 3: dbt Transformations (Silver -> Gold -> Marts) ----
    with TaskGroup("dbt_transformations") as dbt_group:
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
        )
        dbt_run_staging = BashOperator(
            task_id="dbt_run_staging",
            bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_DIR}",
        )
        dbt_run_silver = BashOperator(
            task_id="dbt_run_silver",
            bash_command=f"cd {DBT_DIR} && dbt run --select silver --profiles-dir {DBT_DIR}",
        )
        dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=f"cd {DBT_DIR} && dbt run --select gold --profiles-dir {DBT_DIR}",
        )
        dbt_run_marts = BashOperator(
            task_id="dbt_run_marts",
            bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_DIR}",
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
        )

        dbt_deps >> dbt_run_staging >> dbt_run_silver >> dbt_run_gold >> dbt_run_marts >> dbt_test

    # ---- DAG Dependencies ----
    ingestion_group >> process_sentiment >> dbt_group


# ----------------------------------------------------------
# DAG: Weekly Fundamentals Refresh
# ----------------------------------------------------------
with DAG(
    dag_id="finance_weekly_fundamentals",
    default_args=default_args,
    description="Weekly refresh of company fundamentals",
    schedule_interval="0 8 * * 6",  # Saturday at 8 AM
    catchup=False,
    tags=["finance", "fundamentals"],
    max_active_runs=1,
) as weekly_dag:

    refresh_fundamentals = PythonOperator(
        task_id="refresh_company_fundamentals",
        python_callable=run_fundamentals_ingestion,
    )

    dbt_refresh = BashOperator(
        task_id="dbt_refresh_all",
        bash_command=f"cd {DBT_DIR} && dbt run --full-refresh --profiles-dir {DBT_DIR}",
    )

    refresh_fundamentals >> dbt_refresh
