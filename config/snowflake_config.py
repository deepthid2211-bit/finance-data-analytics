"""Snowflake connection configuration and helper utilities."""

import os
from dotenv import load_dotenv
import snowflake.connector
from loguru import logger

load_dotenv()


def get_snowflake_connection():
    """Create and return a Snowflake connection using .env credentials."""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        logger.info("Snowflake connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def execute_query(conn, query, params=None):
    """Execute a query on Snowflake and return results."""
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    finally:
        cursor.close()


def execute_query_no_fetch(conn, query):
    """Execute a DDL/DML query without fetching results."""
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        logger.info(f"Query executed: {query[:80]}...")
    finally:
        cursor.close()
