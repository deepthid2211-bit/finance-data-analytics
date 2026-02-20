"""
One-time Snowflake setup script.
Runs all SQL scripts to create the database, schemas, tables, and objects.
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
from loguru import logger

from config.snowflake_config import get_snowflake_connection

load_dotenv()

SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "snowflake")

SQL_FILES = [
    "001_setup_database.sql",
    "002_snowpipe_setup.sql",
    "003_silver_layer.sql",
    "004_gold_layer.sql",
    "005_data_marts.sql",
]


def parse_sql_statements(content):
    """Parse SQL file content into individual executable statements.
    Handles comments and multi-line statements properly.
    """
    # Remove block comments
    # Remove single-line comments (lines starting with --)
    lines = content.split("\n")
    cleaned_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        # Remove inline comments
        if "--" in line:
            line = line[:line.index("--")]
        cleaned_lines.append(line)

    cleaned = "\n".join(cleaned_lines)

    # Split by semicolons
    raw_statements = cleaned.split(";")

    statements = []
    for stmt in raw_statements:
        stmt = stmt.strip()
        if stmt and len(stmt) > 3:  # Skip empty or tiny fragments
            statements.append(stmt)

    return statements


def run_sql_file(conn, filepath):
    """Execute all statements in a SQL file."""
    logger.info(f"Running: {os.path.basename(filepath)}")

    with open(filepath, "r") as f:
        content = f.read()

    statements = parse_sql_statements(content)
    logger.info(f"  Found {len(statements)} statements")

    cursor = conn.cursor()
    success = 0
    for i, stmt in enumerate(statements):
        try:
            cursor.execute(stmt)
            success += 1
            # Log first 80 chars of each statement
            preview = " ".join(stmt.split())[:80]
            logger.info(f"  [{i+1}/{len(statements)}] OK: {preview}")
        except Exception as e:
            preview = " ".join(stmt.split())[:80]
            logger.warning(f"  [{i+1}/{len(statements)}] WARN: {preview}")
            logger.warning(f"    Error: {e}")

    cursor.close()
    logger.info(f"Completed: {os.path.basename(filepath)} ({success}/{len(statements)} succeeded)")


def main():
    logger.info("=" * 60)
    logger.info("SNOWFLAKE DATABASE SETUP")
    logger.info("=" * 60)

    conn = get_snowflake_connection()

    try:
        for sql_file in SQL_FILES:
            filepath = os.path.join(SQL_DIR, sql_file)
            if os.path.exists(filepath):
                run_sql_file(conn, filepath)
            else:
                logger.warning(f"File not found: {filepath}")

        # Verify setup
        logger.info("=" * 60)
        logger.info("VERIFYING SETUP...")
        cursor = conn.cursor()

        cursor.execute("USE DATABASE FINANCE_DB")
        cursor.execute("SHOW SCHEMAS IN DATABASE FINANCE_DB")
        schemas = cursor.fetchall()
        logger.info(f"Schemas created: {[s[1] for s in schemas]}")

        cursor.execute("SHOW TABLES IN SCHEMA BRONZE")
        tables = cursor.fetchall()
        logger.info(f"Bronze tables: {[t[1] for t in tables]}")

        cursor.execute("SHOW TABLES IN SCHEMA SILVER")
        tables = cursor.fetchall()
        logger.info(f"Silver tables: {[t[1] for t in tables]}")

        cursor.execute("SHOW TABLES IN SCHEMA GOLD")
        tables = cursor.fetchall()
        logger.info(f"Gold tables: {[t[1] for t in tables]}")

        cursor.close()
        logger.info("=" * 60)
        logger.info("SETUP COMPLETE!")
        logger.info("=" * 60)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
