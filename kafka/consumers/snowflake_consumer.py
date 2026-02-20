"""
Kafka Consumer for Loading Streaming Data into Snowflake
Consumes messages from Kafka topics and loads into Snowflake Bronze layer.
"""

import os
import json
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from loguru import logger

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.snowflake_config import get_snowflake_connection, execute_query_no_fetch

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STOCK_TOPIC = os.getenv("KAFKA_TOPIC_STOCK_PRICES", "stock_prices")
CRYPTO_TOPIC = os.getenv("KAFKA_TOPIC_CRYPTO", "crypto_prices")
NEWS_TOPIC = os.getenv("KAFKA_TOPIC_NEWS", "finance_news")


class SnowflakeKafkaConsumer:
    """Consumes Kafka messages and loads them into Snowflake Bronze tables."""

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "snowflake-loader",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        })
        self.conn = get_snowflake_connection()
        self.conn.cursor().execute("USE DATABASE FINANCE_DB")
        self.conn.cursor().execute("USE SCHEMA BRONZE")

    def close(self):
        self.consumer.close()
        if self.conn:
            self.conn.close()

    def subscribe(self):
        """Subscribe to all finance topics."""
        topics = [STOCK_TOPIC, CRYPTO_TOPIC, NEWS_TOPIC]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def process_message(self, msg):
        """Route message to appropriate Snowflake table based on topic."""
        topic = msg.topic()
        value = json.loads(msg.value().decode("utf-8"))

        if topic == STOCK_TOPIC:
            self._load_stock_price(value)
        elif topic == CRYPTO_TOPIC:
            self._load_crypto_price(value)
        elif topic == NEWS_TOPIC:
            self._load_news(value)

    def _load_stock_price(self, data):
        """Load stock price message to Bronze."""
        ticker = data.get("ticker", "UNKNOWN")
        escaped = json.dumps(data).replace("'", "\\'")
        query = f"""
            INSERT INTO BRONZE.RAW_STOCK_PRICES (source, ticker, raw_data)
            SELECT 'kafka_stream', '{ticker}', PARSE_JSON('{escaped}')
        """
        execute_query_no_fetch(self.conn, query)
        logger.debug(f"Loaded stock price for {ticker}")

    def _load_crypto_price(self, data):
        """Load crypto price message to Bronze."""
        ticker = data.get("ticker", "UNKNOWN")
        escaped = json.dumps(data).replace("'", "\\'")
        query = f"""
            INSERT INTO BRONZE.RAW_CRYPTO_PRICES (source, ticker, raw_data)
            SELECT 'kafka_stream', '{ticker}', PARSE_JSON('{escaped}')
        """
        execute_query_no_fetch(self.conn, query)
        logger.debug(f"Loaded crypto price for {ticker}")

    def _load_news(self, data):
        """Load news message to Bronze."""
        ticker = data.get("ticker", "UNKNOWN")
        headline = data.get("headline", "").replace("'", "''")
        summary = data.get("summary", "").replace("'", "''")
        url = data.get("url", "")
        pub_at = data.get("published_at", "")
        escaped = json.dumps(data).replace("'", "\\'")
        query = f"""
            INSERT INTO BRONZE.RAW_FINANCIAL_NEWS
                (source, ticker, headline, article_text, article_url, published_at, raw_data)
            SELECT
                'kafka_stream',
                '{ticker}',
                '{headline}',
                '{summary}',
                '{url}',
                TRY_TO_TIMESTAMP('{pub_at}'),
                PARSE_JSON('{escaped}')
        """
        execute_query_no_fetch(self.conn, query)
        logger.debug(f"Loaded news for {ticker}")

    def run(self):
        """Main consumer loop."""
        self.subscribe()
        logger.info("Consumer started. Waiting for messages...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                self.process_message(msg)

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")
        finally:
            self.close()


if __name__ == "__main__":
    consumer = SnowflakeKafkaConsumer()
    consumer.run()
