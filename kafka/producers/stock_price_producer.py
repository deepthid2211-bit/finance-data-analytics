"""
Kafka Producer for Real-Time Stock Price Streaming
Streams live stock price data from Yahoo Finance to Kafka topics.
"""

import json
import os
import time
from datetime import datetime

import yfinance as yf
from confluent_kafka import Producer
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STOCK_TOPIC = os.getenv("KAFKA_TOPIC_STOCK_PRICES", "stock_prices")
CRYPTO_TOPIC = os.getenv("KAFKA_TOPIC_CRYPTO", "crypto_prices")
NEWS_TOPIC = os.getenv("KAFKA_TOPIC_NEWS", "finance_news")

STOCK_TICKERS = os.getenv("YAHOO_STOCK_TICKERS", "AAPL,MSFT,GOOGL").split(",")
CRYPTO_TICKERS = os.getenv("YAHOO_CRYPTO_TICKERS", "BTC-USD,ETH-USD").split(",")


def delivery_callback(err, msg):
    """Kafka delivery callback."""
    if err:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


class FinanceKafkaProducer:
    """Produces real-time finance data to Kafka topics."""

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "finance-producer",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
        })

    def produce_stock_prices(self):
        """Stream real-time stock prices to Kafka."""
        logger.info(f"Streaming stock prices for {len(STOCK_TICKERS)} tickers")
        for ticker in STOCK_TICKERS:
            try:
                stock = yf.Ticker(ticker.strip())
                info = stock.info or {}

                message = {
                    "ticker": ticker.strip(),
                    "timestamp": datetime.utcnow().isoformat(),
                    "current_price": info.get("currentPrice") or info.get("regularMarketPrice"),
                    "previous_close": info.get("previousClose") or info.get("regularMarketPreviousClose"),
                    "open": info.get("open") or info.get("regularMarketOpen"),
                    "day_high": info.get("dayHigh") or info.get("regularMarketDayHigh"),
                    "day_low": info.get("dayLow") or info.get("regularMarketDayLow"),
                    "volume": info.get("volume") or info.get("regularMarketVolume"),
                    "market_cap": info.get("marketCap"),
                    "source": "yahoo_finance_realtime",
                }

                self.producer.produce(
                    STOCK_TOPIC,
                    key=ticker.strip(),
                    value=json.dumps(message),
                    callback=delivery_callback,
                )
                logger.info(f"Produced stock price for {ticker}: ${message.get('current_price')}")

            except Exception as e:
                logger.error(f"Error producing {ticker}: {e}")

        self.producer.flush()

    def produce_crypto_prices(self):
        """Stream real-time crypto prices to Kafka."""
        logger.info(f"Streaming crypto prices for {len(CRYPTO_TICKERS)} tickers")
        for ticker in CRYPTO_TICKERS:
            try:
                crypto = yf.Ticker(ticker.strip())
                fast_info = crypto.fast_info

                message = {
                    "ticker": ticker.strip(),
                    "timestamp": datetime.utcnow().isoformat(),
                    "current_price": fast_info.get("last_price"),
                    "previous_close": fast_info.get("previous_close"),
                    "open": fast_info.get("open"),
                    "day_high": fast_info.get("day_high"),
                    "day_low": fast_info.get("day_low"),
                    "volume": fast_info.get("last_volume"),
                    "market_cap": fast_info.get("market_cap"),
                    "source": "yahoo_finance_realtime",
                }

                self.producer.produce(
                    CRYPTO_TOPIC,
                    key=ticker.strip(),
                    value=json.dumps(message),
                    callback=delivery_callback,
                )
                logger.info(f"Produced crypto price for {ticker}: ${message.get('current_price')}")

            except Exception as e:
                logger.error(f"Error producing crypto {ticker}: {e}")

        self.producer.flush()

    def produce_news(self):
        """Stream financial news to Kafka."""
        logger.info("Streaming financial news...")
        for ticker in STOCK_TICKERS:
            try:
                stock = yf.Ticker(ticker.strip())
                news = stock.news or []

                for article in news:
                    content = article.get("content", {})
                    message = {
                        "ticker": ticker.strip(),
                        "timestamp": datetime.utcnow().isoformat(),
                        "headline": content.get("title", ""),
                        "summary": content.get("summary", ""),
                        "url": content.get("canonicalUrl", {}).get("url", ""),
                        "published_at": content.get("pubDate", ""),
                        "provider": content.get("provider", {}).get("displayName", ""),
                        "source": "yahoo_finance_news",
                    }

                    self.producer.produce(
                        NEWS_TOPIC,
                        key=ticker.strip(),
                        value=json.dumps(message),
                        callback=delivery_callback,
                    )

                logger.info(f"Produced {len(news)} news articles for {ticker}")

            except Exception as e:
                logger.error(f"Error producing news for {ticker}: {e}")

        self.producer.flush()

    def run_continuous(self, interval_seconds=60):
        """Run continuous streaming at specified intervals."""
        logger.info(f"Starting continuous streaming (interval: {interval_seconds}s)")
        while True:
            try:
                self.produce_stock_prices()
                self.produce_crypto_prices()
                self.produce_news()
                logger.info(f"Cycle complete. Sleeping {interval_seconds}s...")
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                logger.info("Streaming stopped by user.")
                break
            except Exception as e:
                logger.error(f"Error in streaming cycle: {e}")
                time.sleep(10)


if __name__ == "__main__":
    producer = FinanceKafkaProducer()
    producer.run_continuous(interval_seconds=60)
