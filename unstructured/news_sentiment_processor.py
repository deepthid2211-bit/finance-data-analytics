"""
Unstructured Data Processing Module
Processes financial news articles with NLP:
- Sentiment analysis (TextBlob)
- Entity extraction
- Text classification
Loads enriched data into Snowflake Silver layer.
"""

import os
import json
import hashlib
from datetime import datetime

from textblob import TextBlob
from loguru import logger
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.snowflake_config import get_snowflake_connection, execute_query, execute_query_no_fetch

load_dotenv()


class NewsSentimentProcessor:
    """Process raw financial news with NLP and load to Silver layer."""

    def __init__(self):
        self.conn = get_snowflake_connection()
        self.conn.cursor().execute("USE DATABASE FINANCE_DB")

    def close(self):
        if self.conn:
            self.conn.close()

    def fetch_unprocessed_news(self, limit=500):
        """Fetch news from Bronze that hasn't been processed into Silver yet."""
        query = """
            SELECT
                b.ticker,
                b.headline,
                b.article_text,
                b.article_url,
                b.published_at,
                b.raw_data,
                b._loaded_at
            FROM BRONZE.RAW_FINANCIAL_NEWS b
            LEFT JOIN SILVER.NEWS_ENRICHED s
                ON s.ticker = b.ticker AND s.headline = b.headline
            WHERE s.news_id IS NULL
              AND b.headline IS NOT NULL
              AND b.headline != ''
            ORDER BY b._loaded_at DESC
            LIMIT {limit}
        """.format(limit=limit)
        return execute_query(self.conn, query)

    def analyze_sentiment(self, text):
        """Perform sentiment analysis using TextBlob."""
        if not text or text.strip() == "":
            return 0.0, "neutral", 0.0

        blob = TextBlob(text)
        polarity = blob.sentiment.polarity
        subjectivity = blob.sentiment.subjectivity

        if polarity > 0.1:
            label = "positive"
        elif polarity < -0.1:
            label = "negative"
        else:
            label = "neutral"

        return round(polarity, 4), label, round(subjectivity, 4)

    def extract_entities(self, text):
        """Extract key financial entities from text."""
        if not text:
            return []

        blob = TextBlob(text)
        # Extract noun phrases as key entities
        entities = list(set(blob.noun_phrases))
        return entities[:20]  # Limit to top 20

    def generate_news_id(self, ticker, headline, published_at):
        """Generate a deterministic unique ID for a news article."""
        raw = f"{ticker}:{headline}:{published_at}"
        return hashlib.md5(raw.encode()).hexdigest()

    def process_and_load(self):
        """Main processing pipeline: fetch, analyze, load to Silver."""
        logger.info("Fetching unprocessed news from Bronze...")
        rows = self.fetch_unprocessed_news()

        if not rows:
            logger.info("No unprocessed news found.")
            return 0

        logger.info(f"Processing {len(rows)} news articles...")
        processed = 0

        for row in rows:
            try:
                ticker = row[0]
                headline = row[1] or ""
                article_text = row[2] or ""
                article_url = row[3] or ""
                published_at = row[4]
                raw_data = row[5]

                # Combine headline + article for better sentiment
                full_text = f"{headline}. {article_text}" if article_text else headline

                # Sentiment analysis
                sentiment_score, sentiment_label, subjectivity = self.analyze_sentiment(full_text)

                # Entity extraction
                entities = self.extract_entities(full_text)

                # Generate unique ID
                news_id = self.generate_news_id(ticker, headline, str(published_at))

                # Determine provider from raw_data
                provider = ""
                if raw_data:
                    try:
                        data = json.loads(raw_data) if isinstance(raw_data, str) else raw_data
                        provider = data.get("provider", "")
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Load to Silver
                self._insert_enriched_news(
                    news_id=news_id,
                    ticker=ticker,
                    headline=headline,
                    summary=article_text,
                    article_url=article_url,
                    published_at=published_at,
                    source_provider=provider,
                    sentiment_score=sentiment_score,
                    sentiment_label=sentiment_label,
                    subjectivity=subjectivity,
                    entities=entities,
                )
                processed += 1

            except Exception as e:
                logger.error(f"Error processing article '{headline[:50]}...': {e}")

        logger.info(f"Processed {processed}/{len(rows)} articles into Silver layer.")
        return processed

    def _insert_enriched_news(self, **kwargs):
        """Insert enriched news record into Silver.NEWS_ENRICHED."""
        headline = kwargs["headline"].replace("'", "''")
        summary = (kwargs["summary"] or "").replace("'", "''")
        provider = str(kwargs["source_provider"]).replace("'", "''")
        entities_json = json.dumps(kwargs["entities"])
        pub_at = kwargs["published_at"] if kwargs["published_at"] else None
        pub_at_str = f"'{pub_at}'" if pub_at else "NULL"

        query = f"""
            INSERT INTO SILVER.NEWS_ENRICHED
                (news_id, ticker, headline, summary, article_url, published_at,
                 source_provider, sentiment_score, sentiment_label, subjectivity_score,
                 key_entities, source)
            SELECT
                '{kwargs["news_id"]}',
                '{kwargs["ticker"]}',
                '{headline}',
                '{summary}',
                '{kwargs["article_url"]}',
                TRY_TO_TIMESTAMP({pub_at_str}),
                '{provider}',
                {kwargs["sentiment_score"]},
                '{kwargs["sentiment_label"]}',
                {kwargs["subjectivity"]},
                PARSE_JSON('{entities_json}'),
                'nlp_processor'
        """
        execute_query_no_fetch(self.conn, query)


if __name__ == "__main__":
    processor = NewsSentimentProcessor()
    try:
        count = processor.process_and_load()
        logger.info(f"Total articles processed: {count}")
    finally:
        processor.close()
