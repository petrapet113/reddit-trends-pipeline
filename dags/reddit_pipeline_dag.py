"""
Airflow DAG: Reddit Trends Pipeline.

Runs every 15 minutes:
  1. scrape  — fetch top posts from a configured list of subreddits
  2. publish — push each post to the `reddit_posts` Kafka topic
  3. transform — Bronze -> Silver -> Gold via DuckDB SQL
  4. tag     — LLM tags fresh Silver rows with sentiment + topic
  5. refresh_gold — re-aggregate hourly Gold roll-ups

The Kafka consumer (scrapers/kafka_consumer.py) runs as a separate
long-lived process; this DAG only orchestrates the producer side and
the SQL transformations.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# Make scrapers/ importable from inside Airflow workers.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scrapers"))

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "petra",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SUBREDDITS = os.environ.get(
    "REDDIT_SUBREDDITS",
    "dataengineering,MachineLearning,Python,programming,datascience",
).split(",")

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/warehouse.duckdb")
SQL_DIR = PROJECT_ROOT / "sql"


@dag(
    dag_id="reddit_trends_pipeline",
    description="Scrape Reddit, stream to Kafka, land in DuckDB, tag with LLM, refresh dashboard.",
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["reddit", "kafka", "duckdb", "llm"],
)
def reddit_trends():

    @task
    def scrape() -> list[dict]:
        from reddit_scraper import fetch_many, to_dict
        posts = fetch_many(SUBREDDITS, limit=25, time_window="day")
        return [to_dict(p) for p in posts]

    @task
    def publish(posts: list[dict]) -> int:
        from reddit_scraper import RedditPost
        from kafka_producer import build_producer, publish_posts

        producer = build_producer()
        rebuilt = [RedditPost(**p) for p in posts]
        return publish_posts(producer, rebuilt)

    @task
    def transform_sql() -> None:
        import duckdb
        sql = (SQL_DIR / "transformations.sql").read_text()
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute(sql)
        conn.close()

    @task
    def tag_with_llm() -> int:
        import duckdb
        from llm_tagger import update_silver_tags

        conn = duckdb.connect(DUCKDB_PATH)
        rows = update_silver_tags(conn, limit=200)
        conn.close()
        return rows

    @task
    def refresh_gold() -> None:
        # Gold refresh is part of transformations.sql, but we run it again
        # AFTER tagging so the pct_positive metric reflects the latest tags.
        import duckdb
        sql = (SQL_DIR / "transformations.sql").read_text()
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute(sql)
        conn.close()

    posts = scrape()
    published_count = publish(posts)
    sql_done = transform_sql()
    tagged = tag_with_llm()
    refreshed = refresh_gold()

    # explicit ordering (TaskFlow infers data deps automatically, but we
    # also want tag_with_llm to wait for the bronze->silver merge)
    published_count >> sql_done >> tagged >> refreshed


reddit_trends()
