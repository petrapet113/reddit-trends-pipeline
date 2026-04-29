"""
Kafka consumer that drains the reddit_posts topic into DuckDB.

Single-process consumer for simplicity. Scale-out is achieved by adding more
consumer instances to the same group_id — Kafka rebalances partitions
automatically.
"""
from __future__ import annotations

import json
import logging
import os
import signal
from typing import Optional

import duckdb
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("REDDIT_TOPIC", "reddit_posts")
KAFKA_GROUP = os.environ.get("REDDIT_CONSUMER_GROUP", "reddit-trends-warehouse-1")
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "warehouse.duckdb")

UPSERT_SQL = """
INSERT INTO bronze_reddit_posts (
    post_id, subreddit, title, author, score, num_comments, upvote_ratio,
    permalink, url, over_18, is_video, domain, flair, created_utc, fetched_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (post_id, fetched_at) DO NOTHING;
"""


class GracefulShutdown:
    """Toggles a flag on SIGINT/SIGTERM so the consumer loop exits cleanly."""

    stop = False

    def __init__(self):
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, *_):
        logger.info("shutdown signal received")
        self.stop = True


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",  # for replay; switch to "latest" in prod
        "enable.auto.commit": False,      # commit manually after a successful insert
    })


def open_warehouse(db_path: str = DUCKDB_PATH) -> duckdb.DuckDBPyConnection:
    """Initialise DuckDB and ensure the bronze table exists."""
    conn = duckdb.connect(db_path)
    conn.execute(open("sql/ddl.sql").read())
    return conn


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    warehouse = open_warehouse()
    shutdown = GracefulShutdown()

    logger.info("consuming from %s, group=%s, db=%s", KAFKA_TOPIC, KAFKA_GROUP, DUCKDB_PATH)

    try:
        while not shutdown.stop:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            payload = json.loads(msg.value().decode("utf-8"))
            warehouse.execute(UPSERT_SQL, [
                payload["post_id"], payload["subreddit"], payload["title"], payload["author"],
                payload["score"], payload["num_comments"], payload["upvote_ratio"],
                payload["permalink"], payload["url"], payload["over_18"], payload["is_video"],
                payload["domain"], payload.get("flair"), payload["created_utc"], payload["fetched_at"],
            ])
            consumer.commit(asynchronous=False)

    finally:
        consumer.close()
        warehouse.close()
        logger.info("consumer stopped cleanly")


if __name__ == "__main__":
    run()
