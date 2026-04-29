"""
Kafka producer for Reddit posts.

Publishes RedditPost records (one message per post) to the configured topic.
The producer uses post_id as the message key so the same post lands on the
same partition across re-fetches — useful when consumers compute deltas
(score changes, comment growth) over time.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Iterable

from confluent_kafka import Producer

from reddit_scraper import RedditPost, to_dict

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("REDDIT_TOPIC", "reddit_posts")


def _delivery_callback(err, msg):
    if err is not None:
        logger.error("delivery failed: %s", err)
    else:
        logger.debug("delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "reddit-trends-producer",
        "linger.ms": 50,         # micro-batch for throughput without big delay
        "compression.type": "snappy",
        "acks": "all",
        "enable.idempotence": True,
    })


def publish_posts(producer: Producer, posts: Iterable[RedditPost], topic: str = KAFKA_TOPIC) -> int:
    """Publish posts to Kafka. Returns count of messages flushed."""
    count = 0
    for post in posts:
        producer.produce(
            topic=topic,
            key=post.post_id.encode("utf-8"),
            value=json.dumps(to_dict(post)).encode("utf-8"),
            on_delivery=_delivery_callback,
        )
        count += 1
        # poll lightly to handle delivery callbacks
        producer.poll(0)

    producer.flush(timeout=10)
    logger.info("published %d posts to topic '%s'", count, topic)
    return count
