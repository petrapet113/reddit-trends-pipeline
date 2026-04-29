"""
LLM-augmented tagging step.

Given a batch of Silver-layer posts, this module asks an LLM to classify each
post into a sentiment bucket (positive / neutral / negative) and a topic tag
(tooling / career / showcase / news / question / other).

Provider is pluggable. Defaults to OpenAI Chat Completions; falls back to a
deterministic rule-based tagger when no API key is configured. The fallback
keeps the pipeline runnable in offline / demo environments and avoids spend
during local development.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Iterable

import duckdb

logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
OPENAI_MODEL = os.environ.get("LLM_MODEL", "gpt-4o-mini")

SYSTEM_PROMPT = """\
You are a JSON-only classifier for Reddit posts in technical communities
(data engineering, machine learning, programming).

For each post, return:
- sentiment: one of "positive", "neutral", "negative"
- topic:     one of "tooling", "career", "showcase", "news", "question", "other"

Output a JSON object: {"results": [{"post_id": "...", "sentiment": "...", "topic": "..."}]}.
"""


@dataclass(frozen=True)
class TagResult:
    post_id: str
    sentiment: str
    topic: str


# ── LLM path ────────────────────────────────────────────────────────────
def _tag_via_openai(batch: list[dict]) -> list[TagResult]:
    from openai import OpenAI  # lazy import — only required when using the LLM path

    client = OpenAI(api_key=OPENAI_API_KEY)
    user_payload = json.dumps({"posts": batch})

    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_payload},
        ],
    )
    raw = response.choices[0].message.content or "{}"
    parsed = json.loads(raw).get("results", [])
    return [TagResult(p["post_id"], p["sentiment"], p["topic"]) for p in parsed]


# ── Rule-based fallback (deterministic, no network) ────────────────────
NEGATIVE_HINTS = {"fail", "broken", "stuck", "issue", "bug", "error", "wtf", "horrible"}
POSITIVE_HINTS = {"love", "great", "amazing", "excited", "shipped", "launch", "thanks"}
TOPIC_RULES = (
    ("showcase", {"showcase", "i built", "i made", "open source", "i shipped"}),
    ("career",   {"job", "interview", "hiring", "salary", "fired", "career"}),
    ("tooling",  {"airflow", "kafka", "dbt", "snowflake", "spark", "duckdb", "pandas"}),
    ("news",     {"announc", "release", "version", "launch", "ga"}),
    ("question", {"how do i", "anyone", "help", "?"}),
)


def _tag_rule_based(batch: list[dict]) -> list[TagResult]:
    out: list[TagResult] = []
    for post in batch:
        title = (post.get("title") or "").lower()

        if any(h in title for h in NEGATIVE_HINTS):
            sentiment = "negative"
        elif any(h in title for h in POSITIVE_HINTS):
            sentiment = "positive"
        else:
            sentiment = "neutral"

        topic = "other"
        for label, hints in TOPIC_RULES:
            if any(h in title for h in hints):
                topic = label
                break

        out.append(TagResult(post["post_id"], sentiment, topic))
    return out


def tag_batch(batch: list[dict]) -> list[TagResult]:
    """Tag a batch of posts. Uses the LLM if OPENAI_API_KEY is set; otherwise the rule-based fallback."""
    if not batch:
        return []
    try:
        if OPENAI_API_KEY:
            return _tag_via_openai(batch)
        logger.info("OPENAI_API_KEY not set — using rule-based fallback for %d posts", len(batch))
        return _tag_rule_based(batch)
    except Exception as exc:  # noqa: BLE001 — never let LLM hiccups stop the pipeline
        logger.warning("LLM tagging failed (%s), falling back to rules", exc)
        return _tag_rule_based(batch)


# ── DuckDB integration ─────────────────────────────────────────────────
def update_silver_tags(conn: duckdb.DuckDBPyConnection, limit: int = 200) -> int:
    """Read untagged Silver rows, tag them, write results back. Returns rows updated."""
    untagged = conn.execute(
        "SELECT post_id, title FROM silver_reddit_posts WHERE sentiment IS NULL OR topic_tag IS NULL LIMIT ?",
        [limit],
    ).fetchall()
    batch = [{"post_id": r[0], "title": r[1]} for r in untagged]
    if not batch:
        return 0

    tagged = tag_batch(batch)
    for t in tagged:
        conn.execute(
            "UPDATE silver_reddit_posts SET sentiment = ?, topic_tag = ? WHERE post_id = ?",
            [t.sentiment, t.topic, t.post_id],
        )
    logger.info("tagged %d Silver rows", len(tagged))
    return len(tagged)
