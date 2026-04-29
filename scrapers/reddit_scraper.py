"""
Reddit public API scraper.

Reads top posts from a list of subreddits via the public r/<subreddit>/top.json
endpoint (no auth required for read-only access). Returns normalized records
ready for downstream Kafka publishing.

The Reddit JSON API is documented at https://www.reddit.com/dev/api.
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Iterable

import requests

logger = logging.getLogger(__name__)

REDDIT_HEADERS = {
    # Reddit requires a descriptive User-Agent on every request.
    "User-Agent": "reddit-trends-pipeline/0.1 (by u/petrapet113)"
}


@dataclass(frozen=True)
class RedditPost:
    """Normalized record published to Kafka and stored in DuckDB."""

    post_id: str
    subreddit: str
    title: str
    author: str
    score: int
    num_comments: int
    upvote_ratio: float
    permalink: str
    url: str
    over_18: bool
    is_video: bool
    domain: str
    flair: str | None
    created_utc: str  # ISO-8601
    fetched_at: str  # ISO-8601


def fetch_top_posts(subreddit: str, limit: int = 25, time_window: str = "day") -> list[RedditPost]:
    """
    Fetch top posts from a subreddit.

    Args:
        subreddit: Subreddit name without the 'r/' prefix (e.g. "dataengineering").
        limit:     1..100. Reddit caps at 100 per request.
        time_window: hour | day | week | month | year | all.

    Raises:
        requests.HTTPError on non-200 response.
    """
    if limit < 1 or limit > 100:
        raise ValueError("limit must be between 1 and 100")

    url = f"https://www.reddit.com/r/{subreddit}/top.json"
    params = {"limit": limit, "t": time_window}

    response = requests.get(url, headers=REDDIT_HEADERS, params=params, timeout=15)
    response.raise_for_status()
    payload = response.json()

    posts: list[RedditPost] = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for child in payload.get("data", {}).get("children", []):
        d = child.get("data", {})
        try:
            post = RedditPost(
                post_id=d["id"],
                subreddit=subreddit,
                title=d.get("title", ""),
                author=d.get("author", "[deleted]"),
                score=int(d.get("score", 0)),
                num_comments=int(d.get("num_comments", 0)),
                upvote_ratio=float(d.get("upvote_ratio", 0.0)),
                permalink=f"https://reddit.com{d.get('permalink', '')}",
                url=d.get("url", ""),
                over_18=bool(d.get("over_18", False)),
                is_video=bool(d.get("is_video", False)),
                domain=d.get("domain", ""),
                flair=d.get("link_flair_text"),
                created_utc=datetime.fromtimestamp(
                    d.get("created_utc", 0), tz=timezone.utc
                ).isoformat(),
                fetched_at=fetched_at,
            )
            posts.append(post)
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Skipped malformed post payload: %s", exc)
            continue

    logger.info("fetched %d posts from r/%s", len(posts), subreddit)
    return posts


def fetch_many(subreddits: Iterable[str], limit: int = 25, time_window: str = "day") -> list[RedditPost]:
    """Fetch from multiple subreddits, sleeping between calls to respect Reddit rate limits."""
    out: list[RedditPost] = []
    for sub in subreddits:
        try:
            out.extend(fetch_top_posts(sub, limit=limit, time_window=time_window))
        except requests.HTTPError as exc:
            logger.error("HTTP %s on r/%s: %s", exc.response.status_code, sub, exc)
        # Reddit asks for max 1 req/sec on unauthenticated traffic.
        time.sleep(1.1)
    return out


def to_dict(post: RedditPost) -> dict:
    """Serialize RedditPost to dict for Kafka / JSON output."""
    return asdict(post)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sample = fetch_many(["dataengineering", "MachineLearning", "Python"], limit=10)
    print(f"Got {len(sample)} posts. First: {sample[0].title if sample else 'none'}")
