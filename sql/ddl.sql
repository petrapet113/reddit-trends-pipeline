-- ─────────────────────────────────────────────────────────────────────
-- DuckDB warehouse for the Reddit Trends Pipeline.
--
-- Three layers, mirroring a Medallion-style architecture:
--   bronze:  immutable raw posts as fetched from Kafka
--   silver:  deduplicated, type-cleaned, sentiment-tagged
--   gold:    aggregated analytics for the dashboard
-- ─────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bronze_reddit_posts (
    post_id       VARCHAR     NOT NULL,
    subreddit     VARCHAR     NOT NULL,
    title         VARCHAR     NOT NULL,
    author        VARCHAR,
    score         INTEGER,
    num_comments  INTEGER,
    upvote_ratio  DOUBLE,
    permalink     VARCHAR,
    url           VARCHAR,
    over_18       BOOLEAN,
    is_video      BOOLEAN,
    domain        VARCHAR,
    flair         VARCHAR,
    created_utc   TIMESTAMP,
    fetched_at    TIMESTAMP   NOT NULL,
    PRIMARY KEY (post_id, fetched_at)
);

-- Silver layer: latest snapshot per post + LLM-tagged sentiment & topic.
CREATE TABLE IF NOT EXISTS silver_reddit_posts (
    post_id       VARCHAR     PRIMARY KEY,
    subreddit     VARCHAR     NOT NULL,
    title         VARCHAR     NOT NULL,
    author        VARCHAR,
    score         INTEGER,
    num_comments  INTEGER,
    upvote_ratio  DOUBLE,
    domain        VARCHAR,
    sentiment     VARCHAR,        -- 'positive' | 'neutral' | 'negative' (LLM output)
    topic_tag     VARCHAR,        -- e.g. 'tooling', 'career', 'showcase' (LLM output)
    created_utc   TIMESTAMP,
    last_seen_at  TIMESTAMP       NOT NULL
);

-- Gold layer: hourly subreddit roll-ups for the Streamlit dashboard.
CREATE TABLE IF NOT EXISTS gold_subreddit_hourly (
    bucket_start   TIMESTAMP   NOT NULL,
    subreddit      VARCHAR     NOT NULL,
    post_count     INTEGER     NOT NULL,
    avg_score      DOUBLE      NOT NULL,
    median_score   DOUBLE      NOT NULL,
    avg_comments   DOUBLE      NOT NULL,
    pct_positive   DOUBLE,
    PRIMARY KEY (bucket_start, subreddit)
);
