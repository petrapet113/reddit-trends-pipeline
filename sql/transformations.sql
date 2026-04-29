-- ─────────────────────────────────────────────────────────────────────
-- Bronze -> Silver -> Gold transformations.
-- Run as a single Airflow task (or `duckdb warehouse.duckdb < transformations.sql`).
-- ─────────────────────────────────────────────────────────────────────

-- ── Bronze -> Silver ─────────────────────────────────────────────────
-- Pick the most recent snapshot per post_id and merge it into Silver.
-- Sentiment + topic are populated by the LLM helper before this runs;
-- if missing, we keep the previous Silver value to avoid clobbering.
INSERT INTO silver_reddit_posts AS s (
    post_id, subreddit, title, author, score, num_comments, upvote_ratio,
    domain, sentiment, topic_tag, created_utc, last_seen_at
)
SELECT
    b.post_id,
    b.subreddit,
    b.title,
    b.author,
    b.score,
    b.num_comments,
    b.upvote_ratio,
    b.domain,
    NULL              AS sentiment,   -- filled by Airflow LLM step
    NULL              AS topic_tag,
    b.created_utc,
    b.fetched_at      AS last_seen_at
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY post_id ORDER BY fetched_at DESC) AS rn
    FROM bronze_reddit_posts
) b
WHERE b.rn = 1
ON CONFLICT (post_id) DO UPDATE SET
    score        = excluded.score,
    num_comments = excluded.num_comments,
    upvote_ratio = excluded.upvote_ratio,
    last_seen_at = excluded.last_seen_at;


-- ── Silver -> Gold (hourly subreddit roll-up) ────────────────────────
INSERT INTO gold_subreddit_hourly (
    bucket_start, subreddit, post_count, avg_score, median_score,
    avg_comments, pct_positive
)
SELECT
    date_trunc('hour', last_seen_at)               AS bucket_start,
    subreddit,
    COUNT(*)                                       AS post_count,
    AVG(score)                                     AS avg_score,
    median(score)                                  AS median_score,
    AVG(num_comments)                              AS avg_comments,
    AVG(CASE WHEN sentiment = 'positive' THEN 1.0 ELSE 0.0 END)
                                                   AS pct_positive
FROM silver_reddit_posts
GROUP BY 1, 2
ON CONFLICT (bucket_start, subreddit) DO UPDATE SET
    post_count   = excluded.post_count,
    avg_score    = excluded.avg_score,
    median_score = excluded.median_score,
    avg_comments = excluded.avg_comments,
    pct_positive = excluded.pct_positive;
