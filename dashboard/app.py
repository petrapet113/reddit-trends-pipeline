"""
Streamlit dashboard — Reddit Trends.

Reads from the Gold layer of the DuckDB warehouse and renders three views:
  1. Hourly volume + score per subreddit
  2. Sentiment mix (positive / neutral / negative)
  3. Top posts table with topic tags

Run with: `streamlit run dashboard/app.py`
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "warehouse.duckdb")

st.set_page_config(page_title="Reddit Trends", page_icon="📊", layout="wide")
st.title("Reddit Trends — Real-Time Pipeline")
st.caption("Bronze → Silver (LLM-tagged) → Gold · refreshed every 15 min via Airflow.")


@st.cache_data(ttl=60)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        return conn.execute(sql, params).fetch_df()
    finally:
        conn.close()


# ── Filters ──────────────────────────────────────────────────────────────
subreddits = query("SELECT DISTINCT subreddit FROM silver_reddit_posts ORDER BY 1")
selected = st.sidebar.multiselect(
    "Subreddits",
    subreddits["subreddit"].tolist(),
    default=subreddits["subreddit"].tolist()[:5],
)
hours_back = st.sidebar.slider("Hours back", min_value=1, max_value=168, value=24)

if not selected:
    st.warning("Select at least one subreddit in the sidebar.")
    st.stop()

cutoff = datetime.utcnow() - timedelta(hours=hours_back)
sub_filter = "AND subreddit IN (" + ",".join(["?"] * len(selected)) + ")"

# ── Headline metrics ─────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

totals = query(
    f"""
    SELECT
      COUNT(*)              AS posts,
      AVG(score)            AS avg_score,
      AVG(num_comments)     AS avg_comments,
      AVG(CASE WHEN sentiment = 'positive' THEN 1.0 ELSE 0.0 END) AS pct_pos
    FROM silver_reddit_posts
    WHERE last_seen_at >= ? {sub_filter}
    """,
    (cutoff, *selected),
)

col1.metric("Posts", f"{int(totals['posts'][0])}")
col2.metric("Avg score", f"{totals['avg_score'][0]:.0f}")
col3.metric("Avg comments", f"{totals['avg_comments'][0]:.0f}")
col4.metric("% positive", f"{(totals['pct_pos'][0] or 0) * 100:.0f}%")

st.divider()

# ── Hourly volume per subreddit ─────────────────────────────────────────
st.subheader("Hourly post volume per subreddit")
hourly = query(
    f"""
    SELECT bucket_start, subreddit, post_count, avg_score
    FROM gold_subreddit_hourly
    WHERE bucket_start >= ? {sub_filter}
    ORDER BY bucket_start
    """,
    (cutoff, *selected),
)
if not hourly.empty:
    pivot = hourly.pivot_table(
        index="bucket_start", columns="subreddit", values="post_count", aggfunc="sum"
    ).fillna(0)
    st.line_chart(pivot)
else:
    st.info("No Gold data yet — trigger the Airflow DAG and refresh.")

# ── Sentiment mix ───────────────────────────────────────────────────────
st.subheader("Sentiment mix")
sentiment = query(
    f"""
    SELECT subreddit, sentiment, COUNT(*) AS n
    FROM silver_reddit_posts
    WHERE last_seen_at >= ? {sub_filter} AND sentiment IS NOT NULL
    GROUP BY 1, 2
    """,
    (cutoff, *selected),
)
if not sentiment.empty:
    pivot = sentiment.pivot_table(
        index="subreddit", columns="sentiment", values="n", aggfunc="sum"
    ).fillna(0)
    st.bar_chart(pivot)
else:
    st.info("Sentiment tags not yet populated — wait for the next Airflow run or run llm_tagger.update_silver_tags() manually.")

# ── Top posts ───────────────────────────────────────────────────────────
st.subheader("Top posts (last window)")
top_posts = query(
    f"""
    SELECT subreddit, title, author, score, num_comments, sentiment, topic_tag, last_seen_at
    FROM silver_reddit_posts
    WHERE last_seen_at >= ? {sub_filter}
    ORDER BY score DESC
    LIMIT 25
    """,
    (cutoff, *selected),
)
st.dataframe(top_posts, use_container_width=True, hide_index=True)
