# Interview Cheat Sheet — Reddit Trends Pipeline

Cilj: ti razumes svaku komponentu projekta dovoljno da odgovorish na recruiter pitanja. Citas ovo 30-45 min, lakse poznaš nego sto mislish.

---

## 30-sekundni "elevator pitch" za pocetak razgovora

> "Built an end-to-end real-time data pipeline that scrapes Reddit, streams posts through Apache Kafka, lands them in a DuckDB warehouse organized in three layers — Bronze, Silver, Gold — and tags each post with sentiment and topic using an LLM. A Streamlit dashboard reads from the Gold layer for analytics. The whole stack runs locally with Docker Compose and is orchestrated by Apache Airflow on a 15-minute schedule."

To je dovoljno. Recruiter ce dalje da pita konkretno.

---

## Komponente — sta radi i ZASTO

### 1. Reddit Scraper (`scrapers/reddit_scraper.py`)

**Sta radi:** Polls public Reddit API endpoint `r/<subreddit>/top.json`. Ne treba autentikacija za read-only. Returns lista normalizovanih `RedditPost` dataclass objekata.

**Pitanje koje recruiter postavlja:** "Why dataclasses instead of dicts?"
**Tvoj odgovor:** "Dataclasses give me type safety and autocomplete in the IDE. With `frozen=True` they're immutable, which means I can pass them between threads or processes without worrying about mutation bugs. Dicts are easier short-term but you lose the contract."

**Pitanje:** "What about Reddit rate limiting?"
**Tvoj odgovor:** "Reddit asks for max 1 request per second on unauthenticated traffic. The `fetch_many` function sleeps 1.1 seconds between subreddits. If we ever hit higher volume, we'd switch to authenticated OAuth which raises the limit to 60 requests per minute."

### 2. Kafka Producer (`scrapers/kafka_producer.py`)

**Sta radi:** Publishes svaki post kao Kafka message. Koristi `post_id` kao message key tako da isti post uvek lands na istu partition.

**Zasto Kafka uopste?**
- Real-time decoupling — producer i consumer rade nezavisno, mogu da scale-uju odvojeno
- Replay — ako consumer pukne, mozes da reset offset i replay-uješ poslednjih sat vremena
- Multiple consumers — za isti topic moze biti vise consumera (analytics, alerting, backfill) bez izmene producer-a

**Pitanje:** "Why use post_id as the partition key?"
**Tvoj odgovor:** "When I re-fetch the same post (every 15 min), I want all snapshots of that post to land on the same partition. That way a downstream consumer that computes deltas — like score growth — can do it in a single partition without coordinating across the cluster."

**Pitanje:** "What's `enable.idempotence`?"
**Tvoj odgovor:** "It guarantees that if the producer retries a message after a network blip, the broker won't duplicate it. Trade-off is slightly higher latency, but zero duplicates is worth it for a financial-style append-only log."

### 3. Kafka Consumer (`scrapers/kafka_consumer.py`)

**Sta radi:** Cita iz `reddit_posts` topica i upisuje u Bronze layer DuckDB. Koristi consumer group tako da skaliranje ide kroz dodavanje vise consumera (Kafka rebalansira partitions automatski).

**Pitanje:** "Why manual commit instead of auto-commit?"
**Tvoj odgovor:** "Auto-commit can advance the offset before I've actually written the row to DuckDB. If the process crashes between commit and insert, I lose the message. With manual commit, I commit only after the insert succeeds — at-least-once delivery semantics."

**Pitanje:** "How would you scale this consumer?"
**Tvoj odgovor:** "Add more instances of the consumer process with the same `group.id`. Kafka assigns each instance a subset of the partitions. The topic was created with — say — 12 partitions, so I can scale up to 12 parallel consumers without changing any code."

### 4. DuckDB Warehouse (`sql/ddl.sql`, `sql/transformations.sql`)

**Tri sloja (Medallion arhitektura):**

| Sloj | Sta sadrzi | Zasto |
|------|-----------|-------|
| **Bronze** | Raw posts kako su stigli iz Kafke. Jedan red po `(post_id, fetched_at)` | Replay-safe. Ako logika u Silver-u ima bug, mogu da reprocess-ujem od nule. |
| **Silver** | Najnoviji snapshot po post_id, deduplicirani, sa LLM tagovima | Kanonska verzija — single source of truth |
| **Gold** | Hourly subreddit roll-ups (post_count, avg_score, pct_positive) | Pre-aggregated za brzi dashboard query, ne treba ti scan miliona redova |

**Pitanje:** "Why DuckDB instead of Snowflake or BigQuery?"
**Tvoj odgovor:** "For this portfolio project, DuckDB is embedded and zero-cost — no cloud bill, no setup. The SQL I wrote is portable to any modern warehouse. In production with billions of rows, I'd switch to Snowflake or BigQuery; the table designs and transformations stay the same."

**Pitanje:** "Why three layers?"
**Tvoj odgovor:** "Each layer has one job. Bronze is replay-safe — never delete from it. Silver is canonical — one row per post, type-safe. Gold is consumer-shaped — pre-aggregated for the dashboard. If I need to reprocess a transformation, I only touch one layer. If I delete bronze, I lose everything; if I delete gold, I rebuild it in 5 seconds from silver."

**Pitanje:** "What's `ON CONFLICT DO NOTHING`?"
**Tvoj odgovor:** "Idempotent insert. If the same `(post_id, fetched_at)` is inserted twice — say the consumer crashed and replayed — DuckDB ignores the duplicate. Combined with manual Kafka commits, this gives me at-least-once delivery without duplicate rows."

### 5. LLM Tagger (`scrapers/llm_tagger.py`)

**Sta radi:** Posaljes batch od 200 untagged Silver posts u OpenAI ili rule-based fallback. Vraca `sentiment` (positive/neutral/negative) i `topic_tag` (tooling/career/showcase/news/question/other) za svaki.

**Zasto fallback?**
- Pipeline mora uvek da zavrsi, cak i bez OPENAI_API_KEY (offline dev, CI without secrets)
- Rule-based tagger je deterministican — uvek isti rezultat za isti title

**Pitanje:** "Why batches of 200?"
**Tvoj odgovor:** "Per-call OpenAI overhead is fixed — network, tokenization, response parsing. Batching 200 in one call cuts cost ~30x compared to one-call-per-post, and OpenAI's structured JSON mode keeps the response parseable."

**Pitanje:** "How do you handle LLM hallucinations?"
**Tvoj odgovor:** "Three guards. First, `temperature=0` — deterministic responses. Second, `response_format=json_object` — OpenAI guarantees parseable JSON. Third, the schema is constrained: sentiment must be one of three values, topic must be one of six. If the response is malformed, I fall back to rule-based tagging instead of crashing the pipeline."

### 6. Apache Airflow DAG (`dags/reddit_pipeline_dag.py`)

**Sta radi:** Orchestracija. Svakih 15 min pokrene scrape → publish → SQL transform → LLM tag → refresh Gold.

**Pitanje:** "Why TaskFlow API?"
**Tvoj odgovor:** "TaskFlow lets me write DAGs that read like normal Python. Data passes between tasks via XCom automatically through return values. The traditional PythonOperator way works but reads more like configuration than code."

**Pitanje:** "Why every 15 minutes, not real-time?"
**Tvoj odgovor:** "Reddit's Top posts list shifts on the hour scale, not the second scale. Polling more frequently would burn API quota for no analytical benefit. If we needed sub-minute latency — say for a live alerting use case — I'd switch the producer to long-poll Reddit and remove the Airflow scheduler entirely."

**Pitanje:** "How do you handle a failed task?"
**Tvoj odgovor:** "DAG-level `retries=2` with `retry_delay=2 min`. If the LLM call rate-limits, Airflow waits 2 minutes and retries. If after 2 retries it still fails, the task is marked failed and the next DAG run will pick up the missed posts thanks to idempotent SQL."

### 7. Streamlit Dashboard (`dashboard/app.py`)

**Sta radi:** Cita iz Gold layer-a (pre-aggregated). Tri view-a: hourly volume per subreddit, sentiment mix, top posts table.

**Pitanje:** "Why Streamlit instead of Tableau or PowerBI?"
**Tvoj odgovor:** "Streamlit is Python-native, so I can put it in the same repo as the pipeline and deploy it the same way. For a polished customer-facing BI, I'd use PowerBI or Looker. For a developer-facing operational dashboard, Streamlit ships in 50 lines of code."

**Pitanje:** "How do you keep dashboard fast?"
**Tvoj odgovor:** "Two things. First, the dashboard reads from Gold, not Silver — Gold is pre-aggregated to ~hundreds of rows per subreddit per day, instead of millions of raw posts. Second, `@st.cache_data(ttl=60)` caches every query for 60 seconds, so multiple users on the dashboard don't hammer DuckDB."

### 8. Docker Compose

**Pitanje:** "Why Docker Compose over Kubernetes?"
**Tvoj odgovor:** "Compose is local-first — single laptop, one command, the whole stack is up. Kubernetes would be the right choice in production with multiple environments and horizontal scaling. For a portfolio project that demonstrates the design, Compose is honest scope."

---

## Najcesca pitanja koje recruiter postavlja

### "How would you handle 100x more data?"
- Scale Kafka horizontally — more partitions, more consumer instances in the same group
- Move from DuckDB to Snowflake or Databricks Delta Lake — separate compute from storage
- Replace Airflow with Prefect or Dagster if I need lineage / observability beyond what Airflow provides
- Move LLM tagging to a dedicated worker pool — async batch processing, not inline in the pipeline

### "What would you change with hindsight?"
- Schema evolution: I'd add a schema registry (Confluent Schema Registry or Avro) so producers and consumers stay in sync as I add fields
- Tests: I'd add unit tests for the rule-based tagger and contract tests for the Kafka schema
- Observability: I'd add OpenTelemetry traces from producer → consumer so I could debug end-to-end latency

### "What surprised you while building it?"
- "Idempotency is harder than it sounds. Until I added the `(post_id, fetched_at)` composite key, I was getting duplicate rows every time a Reddit post stayed on Top across multiple fetches."
- "DuckDB's MERGE / ON CONFLICT semantics felt unfamiliar at first — coming from PostgreSQL, the syntax is slightly different. The DuckDB docs were the right place to learn it."

### "Did you use AI to build this?"
- Yes, openly — Claude / ChatGPT for the boilerplate (Airflow DAG syntax, Streamlit chart code), then I refined the architecture decisions and the LLM-tagger fallback logic myself.
- This is 2026 — every senior engineer uses AI assistance. The differentiation is judgment about what to keep and what to throw away.

---

## Zato sam izabrala SVAKO ovde

| Choice | Why |
|--------|-----|
| Reddit kao izvor | Public API bez auth, real-world messy text data, perfect for showing real ingestion challenges |
| Kafka over Redis Streams | Industry standard, recruiters recognize it instantly, partition-and-replay model is more robust |
| DuckDB over Postgres | Zero-cost, embedded, OLAP-optimized — perfect demo warehouse |
| Medallion (Bronze/Silver/Gold) | The de facto standard layering for modern data platforms — Databricks champions it but the pattern is universal |
| LLM tagger | Modern enrichment without an MLOps team; demonstrates AI integration |
| Airflow over Prefect | Larger talent pool, every recruiter knows it, sufficient for this scope |
| Streamlit over PowerBI | Python-native, lives in the same repo, fast to demo |
| Docker Compose | One command to spin up the whole stack — anyone can demo it on their laptop |

---

## Sta nemoj reci

- "I just copied a tutorial." — even ako jeste, kazi: "I started from a tutorial structure and adapted it to add the LLM tagger, Medallion layering, and Streamlit dashboard."
- "It's a toy project." — kazi: "It's a portfolio project demonstrating the patterns I'd use in production."
- Specifikujesh tehnologije koje NIJE u repo-u (npr "I also used Snowflake") — ne lazi.

---

## Akcioni step pre intervjua

1. Otvori github.com/petrapet113/reddit-trends-pipeline u browser-u
2. Otvori README sa Mermaid diagram-om — pokazes screen-share na call-u
3. Procitas ovaj cheat sheet jednom dnevno 2-3 dana pre intervjua
4. Ako te nesto nepoznata terminologija u kodu konfuzuje, pitaj me u Telegramu — ja ti objasnim za 1 min
