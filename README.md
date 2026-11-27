# AI Bill Analytics

*A Lambda Architecture for tracking LLM spend: Kafka, Spark, PostgreSQL and Grafana, all running locally in Docker.*

## 1. Problem and Business Value

**Problem.** Teams use several LLM models that are charged per token, but the bill arrives only at month-end, after a runaway job has already overspent the budget. Finance needs accurate history; engineers need to know within seconds. One system cannot be both instant and fully accurate.

**Business value.** Teams see spend per team and model within seconds and get warned at 80% and 100% of budget, while finance keeps an accurate, recomputable history.

---

## 2. Lambda Architecture Primer (in simple words)

Think of a bank. The **monthly statement** is accurate but late. The **SMS after every payment** is instant but only a quick note. The **passbook** shows both in one place. Lambda Architecture works the same way.

| Layer | In simple words | In this project | Tool |
| --- | --- | --- | --- |
| Batch | Slow, careful, always correct. Recounts everything from the saved raw data. | Reads all raw events and calculates the official totals | Spark batch job |
| Speed | Fast, rough, only looks at the newest data. | Updates totals every 10 seconds and raises budget alerts | Spark Streaming + Kafka |
| Serving | One place to ask questions. Combines both answers. | `merged_view` and `budget_status` | PostgreSQL |

**The one rule that avoids double counting:** a cutoff time splits history. Before the cutoff, batch numbers are used; after the cutoff, speed numbers are used.

**Trade-off.** The same price calculation is needed in two places, so we keep it in one shared file (`transformations.py`).

---

## 3. Logical Architecture

```
                  ┌──► Raw files (JSONL, never edited) ──► Spark BATCH ──┐
                  │                                                      │
LLM usage ──► Producer                                                   ▼
  events          │                                              PostgreSQL (serving)
                  └──► Kafka ──► Spark STREAMING ───────────────────────►│
                                                                         ▼
                                                       merged_view / budget_status
                                                                         ▼
                                                                      Grafana
```

Everything runs as Docker Compose services: Kafka (topic `llm-usage-events`, 3 partitions, 7-day retention), a Spark master and worker, PostgreSQL and Grafana.

---

## 4. Goal Hierarchy and Agent Design

### 4.1 Goal and dependency graph

**Top goal (G0):** Give teams and managers an accurate and fresh view of LLM spend, with early budget warnings.

```
G0  Accurate + fresh spend view with early warnings
 │
 ├── G1  Capture every usage event safely
 │         │
 │         ├──────────────┐
 │         ▼              ▼
 ├── G2  Accurate      G3  Fresh live numbers
 │       history           and alerts
 │         │              │
 │         └──────┬───────┘
 │                ▼
 └── G4  One merged, read-only view for people
```

**Dependencies:** G2 and G3 both need G1. G4 needs both G2 and G3. G1 has no prerequisites.

### 4.2 Functional and interaction objectives (quantified)

| Goal | Functional objective (what it must do) | Interaction objective (who it talks to) |
| --- | --- | --- |
| **G1 Capture** | Generate about 50 events per second. Save each event to a raw file first (one file per minute), then send it to Kafka. Inject test faults: 2% duplicates, 2% late (30 to 300 s), 0.5% bad events. | Tell the raw store and Kafka about every event, in that order. |
| **G2 Accurate history** | Read all raw events. Remove duplicates by `event_id`. Reject invalid events with one of 5 reasons. Price each event using the price valid on its date. Total per team, model and slot (09:00 to 13:00, 13:00 to 17:00). Publish the cutoff. | Ask the raw store for events; tell PostgreSQL the totals, the cutoff, the rejects and the dedup counts. |
| **G3 Fresh numbers** | Every 10 seconds: remove duplicates (30 s watermark), validate, price, total per 1-minute window and keep a running total per team. Alert at 80% (`warning`) and 100% (`critical`). Delete speed rows older than the cutoff. | Receive events from Kafka; ask PostgreSQL for budgets and the cutoff; tell PostgreSQL the live totals and alerts. |
| **G4 One view** | Combine batch rows before the cutoff with speed rows after it. Show budget status as `ok`, `warning` or `critical`. Dashboard refreshes every 10 seconds, read-only. | Grafana asks PostgreSQL every 10 s and shows the answer to managers and teams. |

### 4.3 What each agent does

| Agent | Serves goal | Duties (quantified) | Output |
| --- | --- | --- | --- |
| **Producer agent** (`producer.py`) | G1 | 1) Build 1 event per tick at 50 per second. 2) Write to raw file, then Kafka. 3) Repeat 2% of events as duplicates. 4) Stop cleanly on shutdown signal and flush. | Raw JSONL files, Kafka messages |
| **Batch agent** (`batch_job.py`) | G2 | 1) Read all raw events. 2) Remove duplicates. 3) Reject invalid events (5 reasons) to `dead_letter`. 4) Price with date-valid prices. 5) Total into 2 slots a day. 6) Set cutoff = start of latest slot with data. 7) Replace `batch_agg` and `batch_meta`. 8) Log one `dedup_stats` row. | 4 tables updated per run |
| **Speed agent** (`stream_job.py`) | G3 | 1) Read Kafka in 10 s micro-batches. 2) Drop duplicates and events later than 30 s. 3) Price and total per minute. 4) Keep a running cost per team. 5) Write an alert only when a team moves up a level. 6) Remove speed rows older than the cutoff. | `speed_agg`, `speed_cumulative`, `speed_alerts` |
| **Serving agent** (PostgreSQL views) | G4 | 1) Merge batch and speed without overlap. 2) Compute month-to-date cost, `pct_used` and status per team. 3) Give Grafana read-only access. | `merged_view`, `budget_status` |
| **Dashboard agent** (Grafana) | G4 | 1) Run 8 panel queries every 10 s. 2) Colour thresholds at 80% and 100%. 3) Show batch vs speed on one chart. | Live dashboard |

### 4.4 Sender, receiver, message

| # | Sender | Receiver | Message | Type |
| --- | --- | --- | --- | --- |
| 1 | Producer | Raw store | Event as one JSON line | Inform (sent first) |
| 2 | Producer | Kafka | Same event JSON on topic `llm-usage-events` | Inform (sent second) |
| 3 | Raw store | Batch agent | All events saved so far | Reply to a read request |
| 4 | Kafka | Speed agent | New events, delivered in 10 s micro-batches | Inform |
| 5 | Batch agent | PostgreSQL | Totals per slot, cutoff, rejected events, dedup counts | Inform |
| 6 | Speed agent | PostgreSQL | Minute totals, running totals, alert rows | Inform |
| 7 | PostgreSQL | Speed agent | Team budgets and the cutoff | Reply to a query |
| 8 | Grafana | PostgreSQL | SQL query, every 10 s | Query |
| 9 | PostgreSQL | Grafana | Rows from `merged_view`, `budget_status` and others | Reply |
| 10 | Grafana | Manager and teams | Charts, gauges, alert table | Inform |

---

## 5. Code Walkthrough (files, functions, constants)

### 5.1 `producers/event_generator.py`: makes fake events

**Constants**

- `STATUSES`: `success` (4 of 6), `rate_limited`, `error`
- `APPS`: `chatbot`, `rag-search`, `summarizer`, `code-assistant`

**Functions**

- `load_prices(path)`: reads the price CSV and stops if it is empty.
- `make_event(valid_rows, late_rate=0.02, bad_rate=0.005)`: builds one event for a random team and model; backdates it into 09:00 to 17:00 when run outside office hours; sometimes makes it late or invalid.

### 5.2 `producers/producer.py`: sends events

**Constants (environment variables)**

- `KAFKA_BOOTSTRAP` = `kafka:29092`, `TOPIC` = `llm-usage-events`
- `RATE_PER_SEC` = 50, `DURATION_SEC` = 0 (run forever)
- `DUPLICATE_RATE` = 0.02, `LATE_RATE` = 0.02, `BAD_RATE` = 0.005

**Class `MinuteWriter`**

- `__init__(base)`: sets the raw data folder.
- `write(ev, ts)`: appends the event to a file in `year=/month=/day=/hour=/MM.jsonl`.
- `_rotate(key)`: opens a new file when the minute changes.
- `close()`: closes the open file.

**Function**

- `main()`: loop that writes raw first, then Kafka (`acks="all"`), sometimes sends a duplicate, and stops cleanly on shutdown.

### 5.3 `jobs/transformations.py`: shared logic for batch and stream

| Function / Constant | Code Call | Simple Layman Meaning |
| :--- | :--- | :--- |
| **`USAGE_EVENT_SCHEMA` & `PRICE_SCHEMA`** | `StructType([...])` | Defines the exact column names and data types (`TimestampType`, `LongType`, `StringType`) for incoming events and the pricing CSV. |
| **Slot Constants (`09:00`–`17:00`)** | `SLOT_A_START_HOUR = 9`, `SLOT_A_END_HOUR = 13`, `SLOT_B_START_HOUR = 13`, `SLOT_B_END_HOUR = 17` | Defines the two daily 4-hour operational batch windows (`09:00–13:00` and `13:00–17:00` UTC). |
| **`load_prices(spark, path)`** | `spark.read.option("header", True).schema(PRICE_SCHEMA).csv(path)` | Reads `price_budget.csv` and casts token rates, budget caps, and effective dates into numeric/date types. |
| **`dedup(df)`** | `df.dropDuplicates(["event_id"]).filter(F.col("event_id").isNotNull())` | Drops rows with missing `event_id` and removes duplicate `event_id`s. |
| **`add_slot_start(df)`** | `F.when((h >= 9) & (h < 13), "... 09:00:00").when((h >= 13) & (h < 17), "... 13:00:00")` | Buckets each event's timestamp into either the `09:00:00` or `13:00:00` 4-hour batch slot. |
| **`filter_valid(df, prices)`** | `.filter(tokens >= 0).join(known, on="model", how="left_semi")` | Quickly filters out null timestamps, negative tokens, and unknown models (used by `stream_job.py`). |
| **`split_valid_invalid(df, prices)`** | Tags `_r1` (`missing_event_id`), `_r2` (`negative_tokens`), `_r3` (`null_ts`), `unknown_model`, `outside_office_hours` | Splits events into `(valid, invalid)` DataFrames and labels every rejected row with its exact `reject_reason` (used by `batch_job.py`). |
| **`with_cost(df, prices)`** | `(tokens_in / 1000.0) * input_price_per_1k + (tokens_out / 1000.0) * output_price_per_1k` | Joins events with `prices` on `(model, team_id)` within `[effective_from, effective_to]` and calculates USD cost. |
| **`current_slot_start_utc()`** | `now = datetime.now(timezone.utc)` → returns `"YYYY-MM-DD 09:00:00"` / `13:00:00` / `17:00:00` | Helper that returns the start timestamp of the current UTC operational slot. |

### 5.4 `jobs/batch_job.py`: the batch agent

**Constants**
- `DATA_DIR` = `/opt/data`, `PRICE_CSV`, `PG_URL`, `PG_PROPS` (database login from environment)

**Execution Stages (`main()`)**

| Step / Function | Code Call | Simple Layman Meaning |
| :--- | :--- | :--- |
| **Setup & Prices** | `SparkSession.builder` + `T.load_prices()` | Starts Spark, connects to Postgres, and loads the team token pricing & budget CSV. |
| **1. Read Raw Logs** | `spark.read.schema(...).json(...)` | Reads all raw JSONL usage files from `/opt/data/raw` (exits early if empty). |
| **2. Deduplicate** | `T.dedup(raw)` | Removes duplicate `event_id` rows so retries are never billed twice. |
| **3. Validate Split** | `T.split_valid_invalid()` | Separates clean records from bad ones (negative tokens, unknown models, off-hours). |
| **4. Dead-Letter Queue** | `dl.write...jdbc("serving.dead_letter")` | Saves rejected rows with their failure reason into `serving.dead_letter` for auditing. |
| **5. Cost & Time Slot** | `T.with_cost()` + `T.add_slot_start()` | Calculates USD spend per event and assigns each event to its 4-hour batch slot (`09:00` or `13:00`). |
| **6. Group & Sum** | `priced.groupBy(...).agg(...)` | Totals `tokens_in`, `tokens_out`, and `cost` per team, model, and 4-hour slot. |
| **7. Cutoff Watermark** | `max_slot = priced.agg(F.max("slot_start"))` | Finds the latest slot start (`cutoff_ts`) so batch and stream never overlap or double-count. |
| **8. Save Batch Tables** | `agg.write` & `watermark.write` | Writes final batch totals to `serving.batch_agg` and updates `cutoff_ts` in `serving.batch_meta`. |
| **9. Audit Stats** | `stats.write...jdbc("serving.dedup_stats")` | Logs raw vs. deduplicated row counts to prove how many duplicates were caught. |

### 5.5 `jobs/stream_job.py`: the speed agent

**Constants**
- `TRIGGER_SECONDS` = 10 seconds, `WATERMARK` = 30 seconds, `WINDOW` = 1 minute
- `CKPT_WINDOWED`, `CKPT_CUMULATIVE`: checkpoint folders so the job resumes after a restart
- `KAFKA_BOOTSTRAP`, `TOPIC`, `PRICE_CSV`, `PG_URL`, `PG_PROPS`

**Execution Pipeline & Functions**

| Step / Function | Code Call (`stream_job.py`) | Simple Layman Meaning |
| :--- | :--- | :--- |
| **1. Setup & Live Kafka Read** | `spark.readStream.format("kafka")` + `T.load_prices()` | Starts Spark Streaming, loads the team pricing CSV, and subscribes live to `llm-usage-events` (`startingOffsets="latest"`). |
| **2. Parse, Watermark & Dedup** | `F.from_json(...)` + `.withWatermark("event_ts", "30 seconds").dropDuplicates(["event_id"])` | Parses raw Kafka bytes into columns, gives late events a 30s grace window, and drops duplicate `event_id`s in flight. |
| **3. Filter & Price Events** | `T.filter_valid()` + `T.with_cost()` | Keeps only clean events (valid model, positive tokens) and calculates USD spend per event. |
| **4. Query A: 1-Min Tumbling Window** | `priced.groupBy(F.window("event_ts", "1 minute"), "team_id", "model")` | Groups live traffic into non-overlapping **1-minute tumbling buckets** (sliding = window = `1 min`), triggering every `10s`. |
| **5. `write_windowed()` & Idle Handling** | `if n == 0: return` + `ON CONFLICT DO UPDATE` + `DELETE ... < cutoff_ts` | Skips empty batches (e.g., idle weekends with 0 events), upserts 1-min buckets into `speed_agg`, and deletes minutes already finalized by Batch. |
| **6. Query B: Stateful Cumulative** | `priced.groupBy("team_id").agg(F.sum("cost")...)` (`outputMode="complete"`) | Maintains **running team totals** ($S_t = f(S_{t-1}, \Delta E_t)$) across micro-batches, checkpointed in `/opt/checkpoints/stream_cumulative`. |
| **7. `write_cumulative()` & Alerts** | Joins `team_budget`, detects `80%`/`100%` transitions, upserts `speed_cumulative` | Calculates `% of budget used`, logs `warning_80` or `critical_100` rows to `speed_alerts` only on state transitions, and updates team totals. |
## 6. Batch Layer

**What it is.** The careful, official calculation. It re-reads **all** raw events every run, so any bug fix or price change can be corrected by simply running it again.

**Why it is trusted**

- **Raw data is never edited.** Events are only appended to files, so the original is always there.
- **Recomputed from scratch.** Running it twice gives the same answer, because each run replaces its output tables.
- **Bad data is kept, not lost.** Rejected events go to `dead_letter` with a reason.

**Steps (as printed in the job log)**

1. **RAW:** read all event files.
2. **DEDUP:** keep one row per `event_id`.
3. **VALID:** reject events with `missing_event_id`, `negative_tokens`, `null_ts`, `unknown_model` or `outside_office_hours`.
4. **DEAD:** save rejects to `serving.dead_letter`.
5. **PRICE:** apply the price valid on the event date and calculate cost.
6. **AGG:** total tokens and cost per team, model and slot.
7. **CUTOFF:** the cutoff is the start of the latest slot that has data.
8. **WRITE:** replace `batch_agg` and `batch_meta`.
9. **AUDIT:** add a row to `dedup_stats` (raw, kept, dropped).

**Slots.** Each day has two 4-hour slots: 09:00 to 13:00 and 13:00 to 17:00. Batch publishes older slots; the newest slot is left to the speed layer, which fills it live.

---

## 7. Serving Layer (local PostgreSQL)

PostgreSQL runs `db/init/01-serving.sql` and then `02-stateful.sql` automatically the first time the container starts. Everything lives in the schema `serving`.

### 7.1 DDL hierarchy

```
serving (schema)
├── Batch side
│   ├── batch_agg          one row per team + model + slot
│   └── batch_meta         one row per team + model, holds cutoff_ts
├── Speed side
│   ├── speed_agg          one row per team + model + minute
│   ├── speed_cumulative   one row per team, running total and alert level
│   └── speed_alerts       log of threshold crossings
├── Reference
│   └── team_budget        team-a 500, team-b 300, team-c 200
├── Audit
│   ├── dead_letter        rejected events and the reason
│   └── dedup_stats        raw vs kept vs dropped, per batch run
├── Views
│   ├── merged_view        batch_agg + batch_meta + speed_agg
│   │   └── budget_status  merged_view + team_budget
│   └── speed_budget_status   speed_cumulative only
└── Access
    └── grafana_ro         read-only login for Grafana
```

Order matters: tables first, then views that read them, then permissions.

### 7.2 Tables

**Batch side**

```sql
CREATE TABLE serving.batch_agg (
  team_id text NOT NULL, model text NOT NULL, slot_start timestamptz NOT NULL,
  tokens_in bigint DEFAULT 0, tokens_out bigint DEFAULT 0, cost numeric(14,6) DEFAULT 0,
  PRIMARY KEY (team_id, model, slot_start));

CREATE TABLE serving.batch_meta (
  team_id text NOT NULL, model text NOT NULL,
  max_event_ts timestamptz,
  cutoff_ts timestamptz NOT NULL DEFAULT 'epoch',
  status text NOT NULL DEFAULT 'incomplete',
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (team_id, model));
```

- `batch_agg`: the official totals. The primary key stops duplicate rows.
- `batch_meta`: the **cutoff**, the line between batch time and speed time.

**Speed side**

```sql
CREATE TABLE serving.speed_agg (
  team_id text NOT NULL, model text NOT NULL, minute_start timestamptz NOT NULL,
  tokens_in bigint DEFAULT 0, tokens_out bigint DEFAULT 0, cost numeric(14,6) DEFAULT 0,
  PRIMARY KEY (team_id, model, minute_start));

CREATE TABLE serving.speed_cumulative (
  team_id text PRIMARY KEY,
  total_tokens_in bigint, total_tokens_out bigint, total_cost numeric(14,6),
  budget_limit numeric(14,2) NOT NULL, pct_used numeric(10,2),
  alert_status text DEFAULT 'ok', last_event_ts timestamptz, last_updated timestamptz);

CREATE TABLE serving.speed_alerts (
  alert_ts timestamptz DEFAULT now(), team_id text NOT NULL,
  threshold text NOT NULL, pct_used numeric(10,2), total_cost numeric(14,6));
```

- `speed_agg`: live per-minute totals. The primary key allows safe update-or-insert.
- `speed_cumulative`: running cost per team and its alert level.
- `speed_alerts`: one row each time a team crosses a threshold.

**Reference and audit**

```sql
CREATE TABLE serving.team_budget (team_id text PRIMARY KEY, budget_limit numeric(14,2) NOT NULL);
CREATE TABLE serving.dead_letter (received_at timestamptz DEFAULT now(), layer text, reason text, event text);
CREATE TABLE serving.dedup_stats (run_at timestamptz DEFAULT now(), layer text,
  raw_count bigint, after_dedup_count bigint, dropped_count bigint);
```

### 7.3 Views

```sql
CREATE VIEW serving.merged_view AS
  SELECT b.team_id, b.model, b.slot_start AS window_start, b.tokens_in, b.tokens_out, b.cost,
         'batch' AS layer
  FROM serving.batch_agg b JOIN serving.batch_meta m USING (team_id, model)
  WHERE b.slot_start < m.cutoff_ts            -- before the cutoff: batch
UNION ALL
  SELECT s.team_id, s.model, s.minute_start, s.tokens_in, s.tokens_out, s.cost, 'speed'
  FROM serving.speed_agg s LEFT JOIN serving.batch_meta m USING (team_id, model)
  WHERE s.minute_start >= COALESCE(m.cutoff_ts, 'epoch');   -- at or after: speed
```

- **`merged_view`** is the heart of the design. Batch uses `<` and speed uses `>=`, so each moment belongs to exactly one layer. The `layer` column shows which one. If no batch has run yet, all speed rows show.

```sql
CREATE VIEW serving.budget_status AS
  SELECT team_id, budget_limit, SUM(cost) AS mtd_cost, pct_used,
         CASE WHEN cost >= budget THEN 'critical'
              WHEN cost >= 0.8 * budget THEN 'warning' ELSE 'ok' END AS status
  FROM serving.team_budget LEFT JOIN serving.merged_view ... -- current month only
```

- **`budget_status`** adds up this month’s cost from `merged_view` per team: `ok` below 80%, `warning` from 80%, `critical` from 100%. (Shortened here; the real view also rounds and groups.)
- **`speed_budget_status`** shows only the live stream’s running total.

### 7.4 Access

```sql
CREATE ROLE grafana_ro LOGIN PASSWORD '...';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA serving TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO grafana_ro;
```

Grafana can read everything and change nothing.

---

## 8. Visualization: Grafana

**Why Grafana.** It is **free, open-source and lightweight**: one small Docker container, no extra database, and a built-in PostgreSQL connector. That suits a live cost monitor on a laptop.

**How it is set up**

- **Configuration as code:** the connection and dashboard live in files (`grafana/provisioning/`), so the setup is repeatable.
- **Datasource:** PostgreSQL at `postgres:5432` using the read-only user `grafana_ro`.
- **Dashboard “AI Bill Analytics - Lambda”:** 8 panels, refresh every 10 seconds, default range last 24 hours.

| Panel | Shows |
| --- | --- |
| Cost by team | Spend over time per team |
| Tokens by model | Which model uses the most tokens |
| Dead-letter events | How many events were rejected |
| Budget utilization | % of budget used, yellow at 80, red at 100 |
| Lambda boundary: batch vs speed | Batch and speed cost stacked, showing the handover |
| Dedup audit | Raw, kept and dropped counts per batch run |
| Cumulative stream cost | Total the stream has seen |
| Alert transitions | When a team first crossed 80% or 100% |
