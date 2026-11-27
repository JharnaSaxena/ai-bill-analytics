# Multi-Agent & Goal Hierarchy — AWS Cloud Architecture (`us-east-1`)

## 1. System Expectations

| Area | Expected Behavior | AWS Implementation |
|---|---|---|
| **Ingestion & Fan-Out** | Emit 15–120 events per tick (with 1-in-4 spike bursts); write each event to S3 and Kafka with the exact same `event_id` | `producer_and_archiver.py` on EC2 (`t3.medium`) |
| **Data Quality & DLQ** | Filter out null IDs/teams/models and negative token counts (`tokens_in < 0` or `tokens_out < 0`), routing invalid records to `dead_letter/` | `glue_batch_etl.py` (AWS Glue 4.0) & `run_lambda_speed.py` |
| **Stream Processing** | 5-second micro-batch polling loop (`demo_runner.py`); 1-minute tumbling windows (`minute_start`) and running team spend totals | `run_lambda_speed.py` + Apache Kafka 3.8.0 (KRaft) |
| **Batch Processing** | Serverless PySpark ETL deduplicating raw JSONL logs, joining pricing rates, writing hourly Parquet rollups (`slot_start`), and cataloging in `ai_bill_db` | AWS Glue 4.0 (`2× G.1X`), Glue Crawler, Amazon Athena, `sync_batch_to_pg.py` |
| **No Double Counting** | Batch serves `slot_start <= cutoff_ts`; Speed serves `minute_start > cutoff_ts` with zero overlapping records | `serving.merged_view` (PostgreSQL 15) |
| **Budget Alerts** | Team caps: `team-a` $500, `team-b` $300, `team-c` $200; `warning` at ≥ 80%, `critical` at ≥ 100% | `serving.budget_status` & `serving.speed_cumulative` |
| **Live Dashboard** | 4 live panels (Batch vs. Stream, MTD Budget Alerts, Cost/Min Timeseries, Spend by Model); auto-refresh every 5 seconds | Grafana (`http://<EC2_PUBLIC_IP>:3000`) |

---

## 2. Agents and Execution Patterns

| Agent | AWS Component | Main Responsibility | Execution Pattern |
|---|---|---|---|
| **A1 — Ingestion & Archiver** | `producer_and_archiver.py` (EC2 `t3.medium`) | Generate LLM usage events (including spike bursts), archive `.jsonl` files to S3 `raw/`, and publish to Kafka | Continuous (5s loop in `demo_runner.py`) |
| **A2 — Stream Broker** | Apache Kafka 3.8.0 KRaft (Docker on EC2) | Buffer live events on topic `llm-usage-events` and decouple ingestion from downstream database writes | Continuous |
| **A3 — Speed Layer** | `run_lambda_speed.py` (EC2 `t3.medium`) | Deduplicate in-flight `event_id`s, compute token costs, bucket into 1-minute windows, and update cumulative team alerts | Continuous + recursive state updates |
| **A4 — Batch ETL & Catalog** | AWS Glue 4.0 + Glue Crawler + Amazon Athena + `sync_batch_to_pg.py` | Deduplicate full S3 `raw/` history, isolate bad rows in `dead_letter/`, write Snappy Parquet, query via Athena (`ai_bill_db`), and sync `cutoff_ts` | Periodic / one-shot |
| **A5 — Unified Serving** | PostgreSQL 15 (Docker on EC2) | Store `batch_agg` and `speed_agg`, enforce composite primary keys, and dynamically merge layers at `cutoff_ts` | Reactive |
| **A6 — Monitoring** | Grafana (`:3000` on EC2) | Query `serving.merged_view` and `serving.budget_status` every 5 seconds to visualize live spend and alerts | Periodic polling |

---

## 3. Functional vs. Interaction Objectives

**Functional Objectives — What Each Agent Does Internally**
- **A1:** Synthesize schema-valid JSON usage events (`event_id`, `team_id`, `model`, `tokens_in`, `tokens_out`, `event_ts`) and partition raw logs by `dt=YYYY-MM-DD/hour=HH`.
- **A3:** Validate non-negative tokens, join `price_budget.csv`, compute 1-minute aggregates (`speed_agg`), and maintain running team spend and alert flags (`speed_cumulative`).
- **A4:** Run distributed PySpark `dropDuplicates(["event_id"])`, split invalid records into `s3://<BUCKET>/dead_letter/`, write `batch_agg_parquet`, verify totals in Amazon Athena, and advance `batch_meta.cutoff_ts`.
- **A5:** Maintain ACID consistency, primary-key idempotency, and the non-overlapping `cutoff_ts` boundary between batch and speed views.

**Interaction Objectives — How Agents Coordinate**
- **A1 → S3 `raw/` & A2 (Kafka):** Fan out every event to both S3 and Kafka with identical `event_id` and `event_ts`.
- **A2 → A3:** Deliver ordered Kafka messages from `llm-usage-events` with manual offset commits after PostgreSQL upserts succeed.
- **A3 ↔ A4 via A5:** Isolate writes into separate tables (`speed_agg` vs. `batch_agg`), coordinating handoff solely through `serving.batch_meta.cutoff_ts`.
- **A5 → A6:** Serve unified, pre-reconciled SQL views (`merged_view`, `budget_status`) to Grafana on port `3000`.

---

## 4. Sender–Receiver–Message (SRM)

| Sender | Receiver | AWS Channel / Protocol | Message Payload |
|---|---|---|---|
| **A1** (`producer_and_archiver.py`) | **S3 `raw/`** | AWS SDK (`boto3` HTTPS) | Time-partitioned `.jsonl` files (`dt=.../hour=.../*.jsonl`) |
| **A1** (`producer_and_archiver.py`) | **A2** (Kafka `:9092`) | TCP Kafka Producer (`llm-usage-events`) | Identical JSON event payload with shared `event_id` |
| **A2** (Kafka `:9092`) | **A3** (`run_lambda_speed.py`) | TCP Kafka Consumer (`aibill-speed-consumer`) | Uncommitted JSON usage events from `llm-usage-events` |
| **A3** (`run_lambda_speed.py`) | **A5** (PostgreSQL `:5432`) | PostgreSQL Wire Protocol (`psycopg2`) | Upserts to `serving.speed_agg` and `serving.speed_cumulative` |
| **S3 `raw/` & `price/`** | **A4** (AWS Glue 4.0) | S3 Connector (PySpark `2× G.1X`) | Raw JSONL event logs + `price_budget.csv` rate table |
| **A4** (AWS Glue 4.0) | **S3 `curated/` & `dead_letter/`** | S3 Parquet Writer (Snappy) | `events_parquet/`, `batch_agg_parquet/`, and rejected DLQ rows |
| **A4** (Crawler + Athena + Sync) | **A5** (PostgreSQL `:5432`) | Glue Catalog `ai_bill_db` + `psycopg2` | Verified batch aggregates (`serving.batch_agg`) and `cutoff_ts` (`serving.batch_meta`) |
| **A5** (PostgreSQL `:5432`) | **A6** (Grafana `:3000`) | Internal Docker Network (`postgres:5432`) | Query results from `serving.merged_view` and `serving.budget_status` |

---

## 5. Goal Hierarchy

```mermaid
graph TD
    G0["G0: Accurate real-time and historical LLM cost analytics on AWS"]
    G1["G1: Dual-path cloud event ingestion"]
    G2["G2: Serverless batch ETL and SQL audit"]
    G3["G3: Low-latency Kafka stream processing"]
    G4["G4: Unified serving and live budget alerting"]

    G0 --> G1
    G0 --> G2
    G0 --> G3
    G0 --> G4

    G1 --> G11["G1.1: Archive partitioned JSONL to S3 raw/"]
    G1 --> G12["G1.2: Publish events to Kafka llm-usage-events"]

    G2 --> G21["G2.1: Deduplicate and route invalid rows to S3 dead_letter/"]
    G2 --> G22["G2.2: Write Parquet, query in Athena, and sync cutoff_ts"]

    G3 --> G31["G3.1: Aggregate 1-minute windows in speed_agg"]
    G3 --> G32["G3.2: Track cumulative team spend and alerts"]

    G4 --> G41["G4.1: Merge batch (<= cutoff_ts) and speed (> cutoff_ts)"]
    G4 --> G42["G4.2: Display live 5s Grafana dashboard on EC2 :3000"]
