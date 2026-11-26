# AI Bill Analytics — AWS Architecture & Data Flow Guide

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![AWS](https://img.shields.io/badge/AWS-EC2%20%7C%20S3%20%7C%20Glue%204.0%20%7C%20Athena-orange.svg)](#)
[![Streaming](https://img.shields.io/badge/Streaming-Apache%20Kafka%203.8.0-black.svg)](#)
[![Database](https://img.shields.io/badge/Serving%20DB-PostgreSQL%2015-336791.svg)](#)
[![Visualization](https://img.shields.io/badge/Dashboard-Grafana-F46800.svg)](#)

End-to-end **Lambda Architecture** deployment on AWS for multi-tenant LLM token billing, cost reconciliation, and real-time budget alerting. All implementation scripts, SQL schemas, and Grafana provisioning configs reside in their respective repository folders (`AWS_Setup/`, `jobs/`, `producers/`, `db/init/`, `grafana/`).

---

## 1. End-to-End Architecture & Intermediate Artifacts Flowchart
```mermaid
flowchart TD
    %% Ingestion Layer
    subgraph INGEST["1. Ingestion & Dual-Write Layer (EC2 Compute Node)"]
        GEN["<b>Event Producer</b><br/><i>producers/producer_and_archiver.py</i><br/>Generates JSON usage events:<br/>event_id, team_id, model, tokens_in, tokens_out, event_ts"]
        KAFKA["<b>Apache Kafka 3.8.0 (KRaft)</b><br/>Topic: <code>llm-usage-events</code> | Port: <code>9092</code><br/><b>Intermediate Output:</b> Real-time event stream buffer"]
        GEN -->|Publish JSON event| KAFKA
    end

    %% Raw Storage
    subgraph S3_RAW["2. Raw Data Lake (Amazon S3 - us-east-1)"]
        RAW_OBJ["<b>Raw Partitioned Archive</b><br/><code>s3://&lt;BUCKET&gt;/raw/dt=YYYY-MM-DD/hour=HH/*.jsonl</code><br/><b>Intermediate Output:</b> Immutable raw JSONL batches"]
        PRICE_OBJ["<b>Pricing & Budget Reference</b><br/><code>s3://&lt;BUCKET&gt;/price/price_budget.csv</code><br/>Rates per 1K tokens + team monthly limits"]
        GEN -->|Archive raw batch| RAW_OBJ
    end

    %% Batch Layer
    subgraph BATCH["3. Batch Layer (Serverless AWS Glue & Athena)"]
        GLUE_JOB["<b>AWS Glue 4.0 ETL Job (PySpark)</b><br/><i>jobs/glue_batch_etl.py</i> | 2x G.1X Workers<br/>Deduplicates event_id, validates tokens &ge; 0, joins rates"]
        DLQ["<b>Dead-Letter Queue (DLQ)</b><br/><code>s3://&lt;BUCKET&gt;/dead_letter/</code><br/><b>Intermediate Output:</b> Malformed/negative-token Parquet rows"]
        CURATED["<b>Curated Parquet Datasets</b><br/><code>s3://&lt;BUCKET&gt;/curated/events_parquet/</code><br/><code>s3://&lt;BUCKET&gt;/curated/batch_agg_parquet/</code><br/><b>Intermediate Output:</b> Hourly team/model cost rollups"]
        CRAWLER["<b>AWS Glue Crawler</b><br/>Database: <code>ai_bill_db</code><br/><b>Intermediate Output:</b> External table schemas in Glue Data Catalog"]
        ATHENA["<b>Amazon Athena</b><br/>Results: <code>s3://&lt;BUCKET&gt;/athena-results/</code><br/><b>Intermediate Output:</b> Audited SQL batch cost reports"]

        RAW_OBJ --> GLUE_JOB
        PRICE_OBJ --> GLUE_JOB
        GLUE_JOB -->|Invalid records| DLQ
        GLUE_JOB -->|Valid records| CURATED
        CURATED --> CRAWLER
        CRAWLER --> ATHENA
    end

    %% Speed Layer
    subgraph SPEED["4. Speed Layer (Real-Time Stream Consumer)"]
        CONSUMER["<b>Kafka Speed Consumer</b><br/><i>jobs/run_lambda_speed.py</i><br/>In-flight event_id deduplication + per-minute windowing"]
        KAFKA -->|Consume live stream| CONSUMER
    end

    %% Serving & Visualization
    subgraph SERVING["5. Unified Serving & Visualization Layer (EC2 Docker Stack)"]
        PG_BATCH["<b>PostgreSQL Serving: Batch Tables</b><br/><code>serving.batch_agg</code> &amp; <code>serving.batch_meta</code><br/><b>Intermediate Output:</b> Historical totals + <code>cutoff_ts</code> watermark"]
        PG_SPEED["<b>PostgreSQL Serving: Speed Tables</b><br/><code>serving.speed_agg</code> &amp; <code>serving.speed_cumulative</code><br/><b>Intermediate Output:</b> Minute-level spend &amp; running totals"]
        VIEWS["<b>Unified SQL Views</b><br/><code>serving.merged_view</code> (Batch &le; cutoff_ts + Speed &gt; cutoff_ts)<br/><code>serving.budget_status</code> (ok / warning &ge;80% / critical &ge;100%)"]
        GRAFANA["<b>Grafana Live Dashboard</b><br/>Port: <code>3000</code> | Auto-refresh: <code>5s</code><br/>4 Panels: Batch vs Stream, Budget Alerts, Cost/Min, Model Share"]

        ATHENA -->|Sync batch summary| PG_BATCH
        CONSUMER -->|Upsert live windows| PG_SPEED
        PG_BATCH --> VIEWS
        PG_SPEED --> VIEWS
        VIEWS --> GRAFANA
    end
```

---

## 2. Infrastructure & Machine Specifications

| Component | AWS Service / Software | Configuration & Sizing | Purpose |
| :--- | :--- | :--- | :--- |
| **Compute Host** | Amazon EC2 | `t3.medium` (2 vCPU, 4 GiB RAM), Amazon Linux 2023 (`x86_64`), Region `us-east-1` | Runs Docker Compose stack (Kafka, PostgreSQL, Grafana) and Python ingestion/speed workers |
| **Network & Firewall** | EC2 Security Group | Inbound TCP `22` (SSH) and TCP `3000` (Grafana UI) | Secures shell management and exposes live dashboard endpoint |
| **IAM Permissions** | IAM Role / Instance Profile | `AmazonS3FullAccess`, `AWSGlueConsoleFullAccess`, `AmazonAthenaFullAccess` | Allows EC2 producers and Glue jobs to read/write S3 buckets and catalog tables |
| **Object Storage** | Amazon S3 | Single bucket with 6 prefixes (`raw/`, `curated/`, `dead_letter/`, `scripts/`, `athena-results/`, `price/`) | Decouples raw ingestion landing from curated columnar analytics |
| **Batch Compute** | AWS Glue `4.0` | Spark 3.3 / Python 3, `2` workers of type `G.1X` (4 vCPU, 16 GB RAM per worker) | Distributed deduplication, schema validation, pricing joins, and Parquet compaction |
| **Metadata & SQL** | Glue Crawler + Athena | Catalog DB `ai_bill_db`, serverless Presto/Trino SQL engine | Automatically infers Parquet partition schemas (`dt=YYYY-MM-DD`) for ad-hoc SQL |
| **Container Runtime** | Docker & Docker Compose | `postgres:15-alpine`, `apache/kafka:3.8.0` (KRaft mode), `grafana/grafana:latest` | Self-contained low-latency messaging, relational serving store, and visualization |

---

## 3. Intermediate Data Stages & Verification Checkpoints

| Stage | Location / Artifact | Format | Key Transformation & Verification Metric |
| :--- | :--- | :--- | :--- |
| **Stage 1: Stream Buffer** | Kafka topic `llm-usage-events` | JSON | Decouples high-frequency event generation from downstream database writes |
| **Stage 2: Raw Archive** | `s3://<BUCKET>/raw/dt=.../hour=.../` | `.jsonl` | Time-partitioned raw audit trail preserving every incoming `event_id` |
| **Stage 3: Data Quality Split** | `s3://<BUCKET>/dead_letter/` vs `events_parquet/` | Snappy Parquet | Drops duplicate `event_id` rows and routes negative/null token records to DLQ |
| **Stage 4: Batch Rollup** | `s3://<BUCKET>/curated/batch_agg_parquet/` | Snappy Parquet | Hourly aggregates (`slot_start`) with `DECIMAL(14,6)` cost precision |
| **Stage 5: Cataloged Tables** | Athena DB `ai_bill_db` | Glue Catalog | Verifies row counts and total USD spend per `(team_id, model)` via SQL |
| **Stage 6: Watermark Merge** | `serving.merged_view` (PostgreSQL) | SQL View | Combines `layer = 'batch'` up to `cutoff_ts` with `layer = 'speed'` after `cutoff_ts` with zero double-counting |
| **Stage 7: Live Alerting** | `serving.budget_status` & Grafana (`:3000`) | Dashboard | Evaluates real-time spend against team caps (`team-a`: $500, `team-b`: $300, `team-c`: $200) |

---

## 4. Operational Run Order (High-Level)

1. **Provision Storage & Compute:** Create the S3 bucket prefixes and launch the `t3.medium` EC2 instance in `us-east-1` with ports `22` and `3000` open.
2. **Start Local Serving Stack:** Configure `.env` with your bucket name and bring up PostgreSQL, Kafka, and Grafana via `docker compose up -d`.
3. **Populate Raw & Batch Layers:** Run the event producer/archiver to populate `raw/`, execute the AWS Glue ETL job (`ai-bill-batch-etl`) and Crawler (`ai-bill-curated-crawler`), and verify `batch_agg_parquet` in Amazon Athena.
4. **Reconcile & Stream Live:** Sync batch summaries and `cutoff_ts` into PostgreSQL, run the live speed consumer loop, and open `http://<EC2_PUBLIC_IP>:3000` (`admin` / `admin`) to monitor live metrics and threshold alerts.



