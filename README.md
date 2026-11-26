# AI Bill Analytics: A Lambda Architecture for Real-Time and Historical LLM Cost Attribution on AWS

**Project report and operations guide**
Stack: Apache Kafka · Amazon S3 · AWS Glue · Amazon Athena · PostgreSQL · Grafana · Python
Environment: AWS Academy Learner Lab (sandbox) · Amazon EC2 · Docker
Last updated: 26 November 2026

---

## Abstract

Organizations that consume large language models through APIs face a recurring operational problem: usage is metered in tokens, priced differently per model, and spread across many teams, yet the bill arrives only at the end of the billing cycle. By then, a runaway job, a misconfigured retry loop or an unexpectedly popular feature has already spent the budget. Finance wants an accurate, reconcilable history. Engineering wants to know within seconds that a team has crossed its allowance. These two needs pull in opposite directions: accuracy and completeness favour large, periodic, recomputable batch jobs, while freshness favours small, incremental, low-latency streaming computation.

This project implements a **Lambda Architecture** that serves both needs from a single logical view. LLM usage events are published to Apache Kafka on EC2. A **batch layer** (S3 raw storage, an AWS Glue ETL job, Parquet curated storage, a Glue crawler and the Glue Data Catalog, queried through Athena) produces complete historical aggregates per team and model. A **speed layer** (a Python Kafka consumer) maintains per-minute incremental aggregates for events that the batch layer has not yet processed. A **serving layer** in PostgreSQL exposes `serving.merged_view`, a unified view that unions batch and speed results, and `serving.budget_status`, a budget-versus-spend table with alert states. **Grafana** renders four auto-refreshing panels over this layer, including a side-by-side batch-versus-stream view.

In the verified run, the pipeline brokered and processed 739 unique events (740 in the final stream state), cataloged three Parquet tables, answered an Athena query over nine team/model rows in 539 ms, and drove `team-c` from normal operation into a `critical` budget alert (138.60% of a $200 budget) when a deliberate spike of high-token events was injected. This document describes the design rationale, each layer in detail, the data model, the full reproduction procedure, validation results, operational runbooks, known limitations and the roadmap.

---

## Table of Contents

1. [Introduction and Motivation](#1-introduction-and-motivation)
2. [Lambda Architecture Primer](#2-lambda-architecture-primer)
3. [System Overview](#3-system-overview)
4. [Data Model](#4-data-model)
5. [Infrastructure and Environment](#5-infrastructure-and-environment)
6. [Ingestion Layer](#6-ingestion-layer)
7. [Batch Layer](#7-batch-layer)
8. [Speed Layer](#8-speed-layer)
9. [Serving Layer](#9-serving-layer)
10. [Visualization Layer](#10-visualization-layer)
11. [Step-by-Step Reproduction Guide](#11-step-by-step-reproduction-guide)
12. [Demonstration Scenarios](#12-demonstration-scenarios)
13. [Results and Validation](#13-results-and-validation)
14. [Operations Runbook and Troubleshooting](#14-operations-runbook-and-troubleshooting)
15. [Known Issues and Limitations](#15-known-issues-and-limitations)
16. [Security Considerations](#16-security-considerations)
17. [Cost and Resource Considerations](#17-cost-and-resource-considerations)
18. [Future Work](#18-future-work)
19. [Alternative Visualization: Apache Superset](#19-alternative-visualization-apache-superset)
20. [Glossary](#20-glossary)
21. [Appendix A: Reference SQL](#appendix-a-reference-sql)
22. [Appendix B: Reference Scripts](#appendix-b-reference-scripts)
23. [Appendix C: Proposed Repository Layout](#appendix-c-proposed-repository-layout)

---

## 1. Introduction and Motivation

### 1.1 Problem statement

LLM providers bill by tokens consumed, and the unit price differs by model and by direction (input tokens versus output tokens). A single organization may run several models side by side: a frontier model for complex reasoning, a mid-tier model for general work and a small, cheap model for high-volume classification. Cost therefore depends on three things: who used it (team), what they used (model) and how much (tokens in and out).

Native provider dashboards typically offer coarse, delayed or per-API-key views. They rarely map cleanly onto internal organizational units, and they do not trigger internal budget policy. A platform or FinOps team needs:

- **Attribution:** every request tagged to a team so cost can be charged back.
- **Freshness:** spend visible within seconds to minutes, so anomalies are caught while they are still cheap.
- **Accuracy and auditability:** a historical record that can be recomputed from raw data, reconciled against invoices and corrected when pricing or logic changes.
- **Budget enforcement signals:** month-to-date spend compared with a per-team budget, with an explicit status (`ok`, `critical`) that dashboards and alerts can consume.

### 1.2 Why a single technique is not enough

A pure **batch** system (for example, a nightly job) is simple to reason about and trivially recomputable, but it is stale by design. A pure **streaming** system is fresh but harder to make exactly correct: late or duplicate events, state loss on restart and changes in pricing logic all threaten accuracy, and recomputing history means replaying the log. The Lambda Architecture resolves this by running both and merging their outputs at query time.

### 1.3 Objectives

| # | Objective | How it is met |
|---|---|---|
| O1 | Capture every LLM usage event with team and model attribution | Kafka topic `llm-usage-events` with a fixed event schema |
| O2 | Produce accurate historical aggregates | Glue ETL to partition-friendly Parquet, queryable in Athena |
| O3 | Produce low-latency aggregates for recent activity | Python speed-layer consumer writing per-minute aggregates |
| O4 | Present one coherent view of history plus recent activity | `serving.merged_view` in PostgreSQL |
| O5 | Surface budget breaches | `serving.budget_status` with `pct_used` and `status` |
| O6 | Make the system observable and demonstrable | Grafana dashboard with 5 s refresh and a batch-versus-stream panel |

### 1.4 Scope

In scope: the end-to-end pipeline from event production to dashboard, the reproduction procedure on an AWS Academy sandbox, validation and operations guidance. Out of scope: integration with a real LLM gateway, production-grade high availability, authentication and authorization models, and invoice reconciliation against provider billing exports.

---

## 2. Lambda Architecture Primer

The Lambda Architecture, introduced for large-scale data processing, decomposes a system into three layers that cooperate to answer queries over all data:

1. **Batch layer.** Stores the immutable, append-only master dataset and periodically recomputes *batch views* from it. Because it recomputes from the full raw data, it is the system of record for correctness. Its weakness is latency: views are only as fresh as the last completed run.
2. **Speed layer.** Processes only the most recent data, which the batch layer has not yet absorbed, and maintains incremental *real-time views*. It trades some simplicity and exactness for low latency. Its outputs are transient: once the batch layer catches up, the speed layer's contribution for that time range is superseded.
3. **Serving layer.** Indexes batch views (and, in practice, hosts real-time views too) so they can be queried with low latency. Queries merge batch and real-time results.

### 2.1 Mapping to this project

| Lambda concept | Component in this project |
|---|---|
| Immutable master dataset | Raw JSONL events in Amazon S3 (fed from Kafka) |
| Batch computation | AWS Glue ETL job `ai-bill-batch-etl` (Spark-based) |
| Batch views | Curated Parquet in S3, cataloged in `ai_bill_db`, mirrored to `serving.batch_agg` |
| Real-time computation | Python consumer (`run_lambda_speed.py`) reading `llm-usage-events` |
| Real-time views | `serving.speed_agg` (per-minute buckets) |
| Serving and query merge | PostgreSQL `serving` schema; `serving.merged_view` |
| Presentation | Grafana dashboard `AI Bill Analytics - Live Lambda Serving` |

### 2.2 The merge contract

For correct totals, the merged view must not double count. Each time bucket should be attributed to exactly one layer: the batch layer for buckets at or before its **cutoff** (the high-water mark of data it has fully processed), and the speed layer for buckets after the cutoff. This project records the cutoff in `serving.batch_meta`, alongside the batch run status, precisely so that the merge logic has an authoritative boundary. Section 15 discusses where the current demonstration falls short of this contract.

### 2.3 Trade-offs accepted

Lambda has a well-known cost: the same business logic (here, cost computation and aggregation) exists in two code paths and must be kept consistent. This project accepts that duplication because it makes each layer's role explicit, which is the educational and demonstrative goal. Section 18 describes how a Kappa-style or unified-engine design could remove the duplication.

---

## 3. System Overview

### 3.1 Logical architecture

```
                      ┌──────────────► S3 (raw JSONL) ──► AWS Glue ETL ──► S3 (curated Parquet)
                      │                                                            │
LLM usage events ──► Kafka (EC2)                                       Glue Crawler ──► Glue Data Catalog
 (producers)          │                                                            │
                      │                                                            ▼
                      │                                                   Amazon Athena (ad-hoc SQL)
                      │                                                            │
                      │                                              seed/load  ───┘
                      │                                                            ▼
                      └──────────────► Speed-layer consumer              serving.batch_agg / batch_meta
                                              │                                    │
                                              ▼                                    │
                                       serving.speed_agg ──────────────────────────┤
                                                                                   ▼
                                                                        serving.merged_view
                                                                                   │
                                          serving.budget_status ◄──────────────────┤
                                                   │                               │
                                                   └───────────────┬───────────────┘
                                                                   ▼
                                                      Grafana (4 live panels, 5 s refresh)
```

### 3.2 Component inventory

| Component | Type | Identifier / location | Notes |
|---|---|---|---|
| Kafka broker | Process on EC2 | `localhost:9092` (on the EC2 host) | Single broker; topic `llm-usage-events` |
| Raw store | S3 | JSONL objects | Append-only master dataset |
| ETL job | AWS Glue | `ai-bill-batch-etl` | Reads raw, writes Parquet |
| Crawler | AWS Glue | `ai-bill-curated-crawler` | Catalogs three Parquet tables |
| Catalog database | Glue Data Catalog | `ai_bill_db` | Queried by Athena |
| Ad-hoc query engine | Amazon Athena | Query editor | 539 ms for the nine-row verification query |
| Serving database | PostgreSQL | DB `aibill`, schema `serving`, port 5432 | Batch and speed views |
| Dashboard | Grafana (Docker) | Container `aibill-grafana`, port 3000 | Datasource uid `aibill-pg` |
| Speed consumer | Python script | `run_lambda_speed.py` | Reads Kafka, writes PostgreSQL |
| Load generator | Python script | `live_stream_demo.py` | One event every 3 s |
| Batch loader | Python script | `setup_dual_lambda.py` | Seeds `batch_agg` and `batch_meta` |

### 3.3 Network and ports

| Port | Service | Exposure |
|---|---|---|
| 9092 | Kafka | Host-local; not exposed publicly |
| 5432 | PostgreSQL | Host-local; reached from the Grafana container at `172.17.0.1:5432` over the Docker bridge |
| 3000 | Grafana | Exposed via an inbound security-group rule (TCP 3000) |
| 8088 | Superset (optional) | Only if Superset is deployed; requires its own inbound rule |

### 3.4 Data flow narrative

1. A producer publishes a JSON usage event to `llm-usage-events`.
2. Two independent consumers read it: a sink that lands raw JSONL in S3, and the speed-layer consumer.
3. Periodically, the Glue ETL job reads the raw JSONL, computes cost and aggregates, and writes curated Parquet. The crawler updates the catalog so Athena can query the results.
4. Batch aggregates are loaded into `serving.batch_agg`, with the run's cutoff and status recorded in `serving.batch_meta`.
5. The speed consumer continuously folds new events into `serving.speed_agg` (per-minute buckets) and refreshes `serving.budget_status`.
6. `serving.merged_view` presents both aggregates with a `layer` column identifying their origin.
7. Grafana polls PostgreSQL every five seconds and renders the dashboard.

---

## 4. Data Model

### 4.1 Event schema (Kafka message value)

Messages are UTF-8 JSON documents:

```json
{
  "event_id": "3f6c1d1a-8a52-4c0a-9f2e-0c2a7d8a9b11",
  "team_id": "team-a",
  "model": "gpt-4o",
  "tokens_in": 45000,
  "tokens_out": 12000,
  "event_ts": "2026-11-26T13:00:00+00:00"
}
```

| Field | Type | Semantics |
|---|---|---|
| `event_id` | UUID string | Globally unique identifier; the natural key for de-duplication |
| `team_id` | string | Organizational unit to which cost is attributed |
| `model` | string | Model identifier used for pricing |
| `tokens_in` | integer | Prompt (input) tokens consumed |
| `tokens_out` | integer | Completion (output) tokens produced |
| `event_ts` | ISO-8601 timestamp with UTC offset | Event time (when usage occurred), not ingestion time |

### 4.2 Dimensions and reference data

| Dimension | Values | Notes |
|---|---|---|
| Team | `team-a`, `team-b`, `team-c` | Budgets: $500, $300 and $200 respectively |
| Model | `gpt-4o`, `claude-sonnet-4`, `gpt-4o-mini` | Premium, mid-tier and economy price points |

### 4.3 Derived measure: cost

Cost per event is computed from token counts and a per-model price table:

```
cost = tokens_in  × price_in(model)  +  tokens_out × price_out(model)
```

The price table lives inside the ETL and speed-layer code. It is the single most important piece of logic that both layers must agree on (see Section 2.3). Keep prices in one shared module or configuration file to avoid drift.

### 4.4 Serving schema

| Object | Kind | Purpose | Key columns |
|---|---|---|---|
| `serving.batch_agg` | Table | Historical totals from the batch layer | team, model, window start, tokens in/out, cost |
| `serving.batch_meta` | Table | Batch run bookkeeping | team, model, cutoff, last-run time, status (`succeeded`) |
| `serving.speed_agg` | Table | Per-minute real-time aggregates | `minute_start`, `team_id`, `model`, tokens, `cost` |
| `serving.merged_view` | View | Union of batch and speed rows | `team_id, model, window_start, tokens_in, tokens_out, cost, layer` |
| `serving.budget_status` | Table or view | Month-to-date spend versus budget | `team_id, budget_limit, mtd_cost, pct_used, status` |

> **Important:** the origin column of `serving.merged_view` is named **`layer`**, not `source`. An early version of the demo script grouped by `source` and failed with `psycopg2.errors.UndefinedColumn: column "source" does not exist`. The authoritative column list, obtained with `SELECT * FROM serving.merged_view LIMIT 3`, is `['team_id', 'model', 'window_start', 'tokens_in', 'tokens_out', 'cost', 'layer']`.

---

## 5. Infrastructure and Environment

### 5.1 AWS Academy Learner Lab

All infrastructure was built inside an AWS Academy sandbox. This has operational consequences that shape the design:

- **Session timer.** The lab has a countdown (a typical session is a few hours). At 00:00 the lab stops and its resources are reclaimed. Anything not exported is lost.
- **Restricted IAM.** Learner roles limit which services and actions are available; the project sticks to EC2, S3, Glue and Athena.
- **Ephemeral addressing.** Public IPs change between lab sessions unless an Elastic IP is attached. Documentation therefore uses `<EC2_PUBLIC_IP>`.

### 5.2 EC2 host

A single EC2 instance (Amazon Linux, user `ec2-user`) hosts Kafka, PostgreSQL, the Python scripts and the Grafana container. Access is through EC2 Instance Connect in the browser. The security group must allow inbound **TCP 3000** from the viewer's address for Grafana. Kafka and PostgreSQL remain local to the host.

### 5.3 Software prerequisites on the EC2 host

| Software | Purpose | Verification |
|---|---|---|
| Docker | Runs Grafana | `sudo docker ps` |
| Apache Kafka | Event broker | Topic `llm-usage-events` exists |
| PostgreSQL | Serving layer | `psql -U aibill -d aibill -c '\dn'` shows `serving` |
| Python 3 | Scripts | `python3 --version` |
| `kafka-python`, `psycopg2` | Python clients | `python3 -c "import kafka, psycopg2"` |

### 5.4 Configuration summary

| Setting | Value |
|---|---|
| Kafka bootstrap servers (on host) | `localhost:9092` |
| Kafka topic | `llm-usage-events` |
| PostgreSQL connection (on host) | `dbname=aibill user=aibill password=aibill host=localhost port=5432` |
| PostgreSQL address from Grafana container | `172.17.0.1:5432` |
| Grafana URL | `http://<EC2_PUBLIC_IP>:3000` |
| Grafana datasource uid | `aibill-pg` |
| Glue database | `ai_bill_db` |
| Dashboard uid / title | `ai-bill-live` / `AI Bill Analytics - Live Lambda Serving` |
| Dashboard refresh | `5s` |

---

## 6. Ingestion Layer

### 6.1 Role

The ingestion layer decouples event producers from consumers. Kafka provides a durable, ordered, replayable log. That replayability is the property that makes both the batch layer (recompute from raw) and the speed layer (rebuild state after a crash) tractable.

### 6.2 Kafka topic design

- **Topic:** `llm-usage-events`
- **Key:** unset in the demo. In production, keying by `team_id` would preserve per-team ordering and enable per-team state locality.
- **Serialization:** JSON, UTF-8.
- **Partitions and replication:** single broker, so replication factor 1. Acceptable for a demo, not for production.

### 6.3 Producers

Two producers are used:

1. **Traffic generator** (`live_stream_demo.py`): emits one random event every three seconds, with `tokens_in` uniform in [15 000, 60 000] and `tokens_out` uniform in [8 000, 30 000], randomly choosing a team and a model.
2. **Spike injector:** emits 15 events for `team-c` with 1 200 000 input and 800 000 output tokens on `gpt-4o`, deliberately large to force a budget breach.

### 6.4 Raw landing in S3

Raw events are persisted as JSONL objects in S3. This append-only store is the Lambda master dataset. It is never mutated; all batch computation reads from it, which means any bug fix or price change can be applied retroactively by re-running the ETL.

### 6.5 Event-time versus processing-time

Each event carries `event_ts` generated at the producer. Aggregation windows are defined on **event time**, which is the correct basis for billing. Late-arriving events therefore land in the correct historical bucket during the next batch run, while the speed layer may place them in a recent minute bucket depending on how it keys windows (see Section 8.4).

---

## 7. Batch Layer

### 7.1 Role

The batch layer produces the authoritative, complete aggregates. It reads the entire raw dataset, computes cost, groups by team and model (and time window) and writes columnar output that is cheap to scan.

### 7.2 Pipeline stages

| Stage | Component | Input | Output |
|---|---|---|---|
| 1. Extract | Glue ETL job `ai-bill-batch-etl` | Raw JSONL in S3 | In-memory DataFrame |
| 2. Transform | Same job | Events | Cost per event; aggregates per team/model/window |
| 3. Load | Same job | Aggregates | Parquet files in the curated S3 prefix |
| 4. Catalog | Crawler `ai-bill-curated-crawler` | Parquet files | Three tables in `ai_bill_db` |
| 5. Query | Amazon Athena | Catalog tables | Result sets and CSV export |

### 7.3 Why Parquet

Parquet is columnar, compressed and carries its schema. For aggregate queries over a few columns (team, model, tokens, cost) Athena reads only the required columns, and scanned bytes (the unit of Athena billing) drop sharply compared with JSON.

### 7.4 Athena verification

The verification query returned all **nine** team/model combinations in **539 ms**. The batch aggregates observed were:

| Team | Model | Tokens in | Tokens out | Cost (USD) |
|---|---|---:|---:|---:|
| team-a | gpt-4o | 184 119 | 100 439 | 2.4272 |
| team-b | gpt-4o | 182 436 | 84 218 | 2.1755 |
| team-c | gpt-4o | 149 036 | 78 735 | 1.9262 |
| team-b | claude-sonnet-4 | 154 474 | 90 523 | 1.8213 |
| team-c | claude-sonnet-4 | 153 978 | 75 094 | 1.5883 |
| team-a | claude-sonnet-4 | 130 650 | 74 917 | 1.5157 |
| team-c | gpt-4o-mini | 171 382 | 92 958 | 0.0815 |
| team-a | gpt-4o-mini | 172 169 | 86 149 | 0.0775 |
| team-b | gpt-4o-mini | 149 360 | 77 887 | 0.0691 |

Derived roll-ups (computed from the table above):

| Team | Tokens in | Tokens out | Batch cost (USD) |
|---|---:|---:|---:|
| team-a | 486 938 | 261 505 | 4.0204 |
| team-b | 486 270 | 252 628 | 4.0659 |
| team-c | 474 396 | 246 787 | 3.5960 |
| **Total** | **1 447 604** | **760 920** | **11.6823** |

| Model | Batch cost (USD) | Share |
|---|---:|---:|
| gpt-4o | 6.5289 | 55.9% |
| claude-sonnet-4 | 4.9253 | 42.2% |
| gpt-4o-mini | 0.2281 | 2.0% |

The mix illustrates why per-model attribution matters: the economy model carries similar token volume to the others but contributes about 2% of cost.

### 7.5 Loading batch results into the serving layer

`setup_dual_lambda.py` truncates `serving.batch_agg` and `serving.batch_meta`, then inserts one row per team/model into each. A **cutoff** timestamp (one hour before now) is recorded in `batch_meta` along with the status `succeeded`, and the batch window start is set one hour earlier. In a production flow this step would be automated (for example, a post-ETL job that reads Athena or Parquet output and upserts into PostgreSQL).

### 7.6 Scheduling

In the sandbox, the ETL is run on demand. In production, schedule it with a Glue trigger or Amazon EventBridge (for instance hourly or daily), and have the loader record the cutoff derived from the maximum `event_ts` actually processed rather than a wall-clock offset.

---

## 8. Speed Layer

### 8.1 Role

The speed layer closes the freshness gap. It consumes new events as they arrive and updates `serving.speed_agg` within a second or two, keeping `serving.budget_status` current between batch runs.

### 8.2 Processing logic

For each event consumed from `llm-usage-events`:

1. Parse the JSON payload.
2. Compute `cost` from tokens and the model price table.
3. Truncate `event_ts` to the minute to obtain `minute_start`.
4. Upsert into `serving.speed_agg` keyed by `(minute_start, team_id, model)`, adding tokens and cost.
5. Refresh `serving.budget_status`: recompute each team's month-to-date cost, `pct_used = mtd_cost / budget_limit × 100`, and set `status`.

The consumer reports its progress in the form `[SPEED LAYER] Processed N unique live Kafka events into serving.speed_agg!`. In the verified run N reached 739 and then 740.

### 8.3 Alert semantics

| Condition | `status` |
|---|---|
| `pct_used` below the threshold | `ok` |
| `pct_used` at or above 100% | `critical` |

Observed: `team-c` reached $277.19 against a $200 budget (**138.60%**) after the spike test and was reported `critical`, while `team-a` ($8.04 of $500) and `team-b` ($8.13 of $300) remained `ok`.

### 8.4 Windowing choices

- **Granularity:** one-minute buckets suit the "cost per minute" panel and keep the table small.
- **Event time:** buckets are keyed from `event_ts`. A very late event would land in an older minute, which is correct but may fall inside a range already covered by the batch layer (see Section 15.2).
- **State:** all state is in PostgreSQL, so the consumer itself is stateless and restartable.

### 8.5 Delivery guarantees

Kafka consumers are at-least-once unless offsets and writes are made atomic. Duplicates are therefore possible after a restart. `event_id` is the intended de-duplication key; Section 15.1 discusses idempotency in more detail.

---

## 9. Serving Layer

### 9.1 Role

The serving layer is the single place dashboards and applications query. Hiding the batch/speed split behind `serving.merged_view` means consumers never need to know which layer produced a row, although the `layer` column exposes it when needed (for demos and debugging).

### 9.2 Merge semantics

`serving.merged_view` is a union of:

- rows from `serving.batch_agg` labelled `layer = 'batch'`, and
- rows from `serving.speed_agg` labelled `layer = 'speed'`,

projected onto the common columns `team_id, model, window_start, tokens_in, tokens_out, cost, layer`. Typical queries aggregate over this view:

```sql
SELECT layer, team_id, COUNT(*) AS buckets, ROUND(SUM(cost)::numeric, 4) AS total_cost
FROM serving.merged_view
GROUP BY layer, team_id
ORDER BY team_id, layer;
```

### 9.3 Budget status

`serving.budget_status` has the columns `team_id, budget_limit, mtd_cost, pct_used, status` and is ordered by `pct_used` descending on the dashboard so that the riskiest team is always at the top.

### 9.4 Reference DDL (illustrative)

The exact DDL used in the sandbox is not reproduced here. The following reconstruction matches the observed column names and semantics and can be used to rebuild the serving schema from scratch:

```sql
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.batch_agg (
  team_id      text        NOT NULL,
  model        text        NOT NULL,
  window_start timestamptz NOT NULL,
  tokens_in    bigint      NOT NULL,
  tokens_out   bigint      NOT NULL,
  cost         numeric(14,6) NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.batch_meta (
  team_id      text        NOT NULL,
  model        text        NOT NULL,
  cutoff_ts    timestamptz NOT NULL,
  last_run_ts  timestamptz NOT NULL,
  status       text        NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.speed_agg (
  minute_start timestamptz NOT NULL,
  team_id      text        NOT NULL,
  model        text        NOT NULL,
  tokens_in    bigint      NOT NULL DEFAULT 0,
  tokens_out   bigint      NOT NULL DEFAULT 0,
  cost         numeric(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (minute_start, team_id, model)
);

CREATE OR REPLACE VIEW serving.merged_view AS
SELECT team_id, model, window_start, tokens_in, tokens_out, cost, 'batch'::text AS layer
FROM serving.batch_agg
UNION ALL
SELECT team_id, model, minute_start AS window_start, tokens_in, tokens_out, cost, 'speed'::text AS layer
FROM serving.speed_agg;
```

A cutoff-aware variant, which is the correct contract described in Section 2.2, would filter the speed rows to `minute_start >= (SELECT MAX(cutoff_ts) FROM serving.batch_meta WHERE status = 'succeeded')`.

---

## 10. Visualization Layer

### 10.1 Why Grafana

Grafana is lightweight (a single container), has a native PostgreSQL datasource, refreshes dashboards on an interval and supports threshold coloring, which suits operational cost monitoring on a small EC2 instance.

### 10.2 Provisioning

Grafana is run with two bind mounts so configuration is code, not clicks:

- `/home/ec2-user/grafana/provisioning` mounted at `/etc/grafana/provisioning` (datasources and dashboard providers)
- `/home/ec2-user/grafana/dashboards` mounted at `/var/lib/grafana/dashboards` (dashboard JSON)

The dashboard JSON file is `ai_bill_overview.json`. After rewriting it, restart the container (`sudo docker restart aibill-grafana`) to reload.

### 10.3 Datasource

| Property | Value |
|---|---|
| Type | `postgres` |
| UID | `aibill-pg` |
| Host from container | `172.17.0.1:5432` (Docker bridge gateway to the host) |
| Database / user | `aibill` / `aibill` |

### 10.4 Dashboard panels

Dashboard `AI Bill Analytics - Live Lambda Serving` (uid `ai-bill-live`, refresh `5s`) contains four panels:

| # | Title | Type | Query intent |
|---|---|---|---|
| 1 | Lambda Unified Serving: Batch + Stream Merged | Table | Totals by team and model from `serving.merged_view` |
| 2 | MTD Cost vs Budget Alert Status ($) | Table | `serving.budget_status` ordered by `pct_used` descending |
| 3 | Real-Time Stream Cost per Minute by Team ($) | Time series | `serving.speed_agg` grouped by `minute_start`, `team_id` |
| 4 | Total Spend by LLM Model ($) | Pie chart | `serving.merged_view` grouped by `model` |

Panel 1 query:

```sql
SELECT team_id, model,
       SUM(tokens_in)  AS total_tokens_in,
       SUM(tokens_out) AS total_tokens_out,
       ROUND(SUM(cost)::numeric, 4) AS total_cost_usd
FROM serving.merged_view
GROUP BY 1, 2
ORDER BY 5 DESC;
```

To show the layers explicitly side by side, replace the grouping with `layer, team_id` (note the column name `layer`).

### 10.5 Access

Open `http://<EC2_PUBLIC_IP>:3000`, sign in, skip the password-change prompt if shown, then use the menu **Dashboards → AI Bill Analytics - Live Lambda Serving**. Click the dashboard **title** to open it; ticking the checkbox in the list only selects it.

---

## 11. Step-by-Step Reproduction Guide

> Run commands on the EC2 host through EC2 Instance Connect unless stated otherwise.

### Step 1. Start the lab and open EC2

1. Start the AWS Academy lab and wait for the indicator to turn green.
2. Open the EC2 console, select the instance and connect with **EC2 Instance Connect**.
3. In the instance security group, add an inbound rule for TCP 3000.

### Step 2. Prepare Kafka and the topic

Ensure Kafka is running and create the topic if it is not present:

```bash
# paths vary by install; adjust to your Kafka directory
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic llm-usage-events --partitions 1 --replication-factor 1
```

### Step 3. Prepare PostgreSQL

Create the database, user and the `serving` schema (see the reference DDL in Section 9.4).

### Step 4. Generate raw events and land them in S3

Produce a baseline set of events (the verified run processed 739) and persist them as JSONL in the raw S3 prefix.

### Step 5. Run the batch layer

1. Run the Glue job `ai-bill-batch-etl` and wait for **Succeeded**.
2. Run crawler `ai-bill-curated-crawler`; confirm three tables appear in `ai_bill_db`.
3. In Athena, run an aggregate query over the curated table and confirm nine team/model rows.

### Step 6. Load batch results into the serving layer

```bash
python3 ~/setup_dual_lambda.py
```

Because the view column is `layer`, make sure the verification query inside this script groups by `layer`, not `source`.

### Step 7. Start the speed layer

```bash
python3 ~/run_lambda_speed.py
```

Confirm the log reports processed events and that `serving.budget_status` has rows for all three teams.

### Step 8. Start Grafana

```bash
sudo docker start aibill-grafana 2>/dev/null || sudo docker run -d --name aibill-grafana -p 3000:3000 \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=<choose-a-password> \
  -v /home/ec2-user/grafana/provisioning:/etc/grafana/provisioning \
  -v /home/ec2-user/grafana/dashboards:/var/lib/grafana/dashboards \
  grafana/grafana:latest
```

### Step 9. Start live traffic

```bash
python3 ~/live_stream_demo.py
```

### Step 10. Open the dashboard

Browse to `http://<EC2_PUBLIC_IP>:3000`, open the dashboard and confirm the batch and speed rows and the rising cost-per-minute line.

### Step 11. Export evidence before the timer ends

Screenshot Grafana, the Athena results and the EC2 terminal showing `serving.budget_status`; download the Athena CSV; copy the dashboard JSON into the repository.

---

## 12. Demonstration Scenarios

### 12.1 Scenario A: batch and stream together

**Goal:** show that historical (batch) and recent (speed) data are served from one view.

1. Seed batch data with `setup_dual_lambda.py`.
2. Start `live_stream_demo.py`.
3. In Grafana panel 1, or in SQL, group `serving.merged_view` by `layer`.

**Expected:** rows for both `batch` and `speed`; speed totals grow every few seconds while batch totals stay constant.

### 12.2 Scenario B: budget breach

**Goal:** show the speed layer detecting a breach before any batch run.

1. Note `team-c` at `ok`.
2. Publish the spike (15 events × 2 000 000 tokens on `gpt-4o`).
3. Watch `serving.budget_status`.

**Expected:** `team-c` moves to `critical`. In the verified run it reached $277.19 of $200 (138.60%).

### 12.3 Scenario C: recovery after restart

**Goal:** show the stateless consumer resuming.

1. Stop `live_stream_demo.py` and the consumer.
2. Restart them.
3. Confirm the processed-event counter continues and Grafana resumes updating.

### 12.4 Scenario D: schema discovery

**Goal:** recover from a wrong assumption about column names.

```sql
SELECT * FROM serving.merged_view LIMIT 3;
```

Inspect `cursor.description` (or the column header) to learn the true names, then correct the query.

---

## 13. Results and Validation

### 13.1 Summary of verified outcomes

| Check | Result |
|---|---|
| Events brokered by Kafka | 739 unique events at the first full run; 740 after additional streaming |
| Raw landing in S3 | JSONL objects present |
| Glue ETL job | Succeeded |
| Crawler | Cataloged 3 Parquet tables into `ai_bill_db` |
| Athena | 9 team/model rows returned in 539 ms |
| Speed layer | Processed all events and updated `serving.budget_status` |
| Serving view | Both `batch` and `speed` layers visible in `serving.merged_view` |
| Grafana | 4 panels, 5 s auto-refresh, reachable on port 3000 |
| Alerting | `team-c` flagged `critical` after the spike |

### 13.2 Budget status snapshots

First verified state (after 739 events including the spike test):

| Team | MTD cost (USD) | Budget (USD) | % used | Status |
|---|---:|---:|---:|---|
| team-a | 8.04 | 500.00 | 1.6% | ok |
| team-b | 8.13 | 300.00 | 2.7% | ok |
| team-c | 277.19 | 200.00 | 138.60% | critical |

Later state while `live_stream_demo.py` was running (740 processed events): `team-c` was reported at **$554.38 (277.19%)**. This is exactly twice the earlier figure, which is not explained by one additional ordinary event. See Section 15.1 for the likely cause and the fix.

### 13.3 Acceptance checklist

- [ ] `SELECT DISTINCT layer FROM serving.merged_view;` returns both `batch` and `speed`.
- [ ] `SELECT COUNT(*) FROM serving.budget_status;` returns 3.
- [ ] Grafana panel 3 shows points advancing at least every 5 s while the generator runs.
- [ ] After the spike, `team-c` is `critical` within seconds.
- [ ] Athena returns nine rows for the batch aggregate.
- [ ] Re-running the speed consumer on the same data does not change totals (idempotency test; currently expected to fail, see 15.1).

---

## 14. Operations Runbook and Troubleshooting

### 14.1 Quick health checks

```bash
sudo docker ps                                   # aibill-grafana should be Up, 0.0.0.0:3000->3000/tcp
python3 - << 'EOF'
import psycopg2
c = psycopg2.connect("dbname=aibill user=aibill password=aibill host=localhost port=5432")
cur = c.cursor()
cur.execute("SELECT layer, COUNT(*), ROUND(SUM(cost)::numeric,4) FROM serving.merged_view GROUP BY layer")
print(cur.fetchall())
EOF
```

### 14.2 Symptom table

| Symptom | Likely cause | Resolution |
|---|---|---|
| `column "source" does not exist` | View column is `layer` | Use `layer`; inspect with `SELECT * ... LIMIT 1` |
| Grafana shows stale numbers | Generator or consumer stopped; container down | `sudo docker ps`; restart `aibill-grafana`; rerun `live_stream_demo.py` |
| Grafana unreachable | Security group lacks TCP 3000, public IP changed or lab stopped | Add the rule; use the new public IP; restart the lab |
| Everything stopped at once | Sandbox timer reached 00:00 | Click **Start Lab**, then restart Kafka, PostgreSQL, Grafana and the scripts |
| Panels show "No data" | Datasource host wrong | In the container, use `172.17.0.1:5432`, not `localhost` |
| Dashboard edits ignored | Container not reloaded | `sudo docker restart aibill-grafana` |
| Glue job stuck in Running | Large input or cold start | Wait for completion; check run logs before re-running |
| Athena returns zero rows | Crawler not run after ETL | Re-run `ai-bill-curated-crawler` |
| Duplicate-looking spend | Events processed twice | See Section 15.1 |

### 14.3 Restarting after a sandbox reset

1. Start the lab; note the new public IP.
2. Reconnect with EC2 Instance Connect; confirm Kafka and PostgreSQL are up (data on the instance may or may not persist depending on how the lab was stopped).
3. Restart Grafana and the scripts in the order: PostgreSQL → Kafka → consumer → Grafana → generator.
4. If the S3 data or catalog was reclaimed, re-run the ETL and crawler.

### 14.4 Stopping cleanly

Press `Ctrl+C` in the terminal running `live_stream_demo.py`. Stop Grafana with `sudo docker stop aibill-grafana` if the instance will be reused.

---

## 15. Known Issues and Limitations

This section is deliberately candid so that readers can weigh the results correctly.

### 15.1 Possible double counting and weak idempotency

During the run, `team-c`'s total moved from **$277.19** to **$554.38**, exactly 2×, while the processed-event count only moved from 739 to 740. A one-event increment cannot account for a doubling. Plausible explanations (not verified against the code) are:

- the speed-layer consumer re-reading from the earliest offset after being invoked again, adding the spike events a second time;
- `budget_status` being recomputed from a source that includes the same events twice (for example, a join or union that counts batch and speed rows for the same window);
- non-idempotent upserts that add rather than set.

**Recommended fixes:**

1. De-duplicate on `event_id` (keep a processed-events table or a unique constraint) so replays are harmless.
2. Make offset commits and writes atomic, or make writes idempotent.
3. Compute `budget_status` from a single authoritative expression over `merged_view` with a cutoff filter.

### 15.2 Merge overlap

The demo's batch rows use a window one hour before the cutoff, but the speed layer keeps all buckets since it started. Unless the view filters speed rows to times after the batch cutoff, any overlap is counted by both layers. Implement the cutoff-aware view in Section 9.4.

### 15.3 Script structure

`live_stream_demo.py` calls the speed layer by `exec(open("/home/ec2-user/run_lambda_speed.py").read())` on every loop iteration. This works for a demonstration but re-executes the whole script each time and obscures errors. Replace it with an import and a function call, or run the consumer as a separate long-lived process.

### 15.4 Seeded batch values

`setup_dual_lambda.py` seeds `batch_agg` from hard-coded values copied from the Athena output rather than reading from S3/Athena programmatically. This was a pragmatic bridge; it should be replaced by an automated loader.

### 15.5 Single points of failure

One Kafka broker, one PostgreSQL instance and one Grafana container on one EC2 host. Any failure interrupts the pipeline.

### 15.6 Ephemeral environment

All resources in the AWS Academy sandbox are reclaimed when the lab ends.

### 15.7 Simplified pricing and thresholds

The price table is static and the alert rule has a single `critical` threshold. There is no `warning` band, no per-model price history and no currency handling.

---

## 16. Security Considerations

| Area | Current state in the demo | Recommendation |
|---|---|---|
| Grafana credentials | Default administrator account | Set a strong password at first login; use SSO where possible |
| PostgreSQL credentials | Simple demo user/password stored in scripts | Use secrets (AWS Secrets Manager or environment variables); never commit them |
| Network exposure | Port 3000 open via security group | Restrict the source CIDR to your own IP; put Grafana behind HTTPS (reverse proxy or load balancer) |
| Kafka | Unauthenticated, local | Enable SASL/TLS and ACLs in any shared environment |
| S3 | Raw and curated data | Block public access, enable bucket encryption, scope IAM roles to required prefixes |
| Data content | Token counts and team ids, no prompts or PII | Keep it that way; never log prompt or completion text in usage events |

---

## 17. Cost and Resource Considerations

- **Athena** charges by data scanned. Parquet and column pruning keep verification queries cheap; partitioning curated data by date would reduce scans further.
- **Glue** charges by DPU-hours. Use the smallest worker configuration that completes the job and avoid unnecessary re-runs.
- **EC2** is the dominant fixed cost in a long-lived deployment; the demo fits on a small instance.
- **S3** storage is negligible at this scale, but enable lifecycle rules for raw data in production.
- **Grafana vs. Superset:** Grafana is far lighter; Superset needs a metadata database and often a cache and workers.

---

## 18. Future Work

1. **Unify the logic.** Use a single engine (for example Spark Structured Streaming, Apache Flink or Kafka Streams) for both batch backfill and streaming, moving toward a Kappa-style architecture and eliminating duplicated cost logic.
2. **Exactly-once semantics.** Idempotent sink writes keyed by `event_id`, transactional offset commits.
3. **Cutoff-aware merge.** Derive the boundary from the maximum processed `event_ts` and filter the speed rows in the view.
4. **Managed services.** Amazon MSK for Kafka, Amazon RDS or Aurora PostgreSQL for serving, Amazon Managed Grafana for dashboards.
5. **Alerting.** Grafana alert rules and notifications (email, Slack) with `warning` (for example 80%) and `critical` (100%) tiers and forecast-based alerts for projected month-end overrun.
6. **Pricing service.** A versioned price catalog with effective dates so historical recomputation uses the correct price at the time.
7. **Data quality checks.** Schema validation at ingestion, dead-letter topic for malformed events, reconciliation of batch versus speed totals.
8. **Infrastructure as code.** Terraform or CloudFormation for EC2, security groups, S3, Glue and IAM.
9. **CI and tests.** Unit tests for the cost function, integration tests with a Dockerized Kafka and PostgreSQL.
10. **Cost forecasting and anomaly detection** on `speed_agg` time series.

---

## 19. Alternative Visualization: Apache Superset

Superset is a strong complement when the goal is ad-hoc exploration rather than operational monitoring.

| Aspect | Grafana (used here) | Apache Superset |
|---|---|---|
| Footprint | Single lightweight container | Web app plus metadata database, often cache and workers |
| Best fit | Live operational metrics, auto-refresh, alert thresholds | SQL Lab, pivots, cross-filtering, BI-style slicing |
| PostgreSQL connection | Native datasource at `172.17.0.1:5432` | SQLAlchemy URI `postgresql://<user>:<password>@172.17.0.1:5432/aibill` |
| Default port | 3000 | 8088 (requires its own security-group rule) |

Suggested approach: run Superset with a custom `SECRET_KEY`, create an admin user, register the `aibill` database and build charts over `serving.merged_view`, `serving.speed_agg` and `serving.budget_status`, filtering by `team_id`, `model` and `layer`. On a constrained sandbox instance, running Superset locally against exported data is more reliable than co-hosting it with Kafka and Grafana.

---

## 20. Glossary

| Term | Meaning |
|---|---|
| Lambda Architecture | Data-processing design combining a batch layer, speed layer and serving layer |
| Batch layer | Recomputes complete views from immutable raw data on a schedule |
| Speed layer | Maintains low-latency incremental views for recent data |
| Serving layer | Queryable store that exposes the merged views |
| Cutoff | High-water mark separating data owned by the batch layer from data owned by the speed layer |
| Event time | When the usage actually happened, as recorded by the producer |
| Processing time | When the system handled the event |
| Idempotent | Applying the same operation more than once gives the same result |
| At-least-once | Delivery guarantee under which duplicates are possible |
| Parquet | Columnar, compressed file format that carries its schema |
| Glue Data Catalog | Central metadata store for tables used by Athena and Glue |
| MTD | Month-to-date |
| DPU | Data processing unit, the billing unit of AWS Glue |
| FinOps | Practice of managing and optimizing cloud and usage costs |

---

## Appendix A: Reference SQL

**Layers side by side**

```sql
SELECT layer AS lambda_layer, team_id,
       SUM(tokens_in) AS tokens_in, SUM(tokens_out) AS tokens_out,
       ROUND(SUM(cost)::numeric, 4) AS cost_usd
FROM serving.merged_view
GROUP BY 1, 2
ORDER BY 2, 1;
```

**Budget status**

```sql
SELECT team_id, budget_limit, mtd_cost, pct_used, status
FROM serving.budget_status
ORDER BY pct_used DESC;
```

**Cost per minute (Grafana time series)**

```sql
SELECT minute_start AS time, team_id, SUM(cost) AS cost
FROM serving.speed_agg
GROUP BY 1, 2
ORDER BY 1;
```

**Spend by model**

```sql
SELECT model, ROUND(SUM(cost)::numeric, 4) AS total_cost
FROM serving.merged_view
GROUP BY 1
ORDER BY 2 DESC;
```

**Overlap detector (should return zero rows once the merge is cutoff-aware)**

```sql
SELECT s.team_id, s.model, s.minute_start
FROM serving.speed_agg s
JOIN serving.batch_meta m USING (team_id, model)
WHERE s.minute_start < m.cutoff_ts;
```

---

## Appendix B: Reference Scripts

### B.1 Traffic generator (`live_stream_demo.py`)

```python
import json, uuid, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers="localhost:9092",
                  value_serializer=lambda v: json.dumps(v).encode())
teams  = ["team-a", "team-b", "team-c"]
models = ["gpt-4o", "claude-sonnet-4", "gpt-4o-mini"]

print("Streaming live events into Kafka + Speed Layer (Press Ctrl+C to stop)...")
while True:
    evt = {
        "event_id":  str(uuid.uuid4()),
        "team_id":   random.choice(teams),
        "model":     random.choice(models),
        "tokens_in": random.randint(15000, 60000),
        "tokens_out": random.randint(8000, 30000),
        "event_ts":  datetime.now(timezone.utc).isoformat(),
    }
    p.send("llm-usage-events", evt)
    p.flush()
    # NOTE: prefer running the consumer as a separate process instead of exec()
    exec(open("/home/ec2-user/run_lambda_speed.py").read())
    time.sleep(3)
```

### B.2 Spike injector

```python
import json, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

p = KafkaProducer(bootstrap_servers="localhost:9092",
                  value_serializer=lambda v: json.dumps(v).encode())
for _ in range(15):
    p.send("llm-usage-events", {
        "event_id": str(uuid.uuid4()),
        "team_id": "team-c",
        "model": "gpt-4o",
        "tokens_in": 1_200_000,
        "tokens_out": 800_000,
        "event_ts": datetime.now(timezone.utc).isoformat(),
    })
p.flush()
print("Sent 15 high-token spike events for team-c!")
```

### B.3 Batch loader (corrected)

```python
import psycopg2
from datetime import datetime, timezone, timedelta

conn = psycopg2.connect("dbname=aibill user=aibill password=aibill host=localhost port=5432")
conn.autocommit = True
cur = conn.cursor()

batch_rows = [
    ("team-a", "gpt-4o", 184119, 100439, 2.4272),
    ("team-b", "gpt-4o", 182436, 84218, 2.1755),
    ("team-c", "gpt-4o", 149036, 78735, 1.9262),
    ("team-b", "claude-sonnet-4", 154474, 90523, 1.8213),
    ("team-c", "claude-sonnet-4", 153978, 75094, 1.5883),
    ("team-a", "claude-sonnet-4", 130650, 74917, 1.5157),
    ("team-c", "gpt-4o-mini", 171382, 92958, 0.0815),
    ("team-a", "gpt-4o-mini", 172169, 86149, 0.0775),
    ("team-b", "gpt-4o-mini", 149360, 77887, 0.0691),
]

cur.execute("TRUNCATE serving.batch_agg, serving.batch_meta;")
cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
slot = cutoff - timedelta(hours=1)

for t, m, tin, tout, cost in batch_rows:
    cur.execute("INSERT INTO serving.batch_agg VALUES (%s,%s,%s,%s,%s,%s);",
                (t, m, slot, tin, tout, cost))
    cur.execute("INSERT INTO serving.batch_meta VALUES (%s,%s,%s,%s,'succeeded');",
                (t, m, cutoff, cutoff))

# Fixed: the view's origin column is `layer`, not `source`
cur.execute("""
SELECT layer, team_id, COUNT(*) AS buckets, ROUND(SUM(cost)::numeric, 4) AS total_cost
FROM serving.merged_view
GROUP BY layer, team_id
ORDER BY team_id, layer;
""")
for r in cur.fetchall():
    print(f"Layer: {r[0]:6s} | Team: {r[1]:6s} | Buckets: {r[2]:3d} | Cost: ${r[3]}")
```

### B.4 Schema discovery helper

```python
import psycopg2
conn = psycopg2.connect("dbname=aibill user=aibill password=aibill host=localhost port=5432")
cur = conn.cursor()
cur.execute("SELECT * FROM serving.merged_view LIMIT 3;")
cols = [d[0] for d in cur.description]
print("Columns:", cols)
for r in cur.fetchall():
    print(dict(zip(cols, r)))
```

---

## Appendix C: Proposed Repository Layout

```
ai-bill-analytics/
├── README.md
├── README.html
├── docs/
│   └── screenshots/           # Grafana, Athena, EC2 terminal evidence
├── ingestion/
│   ├── live_stream_demo.py
│   └── spike_injector.py
├── batch/
│   ├── glue_etl_job.py        # ai-bill-batch-etl
│   └── athena_queries.sql
├── speed/
│   └── run_lambda_speed.py
├── serving/
│   ├── schema.sql             # DDL from Section 9.4
│   └── setup_dual_lambda.py
├── grafana/
│   ├── provisioning/
│   └── dashboards/
│       └── ai_bill_overview.json
└── config/
    └── prices.yaml            # single source of truth for model prices
```
