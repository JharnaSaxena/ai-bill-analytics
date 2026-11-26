# AI Bill Analytics

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/Python-3.x-blue.svg)](https://www.python.org/)
[![PLpgSQL](https://img.shields.io/badge/PLpgSQL-PostgreSQL%2015-336791.svg)](https://www.postgresql.org/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-4.0%20%7C%20PySpark-FF9900.svg)](https://aws.amazon.com/glue/)

Lambda Architecture pipeline for LLM cost and savings analytics—combining batch and real-time streaming processing of token usage, compression savings, and team budget alerts.

---

## Overview

**AI Bill Analytics** tracks multi-tenant Large Language Model (LLM) token consumption, computes input/output token costs and savings, reconciles historical batch aggregates with near-real-time stream metrics, and surfaces live budget utilization and threshold alerts (`ok`, `warning`, `critical`) in Grafana.

* **Batch Layer:** Archives raw JSONL events to Amazon S3, deduplicates and validates records in AWS Glue (`4.0`, PySpark) with Dead-Letter Queue (DLQ) routing for invalid events, writes partitioned Parquet datasets, catalogs tables via Glue Crawler, queries historical aggregates with Amazon Athena, and syncs batch windows + watermark cutoffs into PostgreSQL.
* **Speed Layer:** Consumes real-time events from Apache Kafka (`3.8.0`) on EC2, deduplicates in-flight event IDs, computes per-minute and cumulative team spend against configurable budgets, and upserts into PostgreSQL.
* **Serving Layer:** Exposes `serving.merged_view` (switching cleanly from `speed_agg` to `batch_agg` past `batch_meta.cutoff_ts`) and `serving.budget_status` for real-time Grafana visualization.

---

## Repository Structure

```text
ai-bill-analytics/
├── aws_project/          # AWS Lambda / Glue / Athena / EC2 deployment assets
├── local_project/        # Local development & experimentation scripts/queries
├── rough_version_1/      # Early prototype & initial iteration files
├── AWS_readme.md         # Source AWS setup reference notes
└── LICENSE               # MIT License
## Sandbox & Reliability Guardrails

* **Docker Socket Permissions:** Run `sudo usermod -aG docker ec2-user && newgrp docker` (or prefix Docker commands with `sudo`) to avoid `/var/run/docker.sock` permission denied errors.
* **Security Group Rules:** Avoid creating duplicate Security Group ingress rules on port `3000`.
* **Schema Verification:** Always inspect view schemas with `SELECT *` before assuming column names (`serving.merged_view` exposes `layer` as `'batch'` or `'speed'`).
* **Sandbox Persistence:** Export Grafana dashboards (JSON) or capture screenshots before the sandbox or EC2 instance expires.

---

## System & Service Topology

| Layer / Component | AWS / Runtime Stack | Endpoint / Prefix / Schema | Role |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Apache Kafka (`3.8.0`) on EC2 | `localhost:9092`, topic `llm-usage-events` | Buffers real-time LLM usage events |
| **Raw Lake** | Amazon S3 | `s3://<BUCKET>/raw/dt=YYYY-MM-DD/hour=HH/` | Immutable JSONL event archive |
| **Batch ETL** | AWS Glue (`4.0`, PySpark) | `s3://<BUCKET>/scripts/glue_batch_etl.py` | Deduplicates, validates, prices, writes Parquet + DLQ |
| **Catalog & SQL Batch** | Glue Crawler + Amazon Athena | DB `ai_bill_db`, `s3://<BUCKET>/athena-results/` | Catalogs Parquet tables & runs batch cost SQL |
| **Speed & Serving DB** | PostgreSQL `15-alpine` on EC2 | `localhost:5432`, DB `aibill`, schema `serving` | Stores `speed_agg`, `batch_agg`, `merged_view`, `budget_status` |
| **Visualization** | Grafana (Docker) on EC2 | `http://<EC2_PUBLIC_IP>:3000` (`admin`/`admin`) | Live 5s-refresh panels for batch, stream & budgets |

## Quickstart & Setup Guide

### 1. Clone Repository & Export Environment Variables

Replace `<YOUR_GIT_REPO_URL>` with your repository URL if cloning fresh, or work directly inside your local checkout:

```bash
git clone <YOUR_GIT_REPO_URL> ~/ai-bill
cd ~/ai-bill

export AWS_REGION="us-east-1"
export BUCKET_NAME="ai-bill-analytics-${USER:-jharna}-$(date +%s)"
export GLUE_DB="ai_bill_db"
export GLUE_JOB_NAME="ai-bill-batch-etl"
export CRAWLER_NAME="ai-bill-curated-crawler"
export EC2_PUBLIC_IP="$(curl -s [http://169.254.169.254/latest/meta-data/public-ipv4](http://169.254.169.254/latest/meta-data/public-ipv4) || echo '127.0.0.1')"

echo "BUCKET_NAME=$BUCKET_NAME | REGION=$AWS_REGION | EC2_IP=$EC2_PUBLIC_IP"
mkdir -p data/price sql scripts grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards
