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

```markdown
## Quickstart & Setup Guide
### 1. Clone Repository & Export Environment Variables

Clone the repository and configure your environment variables:

```bash
git clone [https://github.com/JharnaSaxena/ai-bill-analytics.git](https://github.com/JharnaSaxena/ai-bill-analytics.git) ~/ai-bill
cd ~/ai-bill

export AWS_REGION="us-east-1"
export BUCKET_NAME="ai-bill-analytics-${USER:-jharna}-$(date +%s)"
export GLUE_DB="ai_bill_db"
export GLUE_JOB_NAME="ai-bill-batch-etl"
export CRAWLER_NAME="ai-bill-curated-crawler"
export EC2_PUBLIC_IP="$(curl -s [http://169.254.169.254/latest/meta-data/public-ipv4](http://169.254.169.254/latest/meta-data/public-ipv4) || echo '127.0.0.1')"

echo "BUCKET_NAME=$BUCKET_NAME | REGION=$AWS_REGION \vert{} EC2_IP=$EC2_PUBLIC_IP"
mkdir -p data/price sql scripts grafana/provisioning/datasources grafana/provisioning/dashboards grafana/dashboards
```

---

### 2. Provision IAM Role, Security Group & EC2 Instance

#### 2.1 Create IAM Role & Instance Profile (If Not Using Pre-Created Sandbox Role)

```bash
cat << 'EOF' > /tmp/ec2-trust.json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ec2.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name AIBillEC2Role --assume-role-policy-document file:///tmp/ec2-trust.json || true
aws iam attach-role-policy --role-name AIBillEC2Role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess || true
aws iam attach-role-policy --role-name AIBillEC2Role --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess || true
aws iam attach-role-policy --role-name AIBillEC2Role --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess || true
aws iam create-instance-profile --instance-profile-name AIBillEC2Profile || true
aws iam add-role-to-instance-profile --instance-profile-name AIBillEC2Profile --role-name AIBillEC2Role || true

```

#### 2.2 Create Security Group & Launch Amazon Linux 2023 Instance

```bash
VPC_ID=$(aws ec2 describe-vpcs --region "$AWS_REGION" --filters "Name=is-default,Values=true" --query "Vpcs[0].VpcId" --output text)
SG_ID=$(aws ec2 create-security-group --region "$AWS_REGION" --group-name "ai-bill-sg-$(date +%s)" --description "SSH and Grafana access" --vpc-id "$VPC_ID" --query "GroupId" --output text)

aws ec2 authorize-security-group-ingress --region "$AWS_REGION" --group-id "$SG_ID" --protocol tcp --port 22 --cidr 0.0.0.0/0
aws ec2 authorize-security-group-ingress --region "$AWS_REGION" --group-id "$SG_ID" --protocol tcp --port 3000 --cidr 0.0.0.0/0

AMI_ID=$(aws ec2 describe-images --region "$AWS_REGION" --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-x86_64" "Name=state,Values=available" \
  --query "sort_by(Images, &CreationDate)[-1].ImageId" --output text)

INSTANCE_ID=$(aws ec2 run-instances --region "$AWS_REGION" \
  --image-id "$AMI_ID" \
  --instance-type "t3.medium" \
  --security-group-ids "$SG_ID" \
  --iam-instance-profile Name="AIBillEC2Profile" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=ai-bill-lambda-node}]" \
  --query "Instances[0].InstanceId" --output text)

aws ec2 wait instance-running --region "$AWS_REGION" --instance-ids "$INSTANCE_ID"
EC2_PUBLIC_IP=$(aws ec2 describe-instances --region "$AWS_REGION" --instance-ids "$INSTANCE_ID" \
  --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

echo "INSTANCE_ID=$INSTANCE_ID | SG_ID=$SG_ID | EC2_PUBLIC_IP=$EC2_PUBLIC_IP"

```

---

### 3. Create Amazon S3 Bucket & Prefixes

```bash
aws s3 mb "s3://${BUCKET_NAME}" --region "$AWS_REGION"
for prefix in raw curated dead_letter scripts athena-results price; do
  aws s3api put-object --bucket "$BUCKET_NAME" --key "${prefix}/" --region "$AWS_REGION"
done
aws s3 ls "s3://${BUCKET_NAME}/"

```

---

### 4. Configure EC2 Environment & Docker Compose Stack

Run inside the EC2 shell (via SSH or EC2 Instance Connect):

```bash
sudo dnf update -y && sudo dnf install -y docker jq python3-pip git
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user

sudo mkdir -p /usr/libexec/docker/cli-plugins
sudo curl -SL "[https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname](https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname) -s)-$(uname -m)" \
  -o /usr/libexec/docker/cli-plugins/docker-compose
sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose

pip3 install --user kafka-python psycopg2-binary boto3
newgrp docker

```

#### 4.1 Pricing & Budget Table (`data/price/price_budget.csv`)

```bash
cat << 'EOF' > ~/ai-bill/data/price/price_budget.csv
team_id,model,input_price_per_1k,output_price_per_1k,budget_limit
team-a,gpt-4o,0.005,0.015,500.00
team-a,claude-sonnet-4,0.003,0.015,500.00
team-a,gpt-4o-mini,0.00015,0.0006,500.00
team-b,gpt-4o,0.005,0.015,300.00
team-b,claude-sonnet-4,0.003,0.015,300.00
team-b,gpt-4o-mini,0.00015,0.0006,300.00
team-c,gpt-4o,0.005,0.015,200.00
team-c,claude-sonnet-4,0.003,0.015,200.00
team-c,gpt-4o-mini,0.00015,0.0006,200.00
EOF

```

#### 4.2 Service Stack (`docker-compose.yml`)

```bash
cat << 'EOF' > ~/ai-bill/docker-compose.yml
services:
  postgres:
    image: postgres:15-alpine
    container_name: aibill-postgres
    environment:
      POSTGRES_DB: aibill
      POSTGRES_USER: aibill
      POSTGRES_PASSWORD: aibill
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./sql/init_serving.sql:/docker-entrypoint-initdb.d/init_serving.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U aibill -d aibill"]
      interval: 5s
      timeout: 5s
      retries: 10

  kafka:
    image: apache/kafka:3.8.0
    container_name: aibill-kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

  grafana:
    image: grafana/grafana:latest
    container_name: aibill-grafana
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards

volumes:
  pgdata:
EOF

```

---

### 5. PostgreSQL Serving Schema & Unified Views (`sql/init_serving.sql`)

```sql
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.batch_agg (
    team_id TEXT NOT NULL,
    model TEXT NOT NULL,
    slot_start TIMESTAMPTZ NOT NULL,
    tokens_in BIGINT NOT NULL,
    tokens_out BIGINT NOT NULL,
    cost NUMERIC(14, 6) NOT NULL,
    PRIMARY KEY (team_id, model, slot_start)
);

CREATE TABLE IF NOT EXISTS serving.batch_meta (
    team_id TEXT NOT NULL,
    model TEXT NOT NULL,
    max_event_ts TIMESTAMPTZ NOT NULL,
    cutoff_ts TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (team_id, model)
);

CREATE TABLE IF NOT EXISTS serving.speed_agg (
    team_id TEXT NOT NULL,
    model TEXT NOT NULL,
    minute_start TIMESTAMPTZ NOT NULL,
    tokens_in BIGINT NOT NULL,
    tokens_out BIGINT NOT NULL,
    cost NUMERIC(14, 6) NOT NULL,
    PRIMARY KEY (team_id, model, minute_start)
);

CREATE TABLE IF NOT EXISTS serving.speed_cumulative (
    team_id TEXT PRIMARY KEY,
    total_tokens_in BIGINT NOT NULL DEFAULT 0,
    total_tokens_out BIGINT NOT NULL DEFAULT 0,
    total_cost NUMERIC(14, 6) NOT NULL DEFAULT 0,
    budget_limit NUMERIC(14, 2) NOT NULL,
    pct_used NUMERIC(8, 2) NOT NULL DEFAULT 0,
    alert_status TEXT NOT NULL DEFAULT 'ok',
    last_event_ts TIMESTAMPTZ
);

CREATE OR REPLACE VIEW serving.merged_view AS
SELECT
    team_id,
    model,
    slot_start AS window_start,
    tokens_in,
    tokens_out,
    cost,
    'batch'::TEXT AS layer
FROM serving.batch_agg
UNION ALL
SELECT
    s.team_id,
    s.model,
    s.minute_start AS window_start,
    s.tokens_in,
    s.tokens_out,
    s.cost,
    'speed'::TEXT AS layer
FROM serving.speed_agg s
LEFT JOIN serving.batch_meta m
    ON s.team_id = m.team_id AND s.model = m.model
WHERE m.cutoff_ts IS NULL OR s.minute_start > m.cutoff_ts;

CREATE OR REPLACE VIEW serving.budget_status AS
WITH limits AS (
    SELECT team_id, MAX(budget_limit) AS budget_limit
    FROM serving.speed_cumulative
    GROUP BY team_id
),
spend AS (
    SELECT
        team_id,
        COALESCE(SUM(cost), 0) AS mtd_cost
    FROM serving.merged_view
    GROUP BY team_id
)
SELECT
    l.team_id,
    l.budget_limit,
    ROUND(COALESCE(sp.mtd_cost, 0)::NUMERIC, 4) AS mtd_cost,
    ROUND(((COALESCE(sp.mtd_cost, 0) / NULLIF(l.budget_limit, 0)) * 100)::NUMERIC, 2) AS pct_used,
    CASE
        WHEN COALESCE(sp.mtd_cost, 0) >= l.budget_limit THEN 'critical'
        WHEN COALESCE(sp.mtd_cost, 0) >= 0.8 * l.budget_limit THEN 'warning'
        ELSE 'ok'
    END AS status
FROM limits l
LEFT JOIN spend sp ON l.team_id = sp.team_id;

```

---

### 6. Grafana Provisioning, Dashboard & Stack Startup

```bash
cat << 'EOF' > ~/ai-bill/grafana/provisioning/datasources/postgres.yml
apiVersion: 1
datasources:
  - name: AIBill-Postgres
    type: postgres
    uid: aibill-pg
    url: postgres:5432
    user: aibill
    secureJsonData:
      password: aibill
    jsonData:
      database: aibill
      sslmode: disable
      postgresVersion: 1500
    isDefault: true
EOF

cat << 'EOF' > ~/ai-bill/grafana/provisioning/dashboards/default.yml
apiVersion: 1
providers:
  - name: AIBill
    type: file
    options:
      path: /var/lib/grafana/dashboards
EOF

cat << 'EOF' > ~/ai-bill/grafana/dashboards/ai_bill_overview.json
{
  "title": "AI Bill Analytics - Live Lambda Serving",
  "uid": "ai-bill-live",
  "refresh": "5s",
  "schemaVersion": 38,
  "panels": [
    {
      "id": 1,
      "title": "Lambda Unified Serving: Batch vs Stream (serving.merged_view)",
      "type": "table",
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
      "datasource": {"type": "postgres", "uid": "aibill-pg"},
      "targets": [
        {
          "rawSql": "SELECT layer, team_id, model, SUM(tokens_in) AS tokens_in, SUM(tokens_out) AS tokens_out, ROUND(SUM(cost)::numeric, 4) AS cost_usd FROM serving.merged_view GROUP BY 1, 2, 3 ORDER BY 2, 1, 3;",
          "format": "table",
          "refId": "A"
        }
      ]
    },
    {
      "id": 2,
      "title": "MTD Cost vs Budget Alert Status ($)",
      "type": "table",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0},
      "datasource": {"type": "postgres", "uid": "aibill-pg"},
      "targets": [
        {
          "rawSql": "SELECT team_id, budget_limit, mtd_cost, pct_used, status FROM serving.budget_status ORDER BY pct_used DESC;",
          "format": "table",
          "refId": "A"
        }
      ]
    },
    {
      "id": 3,
      "title": "Real-Time Stream Cost per Minute by Team ($)",
      "type": "timeseries",
      "gridPos": {"h": 9, "w": 14, "x": 0, "y": 8},
      "datasource": {"type": "postgres", "uid": "aibill-pg"},
      "targets": [
        {
          "rawSql": "SELECT minute_start AS time, team_id, SUM(cost) AS cost FROM serving.speed_agg GROUP BY 1, 2 ORDER BY 1;",
          "format": "time_series",
          "refId": "A"
        }
      ]
    },
    {
      "id": 4,
      "title": "Total Spend by LLM Model ($)",
      "type": "piechart",
      "gridPos": {"h": 9, "w": 10, "x": 14, "y": 8},
      "datasource": {"type": "postgres", "uid": "aibill-pg"},
      "targets": [
        {
          "rawSql": "SELECT model, ROUND(SUM(cost)::numeric, 4) AS total_cost FROM serving.merged_view GROUP BY 1 ORDER BY 2 DESC;",
          "format": "table",
          "refId": "A"
        }
      ]
    }
  ]
}
EOF

cd ~/ai-bill && docker compose up -d

```

---

### 7. Ingestion, Speed Consumer & Batch Sync Scripts

#### 7.1 `scripts/producer_and_archiver.py`

Publishes LLM usage events to Kafka (`llm-usage-events`) and archives hourly-partitioned JSONL batches to S3 (`raw/`).

```python
import json
import os
import random
import uuid
from datetime import datetime, timezone
import boto3
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "llm-usage-events")
BUCKET = os.environ["BUCKET_NAME"]
REGION = os.getenv("AWS_REGION", "us-east-1")

s3 = boto3.client("s3", region_name=REGION)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TEAMS = ["team-a", "team-b", "team-c"]
MODELS = ["gpt-4o", "claude-sonnet-4", "gpt-4o-mini"]


def generate_event(spike: bool = False) -> dict:
    mult = 25 if spike else 1
    return {
        "event_id": str(uuid.uuid4()),
        "team_id": "team-c" if spike else random.choice(TEAMS),
        "model": "gpt-4o" if spike else random.choice(MODELS),
        "tokens_in": random.randint(4000, 25000) * mult,
        "tokens_out": random.randint(2000, 15000) * mult,
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def emit_batch(n: int = 150, spike: bool = False):
    records = []
    for _ in range(n):
        evt = generate_event(spike=spike)
        producer.send(TOPIC, evt)
        records.append(evt)
    producer.flush()

    now = datetime.now(timezone.utc)
    key = f"raw/dt={now:%Y-%m-%d}/hour={now:%H}/events_{int(now.timestamp())}_{uuid.uuid4().hex[:8]}.jsonl"
    body = "\n".join(json.dumps(r) for r in records) + "\n"
    s3.put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"))
    print(f"[INGEST] Sent {n} events -> Kafka ({TOPIC}) & s3://{BUCKET}/{key}")


if __name__ == "__main__":
    emit_batch(n=150, spike=False)

```

#### 7.2 `scripts/run_lambda_speed.py`

Consumes Kafka events, filters duplicates and negative token counts, calculates cost from `price_budget.csv`, and updates `serving.speed_agg` and `serving.speed_cumulative`.

```python
import csv
import json
import os
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2

PG_DSN = os.getenv("PG_DSN", "dbname=aibill user=aibill password=aibill host=localhost port=5432")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "llm-usage-events")
PRICE_CSV = os.getenv("PRICE_CSV", "/home/ec2-user/ai-bill/data/price/price_budget.csv")

prices = {}
with open(PRICE_CSV, newline="") as f:
    for row in csv.DictReader(f):
        prices[(row["team_id"], row["model"])] = (
            float(row["input_price_per_1k"]),
            float(row["output_price_per_1k"]),
            float(row["budget_limit"]),
        )

conn = psycopg2.connect(PG_DSN)
conn.autocommit = True
cur = conn.cursor()

for (team_id, _model), (_, _, limit) in prices.items():
    cur.execute(
        """
        INSERT INTO serving.speed_cumulative (team_id, budget_limit)
        VALUES (%s, %s)
        ON CONFLICT (team_id) DO NOTHING;
        """,
        (team_id, limit),
    )

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="aibill-speed-consumer",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    consumer_timeout_ms=2500,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

seen_ids = set()
processed = 0

for msg in consumer:
    evt = msg.value or {}
    eid = evt.get("event_id")
    if not eid or eid in seen_ids:
        continue
    seen_ids.add(eid)

    key = (evt.get("team_id"), evt.get("model"))
    if key not in prices:
        continue

    tin = int(evt.get("tokens_in", 0))
    tout = int(evt.get("tokens_out", 0))
    if tin < 0 or tout < 0:
        continue

    pin, pout, limit = prices[key]
    cost = (tin / 1000.0) * pin + (tout / 1000.0) * pout
    ts = datetime.fromisoformat(evt["event_ts"].replace("Z", "+00:00"))
    m_start = ts.replace(second=0, microsecond=0)

    cur.execute(
        """
        INSERT INTO serving.speed_agg (team_id, model, minute_start, tokens_in, tokens_out, cost)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (team_id, model, minute_start) DO UPDATE SET
            tokens_in = serving.speed_agg.tokens_in + EXCLUDED.tokens_in,
            tokens_out = serving.speed_agg.tokens_out + EXCLUDED.tokens_out,
            cost = serving.speed_agg.cost + EXCLUDED.cost;
        """,
        (key[0], key[1], m_start, tin, tout, cost),
    )

    cur.execute(
        """
        INSERT INTO serving.speed_cumulative (
            team_id, total_tokens_in, total_tokens_out, total_cost, budget_limit, pct_used, alert_status, last_event_ts
        )
        VALUES (
            %s, %s, %s, %s, %s,
            ROUND(((%s / NULLIF(%s, 0)) * 100)::NUMERIC, 2),
            CASE
                WHEN %s >= %s THEN 'critical'
                WHEN %s >= 0.8 * %s THEN 'warning'
                ELSE 'ok'
            END,
            %s
        )
        ON CONFLICT (team_id) DO UPDATE SET
            total_tokens_in = serving.speed_cumulative.total_tokens_in + EXCLUDED.total_tokens_in,
            total_tokens_out = serving.speed_cumulative.total_tokens_out + EXCLUDED.total_tokens_out,
            total_cost = serving.speed_cumulative.total_cost + EXCLUDED.total_cost,
            pct_used = ROUND((((serving.speed_cumulative.total_cost + EXCLUDED.total_cost) / NULLIF(serving.speed_cumulative.budget_limit, 0)) * 100)::NUMERIC, 2),
            alert_status = CASE
                WHEN (serving.speed_cumulative.total_cost + EXCLUDED.total_cost) >= serving.speed_cumulative.budget_limit THEN 'critical'
                WHEN (serving.speed_cumulative.total_cost + EXCLUDED.total_cost) >= 0.8 * serving.speed_cumulative.budget_limit THEN 'warning'
                ELSE 'ok'
            END,
            last_event_ts = GREATEST(COALESCE(serving.speed_cumulative.last_event_ts, EXCLUDED.last_event_ts), EXCLUDED.last_event_ts);
        """,
        (key[0], tin, tout, cost, limit, cost, limit, cost, limit, cost, limit, ts),
    )
    processed += 1

consumer.commit()
consumer.close()
print(f"[SPEED LAYER] Processed {processed} new unique events.")

```

#### 7.3 `scripts/sync_batch_to_pg.py` & `scripts/live_stream_loop.py`

```python
# scripts/sync_batch_to_pg.py
import os
from datetime import datetime, timezone, timedelta
import psycopg2

PG_DSN = os.getenv("PG_DSN", "dbname=aibill user=aibill password=aibill host=localhost port=5432")

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

conn = psycopg2.connect(PG_DSN)
conn.autocommit = True
cur = conn.cursor()

cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
slot = cutoff - timedelta(hours=1)

cur.execute("TRUNCATE serving.batch_agg, serving.batch_meta;")
for team_id, model, tin, tout, cost in batch_rows:
    cur.execute(
        """
        INSERT INTO serving.batch_agg (team_id, model, slot_start, tokens_in, tokens_out, cost)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (team_id, model, slot, tin, tout, cost),
    )
    cur.execute(
        """
        INSERT INTO serving.batch_meta (team_id, model, max_event_ts, cutoff_ts, status)
        VALUES (%s, %s, %s, %s, 'succeeded');
        """,
        (team_id, model, cutoff, cutoff),
    )

print("[BATCH SYNC] Updated serving.batch_agg and serving.batch_meta.")

```

```python
# scripts/live_stream_loop.py
import os
import subprocess
import time
from producer_and_archiver import emit_batch

if not os.getenv("BUCKET_NAME"):
    out = subprocess.check_output(
        ["bash", "-c", "aws s3 ls | awk '/ai-bill-analytics/{print $3}' | head -n1"],
        text=True,
    ).strip()
    os.environ["BUCKET_NAME"] = out

print("Starting continuous Kafka producer + Speed Layer loop (Ctrl+C to stop)...")
while True:
    emit_batch(n=10, spike=False)
    subprocess.run(["python3", "/home/ec2-user/ai-bill/scripts/run_lambda_speed.py"], check=True)
    time.sleep(5)

```

---

### 8. AWS Glue Batch ETL, Crawler & Amazon Athena Execution

#### 8.1 `scripts/glue_batch_etl.py`

```python
import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "RAW_PATH", "CURATED_PATH", "DLQ_PATH", "PRICE_PATH"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_df = spark.read.json(args["RAW_PATH"])
prices_df = spark.read.option("header", True).option("inferSchema", True).csv(args["PRICE_PATH"])

valid_df = (
    raw_df.filter(
        F.col("event_id").isNotNull()
        & F.col("team_id").isNotNull()
        & F.col("model").isNotNull()
        & (F.col("tokens_in") >= 0)
        & (F.col("tokens_out") >= 0)
    )
    .dropDuplicates(["event_id"])
    .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
    .withColumn("dt", F.date_format("event_ts_parsed", "yyyy-MM-dd"))
)

invalid_df = raw_df.subtract(valid_df.select(raw_df.columns))
if invalid_df.limit(1).count() > 0:
    invalid_df.write.mode("append").parquet(args["DLQ_PATH"])

joined_df = valid_df.join(prices_df, on=["team_id", "model"], how="inner").withColumn(
    "cost",
    (F.col("tokens_in") / 1000.0) * F.col("input_price_per_1k")
    + (F.col("tokens_out") / 1000.0) * F.col("output_price_per_1k"),
)

joined_df.write.mode("overwrite").partitionBy("dt").parquet(f"{args['CURATED_PATH']}/events_parquet/")

agg_df = (
    joined_df.withColumn("slot_start", F.date_trunc("hour", "event_ts_parsed"))
    .groupBy("team_id", "model", "slot_start", "dt")
    .agg(
        F.sum("tokens_in").cast("bigint").alias("tokens_in"),
        F.sum("tokens_out").cast("bigint").alias("tokens_out"),
        F.sum("cost").cast("decimal(14,6)").alias("cost"),
    )
)

agg_df.write.mode("overwrite").partitionBy("dt").parquet(f"{args['CURATED_PATH']}/batch_agg_parquet/")

```

#### 8.2 Deploy & Run Glue Job, Crawler & Athena Query

```bash
export GLUE_ROLE_ARN="${GLUE_ROLE_ARN:-arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/LabRole}"

aws s3 cp ~/ai-bill/scripts/glue_batch_etl.py "s3://${BUCKET_NAME}/scripts/glue_batch_etl.py" --region "$AWS_REGION"
aws s3 cp ~/ai-bill/data/price/price_budget.csv "s3://${BUCKET_NAME}/price/price_budget.csv" --region "$AWS_REGION"

aws glue create-database --region "$AWS_REGION" --database-input "{\"Name\":\"${GLUE_DB}\"}" || true

aws glue create-job \
  --region "$AWS_REGION" \
  --name "$GLUE_JOB_NAME" \
  --role "$GLUE_ROLE_ARN" \
  --glue-version "4.0" \
  --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://${BUCKET_NAME}/scripts/glue_batch_etl.py\",\"PythonVersion\":\"3\"}" \
  --default-arguments "{
    \"--RAW_PATH\":\"s3://${BUCKET_NAME}/raw/\",
    \"--CURATED_PATH\":\"s3://${BUCKET_NAME}/curated\",
    \"--DLQ_PATH\":\"s3://${BUCKET_NAME}/dead_letter/\",
    \"--PRICE_PATH\":\"s3://${BUCKET_NAME}/price/price_budget.csv\"
  }" \
  --number-of-workers 2 \
  --worker-type "G.1X"

aws glue create-crawler \
  --region "$AWS_REGION" \
  --name "$CRAWLER_NAME" \
  --role "$GLUE_ROLE_ARN" \
  --database-name "$GLUE_DB" \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://${BUCKET_NAME}/curated/\"}]}"

JOB_RUN_ID=$(aws glue start-job-run --region "$AWS_REGION" --job-name "$GLUE_JOB_NAME" --query 'JobRunId' --output text)
aws glue wait job-run-succeeded --region "$AWS_REGION" --job-name "$GLUE_JOB_NAME" --run-id "$JOB_RUN_ID"
aws glue start-crawler --region "$AWS_REGION" --name "$CRAWLER_NAME"

QUERY_EXEC_ID=$(aws athena start-query-execution \
  --region "$AWS_REGION" \
  --query-string "SELECT team_id, model, SUM(tokens_in) AS total_tokens_in, SUM(tokens_out) AS total_tokens_out, ROUND(SUM(cost), 4) AS total_cost_usd FROM batch_agg_parquet GROUP BY team_id, model ORDER BY total_cost_usd DESC;" \
  --query-execution-context "Database=${GLUE_DB}" \
  --result-configuration "OutputLocation=s3://${BUCKET_NAME}/athena-results/" \
  --query "QueryExecutionId" --output text)

aws athena get-query-results --region "$AWS_REGION" --query-execution-id "$QUERY_EXEC_ID"

```

---

### 9. Run Unified Batch + Stream & Verify Grafana

```bash
python3 ~/ai-bill/scripts/producer_and_archiver.py
python3 ~/ai-bill/scripts/sync_batch_to_pg.py
python3 ~/ai-bill/scripts/run_lambda_speed.py

psql "dbname=aibill user=aibill password=aibill host=localhost port=5432" -c "
SELECT layer, team_id, COUNT(*) AS rows_cnt, ROUND(SUM(cost)::numeric, 4) AS total_cost
FROM serving.merged_view
GROUP BY layer, team_id
ORDER BY team_id, layer;

SELECT * FROM serving.budget_status ORDER BY pct_used DESC;
"

echo "Open Grafana at: http://${EC2_PUBLIC_IP}:3000 (admin / admin)"
python3 ~/ai-bill/scripts/live_stream_loop.py

```

---

### 10. Cleanup & Teardown

```bash
cd ~/ai-bill && docker compose down -v

aws glue delete-crawler --region "$AWS_REGION" --name "$CRAWLER_NAME" || true
aws glue delete-job --region "$AWS_REGION" --job-name "$GLUE_JOB_NAME" || true
aws glue delete-database --region "$AWS_REGION" --name "$GLUE_DB" || true
aws s3 rb "s3://${BUCKET_NAME}" --force || true

if [ -n "${INSTANCE_ID:-}" ]; then
  aws ec2 terminate-instances --region "$AWS_REGION" --instance-ids "$INSTANCE_ID"
  aws ec2 wait instance-terminated --region "$AWS_REGION" --instance-ids "$INSTANCE_ID"
fi
if [ -n "${SG_ID:-}" ]; then
  aws ec2 delete-security-group --region "$AWS_REGION" --group-id "$SG_ID" || true
fi

```

---

## License

This project is licensed under the MIT License.

```

```
