\# AI Bill Analytics — Lambda Architecture



Phase 2 implementation.



\## Stack

\- Kafka 7.6.1 (KRaft) — message log, 7-day retention

\- Spark 3.5.1 standalone — batch + streaming processing

\- PostgreSQL 15.7 — serving layer (read-only for Grafana)

\- Grafana 11.1.0 — dashboards

\- Python 3.11 — producer + shared transformations



\## Fresh install



```bash

copy .env.example .env

mkdir data\\raw data\\price data\\dead-letter data\\checkpoints

docker compose up -d

docker compose --profile apps up -d producer stream-job

Grafana: http://localhost:3000



Shutdown

docker compose --profile apps stop

docker compose stop





