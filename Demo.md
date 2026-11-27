# Live Demo Command sheet  (`demo.md`)

| Phase / Window | Exact Command | What It Does |
| :--- | :--- | :--- |
| **0. Setup Directory** | `cd /d D:\BigDataProject\finally` | Switches to the project root folder containing `docker-compose.yml`. |
| **1. Pause Before Demo** | `docker compose --profile apps stop producer stream-job` | Pauses data generation and streaming without losing saved checkpoints or database rows. |
| **2. Check Containers** | `docker compose ps` | Verifies that all 5 core infrastructure containers (`kafka`, `postgres`, `spark-master`, `spark-worker`, `grafana`) are `Up`. |
| **3. Resume Stream (T1)** | `docker compose --profile apps up -d producer stream-job` | Restarts the dual-path producer and Spark Structured Streaming job from saved checkpoints. |
| **4. Tail Live Logs (T1)** | `docker compose logs -f producer stream-job` | Displays real-time event fan-out and 10-second micro-batch upserts side-by-side. |
| **5. Run Batch Parallel (T2)** | `docker compose --profile apps run --rm --no-deps batch-job` | Executes the 9-stage Spark Batch job from disk in parallel while streaming continues on T1. |
| **6. Check Lambda Split (T3)** | `docker compose exec postgres psql -U data_engineer -d analytics_db -c "SELECT layer, COUNT(*), MIN(window_start) AS first_ts, MAX(window_start) AS last_ts FROM serving.merged_view GROUP BY layer;"` | Proves `batch` and `speed` cover disjoint time ranges around `cutoff_ts` with zero overlap. |
| **7. Check Dedup Audit (T3)** | `docker compose exec postgres psql -U data_engineer -d analytics_db -c "SELECT layer, raw_count, after_dedup_count, dropped_count FROM serving.dedup_stats ORDER BY run_at DESC LIMIT 5;"` | Displays exact duplicate retry counts caught and dropped by Batch and Stream. |
| **8. Check Dead-Letter (T3)** | `docker compose exec postgres psql -U data_engineer -d analytics_db -c "SELECT reason, COUNT(*) FROM serving.dead_letter GROUP BY reason;"` | Lists malformed events isolated into `serving.dead_letter` grouped by validation failure reason. |
| **9. Check Budget Alerts (T3)** | `docker compose exec postgres psql -U data_engineer -d analytics_db -c "SELECT team_id, budget_limit, pct_used, status FROM serving.budget_status;"` | Shows live MTD team spend percentages and threshold alerts (`ok` / `warning` / `critical`). |
| **10. Open Grafana (Browser)** | `start http://localhost:3000` | Opens the live Grafana dashboard (`admin` / `admin123` → set range to **Last 30 minutes**, refresh **10s**). |
| **11. Fix Grafana List (If Needed)** | `docker compose restart grafana` | Reloads auto-provisioned dashboards if **AI Bill Analytics - Lambda** is missing from the menu. |
| **12. Safe Reset (If Needed)** | `docker compose --profile apps stop && docker compose exec postgres psql -U data_engineer -d analytics_db -c "TRUNCATE serving.batch_agg, serving.batch_meta, serving.speed_agg, serving.speed_cumulative, serving.speed_alerts, serving.dead_letter, serving.dedup_stats;"` | Stops apps first and cleanly truncates all serving tables without deleting volumes (`never use down -v`). |
