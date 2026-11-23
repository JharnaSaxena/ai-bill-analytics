CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.batch_agg (
  team_id     text NOT NULL,
  model       text NOT NULL,
  slot_start  timestamptz NOT NULL,
  tokens_in   bigint NOT NULL DEFAULT 0,
  tokens_out  bigint NOT NULL DEFAULT 0,
  cost        numeric(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (team_id, model, slot_start)
);

CREATE TABLE IF NOT EXISTS serving.speed_agg (
  team_id       text NOT NULL,
  model         text NOT NULL,
  minute_start  timestamptz NOT NULL,
  tokens_in     bigint NOT NULL DEFAULT 0,
  tokens_out    bigint NOT NULL DEFAULT 0,
  cost          numeric(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (team_id, model, minute_start)
);

CREATE TABLE IF NOT EXISTS serving.batch_meta (
  team_id       text NOT NULL,
  model         text NOT NULL,
  max_event_ts  timestamptz,
  cutoff_ts     timestamptz NOT NULL DEFAULT 'epoch',
  status        text NOT NULL DEFAULT 'incomplete',
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (team_id, model)
);

CREATE TABLE IF NOT EXISTS serving.dead_letter (
  received_at timestamptz NOT NULL DEFAULT now(),
  layer       text NOT NULL,
  reason      text NOT NULL,
  event       text
);

CREATE TABLE IF NOT EXISTS serving.dedup_stats (
  run_at              timestamptz NOT NULL DEFAULT now(),
  layer               text NOT NULL,
  raw_count           bigint NOT NULL,
  after_dedup_count   bigint NOT NULL,
  dropped_count       bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.team_budget (
  team_id      text PRIMARY KEY,
  budget_limit numeric(14,2) NOT NULL
);

INSERT INTO serving.team_budget (team_id, budget_limit) VALUES
  ('team-a', 500.00), ('team-b', 300.00), ('team-c', 200.00)
ON CONFLICT (team_id) DO UPDATE SET budget_limit = EXCLUDED.budget_limit;

CREATE OR REPLACE VIEW serving.merged_view AS
SELECT b.team_id, b.model, b.slot_start AS window_start,
       b.tokens_in, b.tokens_out, b.cost, 'batch'::text AS layer
FROM serving.batch_agg b
JOIN serving.batch_meta m ON m.team_id = b.team_id AND m.model = b.model
WHERE b.slot_start < m.cutoff_ts
UNION ALL
SELECT s.team_id, s.model, s.minute_start AS window_start,
       s.tokens_in, s.tokens_out, s.cost, 'speed'::text AS layer
FROM serving.speed_agg s
LEFT JOIN serving.batch_meta m ON m.team_id = s.team_id AND m.model = s.model
WHERE s.minute_start >= COALESCE(m.cutoff_ts, 'epoch'::timestamptz);

CREATE OR REPLACE VIEW serving.budget_status AS
SELECT b.team_id, b.budget_limit,
  COALESCE(SUM(mv.cost), 0)::numeric(14,2) AS mtd_cost,
  ROUND((COALESCE(SUM(mv.cost), 0) / b.budget_limit * 100)::numeric, 2) AS pct_used,
  CASE
    WHEN COALESCE(SUM(mv.cost), 0) >= b.budget_limit THEN 'critical'
    WHEN COALESCE(SUM(mv.cost), 0) >= 0.8 * b.budget_limit THEN 'warning'
    ELSE 'ok'
  END AS status
FROM serving.team_budget b
LEFT JOIN serving.merged_view mv
  ON mv.team_id = b.team_id AND mv.window_start >= date_trunc('month', CURRENT_DATE)
GROUP BY b.team_id, b.budget_limit
ORDER BY b.team_id;

CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA serving TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO grafana_ro;