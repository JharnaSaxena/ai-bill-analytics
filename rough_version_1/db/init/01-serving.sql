CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.batch_agg (
  team_id     text          NOT NULL,
  model       text          NOT NULL,
  hour_start  timestamptz   NOT NULL,
  tokens_in   bigint        NOT NULL DEFAULT 0,
  tokens_out  bigint        NOT NULL DEFAULT 0,
  cost        numeric(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (team_id, model, hour_start)
);

CREATE TABLE IF NOT EXISTS serving.speed_agg (
  team_id       text          NOT NULL,
  model         text          NOT NULL,
  minute_start  timestamptz   NOT NULL,
  tokens_in     bigint        NOT NULL DEFAULT 0,
  tokens_out    bigint        NOT NULL DEFAULT 0,
  cost          numeric(14,6) NOT NULL DEFAULT 0,
  PRIMARY KEY (team_id, model, minute_start)
);

CREATE TABLE IF NOT EXISTS serving.batch_meta (
  team_id       text        NOT NULL,
  model         text        NOT NULL,
  max_event_ts  timestamptz,
  cutoff_ts     timestamptz NOT NULL DEFAULT 'epoch',
  status        text        NOT NULL DEFAULT 'incomplete',
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (team_id, model)
);

CREATE OR REPLACE VIEW serving.merged_view AS
SELECT b.team_id, b.model, b.hour_start AS window_start,
       b.tokens_in, b.tokens_out, b.cost, 'batch'::text AS layer
FROM serving.batch_agg b
JOIN serving.batch_meta m
  ON m.team_id = b.team_id AND m.model = b.model
WHERE b.hour_start < m.cutoff_ts
UNION ALL
SELECT s.team_id, s.model, s.minute_start AS window_start,
       s.tokens_in, s.tokens_out, s.cost, 'speed'::text AS layer
FROM serving.speed_agg s
LEFT JOIN serving.batch_meta m
  ON m.team_id = s.team_id AND m.model = s.model
WHERE s.minute_start >= COALESCE(m.cutoff_ts, 'epoch'::timestamptz);

CREATE ROLE grafana_ro LOGIN PASSWORD 'grafana_ro';
ALTER ROLE grafana_ro SET default_transaction_read_only = on;
GRANT USAGE ON SCHEMA serving TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA serving TO grafana_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO grafana_ro;