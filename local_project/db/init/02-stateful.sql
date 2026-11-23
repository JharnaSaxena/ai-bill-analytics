CREATE TABLE IF NOT EXISTS serving.speed_cumulative (
  team_id          text PRIMARY KEY,
  total_tokens_in  bigint NOT NULL DEFAULT 0,
  total_tokens_out bigint NOT NULL DEFAULT 0,
  total_cost       numeric(14,6) NOT NULL DEFAULT 0,
  budget_limit     numeric(14,2) NOT NULL,
  pct_used         numeric(10,2) NOT NULL DEFAULT 0,
  alert_status     text NOT NULL DEFAULT 'ok',
  last_event_ts    timestamptz,
  last_updated     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS serving.speed_alerts (
  alert_ts   timestamptz NOT NULL DEFAULT now(),
  team_id    text NOT NULL,
  threshold  text NOT NULL,
  pct_used   numeric(10,2) NOT NULL,
  total_cost numeric(14,6) NOT NULL
);

CREATE OR REPLACE VIEW serving.speed_budget_status AS
SELECT team_id, budget_limit, total_cost AS stream_cost,
       pct_used, alert_status, last_event_ts
FROM serving.speed_cumulative
ORDER BY team_id;

GRANT SELECT ON serving.speed_cumulative TO grafana_ro;
GRANT SELECT ON serving.speed_alerts TO grafana_ro;
GRANT SELECT ON serving.speed_budget_status TO grafana_ro;