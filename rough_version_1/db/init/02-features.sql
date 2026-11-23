CREATE TABLE IF NOT EXISTS serving.dead_letter (
  received_at timestamptz NOT NULL DEFAULT now(),
  layer       text        NOT NULL,
  reason      text        NOT NULL,
  event       jsonb
);

CREATE TABLE IF NOT EXISTS serving.team_budget (
  team_id      text          PRIMARY KEY,
  budget_limit numeric(14,2) NOT NULL
);

INSERT INTO serving.team_budget (team_id, budget_limit) VALUES
  ('team-a', 500.00),
  ('team-b', 300.00),
  ('team-c', 200.00)
ON CONFLICT (team_id) DO UPDATE SET budget_limit = EXCLUDED.budget_limit;

CREATE OR REPLACE VIEW serving.budget_status AS
SELECT
  b.team_id,
  b.budget_limit,
  COALESCE(SUM(mv.cost), 0)::numeric(14,2)                                AS mtd_cost,
  ROUND((COALESCE(SUM(mv.cost), 0) / b.budget_limit * 100)::numeric, 2)   AS pct_used,
  CASE
    WHEN COALESCE(SUM(mv.cost), 0) >= b.budget_limit       THEN 'critical'
    WHEN COALESCE(SUM(mv.cost), 0) >= 0.8 * b.budget_limit THEN 'warning'
    ELSE 'ok'
  END                                                                     AS status
FROM serving.team_budget b
LEFT JOIN serving.merged_view mv
       ON mv.team_id = b.team_id
      AND mv.window_start >= date_trunc('month', CURRENT_DATE)
GROUP BY b.team_id, b.budget_limit
ORDER BY b.team_id;