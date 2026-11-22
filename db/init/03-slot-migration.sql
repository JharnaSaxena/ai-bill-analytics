$c = @'
-- Migration: hourly → 4-hour slots, add dedup_stats table

-- Rename batch_agg column
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema='serving' AND table_name='batch_agg'
             AND column_name='hour_start') THEN
    ALTER TABLE serving.batch_agg RENAME COLUMN hour_start TO slot_start;
  END IF;
END $$;

-- Dedup audit table
CREATE TABLE IF NOT EXISTS serving.dedup_stats (
  run_at              timestamptz NOT NULL DEFAULT now(),
  layer               text        NOT NULL,
  raw_count           bigint      NOT NULL,
  after_dedup_count   bigint      NOT NULL,
  dropped_count       bigint      NOT NULL,
  window_start        timestamptz,
  window_end          timestamptz
);
GRANT SELECT ON serving.dedup_stats TO grafana_ro;

-- Recreate merged_view (slot_start + speed)
DROP VIEW IF EXISTS serving.merged_view;
CREATE VIEW serving.merged_view AS
SELECT b.team_id, b.model, b.slot_start AS window_start,
       b.tokens_in, b.tokens_out, b.cost, 'batch'::text AS layer
FROM serving.batch_agg b
JOIN serving.batch_meta m
  ON m.team_id = b.team_id AND m.model = b.model
WHERE b.slot_start < m.cutoff_ts
UNION ALL
SELECT s.team_id, s.model, s.minute_start AS window_start,
       s.tokens_in, s.tokens_out, s.cost, 'speed'::text AS layer
FROM serving.speed_agg s
LEFT JOIN serving.batch_meta m
  ON m.team_id = s.team_id AND m.model = s.model
WHERE s.minute_start >= COALESCE(m.cutoff_ts, 'epoch'::timestamptz);

GRANT SELECT ON serving.merged_view TO grafana_ro;
'@
$c = $c -replace "`r`n", "`n"
[IO.File]::WriteAllText((Join-Path $PWD 'db\init\03-slot-migration.sql'), $c)