-- One row per (scenario, backend, commit_sha) with full bench summary
SELECT
  scenario,
  backend,
  commit_sha,
  substring(commit_sha, 1, 8) AS sha8,
  max(ts) AS run_ended,
  round(maxIf(value, name = 'rps'), 1)                   AS rps,
  round(maxIf(value, name = 'read_rps'), 1)              AS read_rps,
  round(maxIf(value, name = 'write_rps'), 1)             AS write_rps,
  round(maxIf(value, name = 'read_p50_ms'), 2)           AS read_p50_ms,
  round(maxIf(value, name = 'read_p95_ms'), 2)           AS read_p95_ms,
  round(maxIf(value, name = 'read_p99_ms'), 2)           AS read_p99_ms,
  round(maxIf(value, name = 'read_p99_90_ms'), 2)        AS read_p99_90_ms,
  round(maxIf(value, name = 'read_p99_99_ms'), 2)        AS read_p99_99_ms,
  round(maxIf(value, name = 'write_p50_ms'), 2)          AS write_p50_ms,
  round(maxIf(value, name = 'write_p95_ms'), 2)          AS write_p95_ms,
  round(maxIf(value, name = 'write_p99_ms'), 2)          AS write_p99_ms,
  round(maxIf(value, name = 'write_p99_90_ms'), 2)       AS write_p99_90_ms,
  round(maxIf(value, name = 'write_p99_99_ms'), 2)       AS write_p99_99_ms,
  round(maxIf(value, name = 'errors'), 0)                AS errors,
  round(maxIf(value, name = 'error_rate') * 100, 4)      AS error_pct,
  round(maxIf(value, name = 'ops'), 0)                   AS ops,
  round(maxIf(value, name = 'read_bytes_per_second'), 0) AS read_bps,
  round(maxIf(value, name = 'write_bytes_per_second'), 0) AS write_bps,
  round(maxIf(value, name = 'total_znode_delta'), 0)     AS znode_delta,
  round(maxIf(value, name = 'total_dirs_bytes_delta'), 0) AS dirs_bytes_delta,
  round(maxIf(value, name = 'bench_duration'), 0)        AS bench_duration_s
FROM keeper_stress_tests.keeper_metrics_ts
WHERE source = 'bench'
  AND stage = 'summary'
  AND branch = 'master'
  AND ts >= '{{TS_FILTER}}'
GROUP BY scenario, backend, commit_sha
ORDER BY scenario, backend, run_ended
FORMAT TSVWithNames
