-- Per-PR-branch latest run on the 3 PR-level scenarios + master baseline at same date
SELECT
  branch,
  scenario,
  substring(commit_sha,1,8) AS sha8,
  max(ts)                                          AS run_ended,
  round(maxIf(value, name='rps'), 0)               AS rps,
  round(maxIf(value, name='read_p99_ms'), 1)       AS read_p99,
  round(maxIf(value, name='write_p99_ms'), 1)      AS write_p99,
  round(maxIf(value, name='error_rate')*100, 4)    AS error_pct,
  round(maxIf(value, name='ops'), 0)               AS ops
FROM keeper_stress_tests.keeper_metrics_ts
WHERE source='bench' AND stage='summary'
  AND branch != 'master'
  AND ts >= '{{TS_FILTER}}'
GROUP BY branch, scenario, commit_sha
ORDER BY branch, scenario, run_ended DESC
FORMAT TSVWithNames
