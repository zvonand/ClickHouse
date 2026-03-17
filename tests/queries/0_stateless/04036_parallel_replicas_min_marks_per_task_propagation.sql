-- Tags: no-fasttest, no-random-settings
-- no-fasttest: requires s3 storage
-- no-random-settings: a lot of settings influence task sizes, so it is simpler to disable randomization completely

-- Test that `min_marks_per_task` is propagated from the initiator (which does PK analysis)
-- to follower replicas (which skip it when `parallel_replicas_index_analysis_only_on_coordinator = 1`).
-- Without the fix, replicas that skip PK analysis see all parts/marks, inflating `sum_marks`
-- and causing the columns size heuristic in `calculateMinMarksPerTask` to choose much larger task sizes
-- than the initiator. With the fix, the coordinator propagates per-part `min_marks_per_task` from the
-- initiator's announcement to follower replicas via the read response protocol.

CREATE TABLE t_pr_task_prop (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS storage_policy = 's3_cache', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_pr_task_prop;

-- Insert enough data across multiple wide parts to trigger the columns size heuristic.
-- Wide parts are needed because the heuristic uses per-column compressed sizes.
INSERT INTO t_pr_task_prop SELECT number, randomString(100) FROM numbers_mt(1_000_000);
INSERT INTO t_pr_task_prop SELECT number + 1_000_000, randomString(100) FROM numbers_mt(1_000_000);
INSERT INTO t_pr_task_prop SELECT number + 2_000_000, randomString(100) FROM numbers_mt(1_000_000);

-- Verify all parts are wide.
SELECT throwIf(countIf(part_type != 'Wide') > 0, 'Expected all wide parts')
FROM system.parts
WHERE (database = currentDatabase()) AND (table = 't_pr_task_prop') AND active
FORMAT Null;

-- Use few threads and minimal base task size so the columns size heuristic dominates.
SET max_threads = 4, merge_tree_min_read_task_size = 1;

-- Query with a selective PK filter: only ~10% of data matches.
-- The initiator does PK analysis and sees a small sum_marks.
-- Replicas skip PK analysis (parallel_replicas_index_analysis_only_on_coordinator = 1)
-- and see all marks. Without propagation, replicas would compute large min_marks_per_task
-- from the inflated sum_marks, leading to very few but very large coordinator requests.
-- With propagation, replicas use the initiator's values and make more, smaller requests.
SELECT *
FROM t_pr_task_prop
WHERE a < 300_000
FORMAT Null
SETTINGS
    enable_parallel_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_index_analysis_only_on_coordinator = 1,
    log_comment = 'pr_task_prop_04036';

SYSTEM FLUSH LOGS query_log;

-- With correct propagation, the initiator's small min_marks_per_task (based on ~10% of marks)
-- is used by all replicas. This leads to reasonably sized requests.
-- Without propagation, replicas would use bytes-based estimate (much larger) as min_marks_per_task,
-- requesting enormous batches and making very few requests (often just 3-5 total).
-- With propagation we should see noticeably more requests.
SELECT throwIf(ProfileEvents['ParallelReplicasNumRequests'] < 5, 'Too few requests - min_marks_per_task may not be propagated to replicas')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'pr_task_prop_04036'
    AND type = 'QueryFinish'
SETTINGS enable_parallel_replicas = 0
FORMAT Null;

DROP TABLE t_pr_task_prop;
