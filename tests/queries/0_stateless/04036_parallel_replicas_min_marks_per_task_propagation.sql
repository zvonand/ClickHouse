-- Tags: no-fasttest, no-random-settings
-- no-fasttest: requires s3 storage
-- no-random-settings: a lot of settings influence task sizes, so it is simpler to disable randomization completely

-- Test that per-part `min_marks_per_task` is propagated from the initiator (which does PK analysis)
-- to follower replicas (which skip it when `parallel_replicas_index_analysis_only_on_coordinator = 1`).
-- Without the fix, replicas that skip PK analysis see all parts/marks,
-- and causing the columns size heuristic in `calculateMinMarksPerTask` to choose much larger task sizes.

CREATE TABLE t_pr_task_prop (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS storage_policy = 's3_cache', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_pr_task_prop;

-- Insert data across multiple wide parts. Wide parts are needed because the heuristic uses per-column compressed sizes.
INSERT INTO t_pr_task_prop SELECT number, randomString(10) FROM numbers_mt(100_000);
INSERT INTO t_pr_task_prop SELECT number + 100_000, randomString(10) FROM numbers_mt(100_000);
INSERT INTO t_pr_task_prop SELECT number + 200_000, randomString(10) FROM numbers_mt(100_000);

-- Verify all parts are wide.
SELECT throwIf(countIf(part_type != 'Wide') > 0, 'Expected all wide parts')
FROM system.parts
WHERE (database = currentDatabase()) AND (table = 't_pr_task_prop') AND active
FORMAT Null;

SET max_threads = 4, merge_tree_min_read_task_size = 1;

-- Query with a selective PK filter so the initiator's PK analysis eliminates most marks.
-- Replicas skip PK analysis and see all marks, but should receive the initiator's
-- per-part `min_marks_per_task` via the coordinator response.
SELECT count()
FROM t_pr_task_prop
WHERE a < 10_000
FORMAT Null
SETTINGS
    enable_parallel_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_index_analysis_only_on_coordinator = 1,
    receive_timeout = 10,
    max_execution_time = 15,
    log_comment = 'pr_task_prop_04036';

SYSTEM FLUSH LOGS query_log;

-- With propagation, the initiator's small `min_marks_per_task` (based on PK-filtered marks)
-- is used by all replicas. Without propagation, replicas would use the bytes-based estimate
-- which is much larger, leading to fewer requests.
SELECT throwIf(ProfileEvents['ParallelReplicasNumRequests'] < 3, 'Too few requests — min_marks_per_task may not be propagated to replicas')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'pr_task_prop_04036'
    AND type = 'QueryFinish'
SETTINGS enable_parallel_replicas = 0
FORMAT Null;

DROP TABLE t_pr_task_prop;
