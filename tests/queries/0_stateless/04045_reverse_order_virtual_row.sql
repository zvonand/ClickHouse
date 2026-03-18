-- Tags: no-parallel-replicas
-- ^ because we are using query_log
-- Test read_in_order_use_virtual_row optimization for DESC (reverse) order
-- with multiple parts. PR #99198 extends the virtual row path to InReverseOrder
-- reads at ReadFromMergeTree.cpp:745-777 using ranges.back().end as the mark.
-- Previously only ASC reads used virtual rows; this covers the reverse path.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_04045_rvrow;

-- index_granularity_bytes = 10485760: disables adaptive granularity so
-- index_granularity = 8192 is the effective granule size regardless of the
-- random MergeTree settings the flaky check injects (e.g. index_granularity_bytes = 1588).
-- add_minmax_index_for_numeric_columns = 0: prevents automatic minmax indexes
-- from changing read_rows (same guard used in 03031).
CREATE TABLE t_04045_rvrow (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY (x, y)
SETTINGS index_granularity = 8192,
         index_granularity_bytes = 10485760,
         add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_04045_rvrow;

-- Two non-overlapping parts so InReverseOrder merge uses virtual rows
INSERT INTO t_04045_rvrow SELECT number, number FROM numbers(8192 * 3);
INSERT INTO t_04045_rvrow SELECT number + 8192 * 3, number + 8192 * 3 FROM numbers(8192 * 3);

-- DESC with preliminary merge (two_level_merge_threshold = 0)
-- Virtual rows are synthetic sentinels not counted in read_rows;
-- two granules of actual data are read: 2 * 8192 = 16384 rows.
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_prelim_merge';

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_prelim_merge'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- DESC without preliminary merge (threshold above part count)
-- Virtual rows are synthetic sentinels not counted in read_rows; 2 * 8192 = 16384.
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 5,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_no_prelim_merge';

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_no_prelim_merge'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- DESC with filter
-- PK index prunes part 2 entirely (all rows >= 24576 fail x < 16384);
-- LIMIT 4 satisfied from part 1's last qualifying granule: 1 * 8192 = 8192 rows.
SELECT x FROM t_04045_rvrow
WHERE x < 8192 * 2
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_filter';

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_filter'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- DESC multi-column key
-- ORDER BY (x DESC, y DESC) matches the table key reversed; virtual rows work
-- identically to the single-column case: 2 * 8192 = 16384 rows.
SELECT x, y FROM t_04045_rvrow
ORDER BY x DESC, y DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192,
         log_comment = 'desc_multicol';

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND current_database = currentDatabase()
    AND log_comment = 'desc_multicol'
    AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t_04045_rvrow;
