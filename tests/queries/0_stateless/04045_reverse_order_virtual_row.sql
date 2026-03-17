-- Tags: no-parallel-replicas
-- ^ because we are using query_log
-- Test read_in_order_use_virtual_row optimization for DESC (reverse) order
-- with multiple parts. PR #99198 extends the virtual row path to InReverseOrder
-- reads at ReadFromMergeTree.cpp:745-777 using ranges.back().end as the mark.
-- Previously only ASC reads used virtual rows; this covers the reverse path.

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_04045_rvrow;

CREATE TABLE t_04045_rvrow (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY (x, y)
SETTINGS index_granularity = 8192;

SYSTEM STOP MERGES t_04045_rvrow;

-- Two non-overlapping parts so InReverseOrder merge uses virtual rows
INSERT INTO t_04045_rvrow SELECT number, number FROM numbers(8192 * 3);
INSERT INTO t_04045_rvrow SELECT number + 8192 * 3, number + 8192 * 3 FROM numbers(8192 * 3);

-- DESC with preliminary merge (two_level_merge_threshold = 0)
-- Expecting 2 virtual rows + one block (8192) for result + one extra block (8192)
-- for next consumption in merge transform = 16386 rows read.
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
-- Expecting 2 virtual rows + one block (8192) for result + one extra block (8192) = 16386.
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
SELECT x FROM t_04045_rvrow
WHERE x < 8192 * 2
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192;

-- DESC multi-column key
SELECT x, y FROM t_04045_rvrow
ORDER BY x DESC, y DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192;

DROP TABLE t_04045_rvrow;
