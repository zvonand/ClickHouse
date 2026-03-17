-- Tags: no-parallel-replicas
-- Test read_in_order_use_virtual_row optimization for DESC (reverse) order
-- with multiple parts. PR #99198 extends the virtual row path to InReverseOrder
-- reads at ReadFromMergeTree.cpp:745-777 using ranges.back().end as the mark.
-- Previously only ASC reads used virtual rows; this covers the reverse path.

DROP TABLE IF EXISTS t_04045_rvrow;

CREATE TABLE t_04045_rvrow (x UInt64, y UInt64)
ENGINE = MergeTree ORDER BY (x, y)
SETTINGS index_granularity = 8192;

SYSTEM STOP MERGES t_04045_rvrow;

-- Two non-overlapping parts so InReverseOrder merge uses virtual rows
INSERT INTO t_04045_rvrow SELECT number, number FROM numbers(8192 * 3);
INSERT INTO t_04045_rvrow SELECT number + 8192 * 3, number + 8192 * 3 FROM numbers(8192 * 3);

-- DESC with preliminary merge (two_level_merge_threshold = 0)
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 0,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192;

-- DESC without preliminary merge (threshold above part count)
SELECT x FROM t_04045_rvrow
ORDER BY x DESC
LIMIT 4
SETTINGS read_in_order_use_virtual_row = 1, read_in_order_two_level_merge_threshold = 5,
         max_threads = 1, optimize_read_in_order = 1, max_block_size = 8192;

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
