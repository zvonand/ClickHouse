-- Tags: no-random-merge-tree-settings

-- Regression test for incorrect column comparison in FinishAggregatingInOrderAlgorithm.
--
-- When GROUP BY column order differs from the table's sorting key order
-- (e.g., GROUP BY a, b on a table ORDER BY b), the sort description passed to
-- FinishAggregatingInOrderAlgorithm has columns in sort-key order [b, a], while
-- the output header has them in GROUP BY order [a, b].
--
-- The bug was in State::State() which built sorting_columns by iterating through
-- the sort description (producing [b_col, a_col] indexed 0,1), but less() used
-- column_number (header position: b→1, a→0) to index into it, accessing the
-- wrong columns. This corrupted merge group boundaries, causing duplicate rows.

SET optimize_aggregation_in_order = 1;

DROP TABLE IF EXISTS t_inorder_merge_order;

-- Table sorted by b. GROUP BY (a, b) puts non-sort column 'a' first in the header.
CREATE TABLE t_inorder_merge_order (a String, b UInt32, c UInt32)
ENGINE = MergeTree ORDER BY b
SETTINGS index_granularity = 128;

-- Create multiple parts to ensure parallel in-order aggregation streams
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);
INSERT INTO t_inorder_merge_order SELECT 'key_' || toString(rand() % 100), number % 200, number FROM numbers(500000);

-- The query must produce exactly 20000 distinct (a, b) groups.
-- Before the fix, with multiple streams and small block sizes, wrong column
-- comparison in the merge step produced duplicate rows (up to 37000+).
SELECT count()
FROM (
    SELECT a, b, count() as cnt
    FROM t_inorder_merge_order
    GROUP BY a, b
)
SETTINGS max_threads = 4, max_block_size = 128, aggregation_in_order_max_block_bytes = 1000;

-- Also test with the duplicate GROUP BY key variant (original failing test pattern)
SELECT count()
FROM (
    SELECT a, b, count() as cnt
    FROM t_inorder_merge_order
    GROUP BY a, b, b
)
SETTINGS max_threads = 4, max_block_size = 128, aggregation_in_order_max_block_bytes = 1000;

DROP TABLE t_inorder_merge_order;
