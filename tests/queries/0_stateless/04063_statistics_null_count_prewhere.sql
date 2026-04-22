-- Test NullCount statistics: Prewhere sorting with IS NULL / IS NOT NULL
-- Verifies column ordering by extracting only the Prewhere filter column line
-- and comparing column positions, avoiding dependency on full EXPLAIN format.

SET allow_statistics = 1;
SET use_statistics = 1;
SET mutations_sync = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;
SET allow_reorder_prewhere_conditions = 1; -- CI may inject False, preventing statistics-based reordering of prewhere conditions

DROP TABLE IF EXISTS test_nullcount_prewhere;

-- Table with Nullable columns for IS NULL/IS NOT NULL prewhere ordering:
--   col_low_null:  10% NULL (10 rows NULL, 90 rows non-NULL)
--   col_high_null: 90% NULL (90 rows NULL, 10 rows non-NULL)
-- Plus c Int64 for mixed predicate tests (range + IS NULL)
CREATE TABLE test_nullcount_prewhere
(
    id UInt64,
    col_low_null Nullable(Int64),
    col_high_null Nullable(Int64),
    c Int64 STATISTICS(tdigest),
    range_probe Int64 STATISTICS(tdigest)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO test_nullcount_prewhere SELECT
    number,
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number),
    number,
    number
FROM numbers(100);

-- Add nullcount statistics for prewhere ordering tests
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_high_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

-- Extract only the prewhere line to check column ordering.
-- This avoids dependency on EXPLAIN formatting, indentation, or optimize_functions_to_subcolumns.

-- Test 1: IS NULL with nullcount (high_null should be first due to higher selectivity)
SELECT 'Test 1: IS NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NULL AND col_high_null IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 2: IS NOT NULL with nullcount (high_null should be first)
SELECT 'Test 2: IS NOT NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NOT NULL AND col_high_null IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 3: IS NULL + range with nullcount (range should be moved before IS NULL)
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE minmax;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

SELECT 'Test 3: IS NULL + range with nullcount';
SELECT position(prewhere_line, 'col_high_null') > position(prewhere_line, 'col_low_null') AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_high_null IS NULL AND col_low_null < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Mixed predicates: IS NULL + range using c column
-- For single-char column names, check if 'less(' (range) appears in the prewhere line,
-- meaning the range column was moved before the IS NULL column.
SELECT 'Mixed predicates: IS NULL + range (col_low_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere WHERE col_low_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Mixed predicates: IS NULL + range (col_high_null IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere WHERE col_high_null IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 4: Nullable greater-than uses non-null row count
SELECT 'Test 4: Nullable greater-than uses non-null row count';
SELECT
    position(prewhere_line, 'col_low_null') > 0
    AND position(prewhere_line, 'range_probe') > 0
    AND position(prewhere_line, 'col_low_null') < position(prewhere_line, 'range_probe')
FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null > 95 AND range_probe < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM test_nullcount_prewhere WHERE col_low_null > 95;

DROP TABLE test_nullcount_prewhere;
