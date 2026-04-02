-- Test NullCount statistics: Prewhere sorting with IS NULL / IS NOT NULL
-- Each test has a "before nullcount" (suboptimal) and "after nullcount" (optimal) pair.
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

-- Table with two Nullable columns:
--   col_low_null:  10% NULL (10 rows NULL, 90 rows non-NULL)
--   col_high_null: 90% NULL (90 rows NULL, 10 rows non-NULL)
CREATE TABLE test_nullcount_prewhere
(
    id UInt64,
    col_low_null Nullable(Int64),
    col_high_null Nullable(Int64)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = '';

INSERT INTO test_nullcount_prewhere SELECT
    number,
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number)
FROM numbers(100);

-- Extract only the prewhere line to check column ordering.
-- This avoids dependency on EXPLAIN formatting, indentation, or optimize_functions_to_subcolumns.

-- Without nullcount: both IS NULL look equally selective → col_low_null first.
SELECT 'Test 1a: IS NULL without nullcount (suboptimal: low_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NULL AND col_high_null IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_high_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

-- With nullcount: high_null (90% NULL) is more selective → first.
SELECT 'Test 1b: IS NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NULL AND col_high_null IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_prewhere DROP STATISTICS col_low_null, col_high_null;

SELECT 'Test 2a: IS NOT NULL without nullcount (suboptimal)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NOT NULL AND col_high_null IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_high_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

SELECT 'Test 2b: IS NOT NULL with nullcount (optimal: high_null first)';
SELECT position(prewhere_line, 'col_high_null') < position(prewhere_line, 'col_low_null') AS high_null_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_low_null IS NOT NULL AND col_high_null IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_prewhere DROP STATISTICS col_low_null, col_high_null;
ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_low_null TYPE minmax;

SELECT 'Test 3a: IS NULL + range without nullcount on high_null';
SELECT position(prewhere_line, 'col_high_null') > position(prewhere_line, 'col_low_null') AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_high_null IS NULL AND col_low_null < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_prewhere ADD STATISTICS col_high_null TYPE nullcount;
ALTER TABLE test_nullcount_prewhere MATERIALIZE STATISTICS col_low_null, col_high_null;

SELECT 'Test 3b: IS NULL + range with nullcount';
SELECT position(prewhere_line, 'col_high_null') > position(prewhere_line, 'col_low_null') AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere
        WHERE col_high_null IS NULL AND col_low_null < 5
    ) WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE IF EXISTS test_nullcount_prewhere2;

CREATE TABLE test_nullcount_prewhere2 (
    a Nullable(Int64) STATISTICS(tdigest, nullcount),
    b Nullable(Int64) STATISTICS(tdigest, nullcount),
    c Int64 STATISTICS(tdigest)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_nullcount_prewhere2
SELECT
    if(number % 10 = 0, NULL, number),   -- a: 10% NULL
    if(number % 10 != 0, NULL, number),  -- b: 90% NULL
    number                                -- c: no NULL
FROM system.numbers LIMIT 10000;

ALTER TABLE test_nullcount_prewhere2 MATERIALIZE STATISTICS a, b, c;

-- For single-char column names, check if 'less(' (range) appears in the prewhere line,
-- meaning the range column was moved before the IS NULL column.
SELECT 'Mixed predicates: IS NULL + range (a IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere2 WHERE a IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

SELECT 'Mixed predicates: IS NULL + range (b IS NULL AND c < 100)';
SELECT position(prewhere_line, 'less(') > 0 AS range_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_prewhere2 WHERE b IS NULL AND c < 100
    ) WHERE explain LIKE '%Prewhere filter column%'
);

DROP TABLE IF EXISTS test_nullcount_prewhere;
DROP TABLE test_nullcount_prewhere2;
