-- Test range selectivity bias correction using null_count statistics

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;

-- =============================================================================
-- Test 3: IS NULL / IS NOT NULL fallback without nullcount (B1/B2 verification)
-- =============================================================================
DROP TABLE IF EXISTS test_fallback_no_nullcount;

CREATE TABLE test_fallback_no_nullcount (
    a Int64 STATISTICS(tdigest),
    b Nullable(Int64) STATISTICS(tdigest)  -- Only tdigest, NO nullcount
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_fallback_no_nullcount
SELECT number, if(number % 2 = 0, NULL, number)
FROM numbers(10000);

-- Test 3a: IS NOT NULL without nullcount — both conditions present in prewhere
-- With B1 fixed (fallback = 0.99), IS NOT NULL should not dominate prewhere ordering.
-- Both 'less' (range on a) and 'not' (IS NOT NULL on b) must appear in the merged prewhere.
SELECT 'Test 3a: IS NOT NULL without nullcount (both conditions in prewhere)';
SELECT position(prewhere_line, 'less') > 0 AS has_range, position(prewhere_line, 'not') > 0 AS has_not FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_nullcount
        WHERE a < 100 AND b IS NOT NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 3b: IS NULL without nullcount — both conditions present in prewhere
-- With B2 fixed (fallback = 0.01), IS NULL should participate in prewhere normally.
SELECT 'Test 3b: IS NULL without nullcount (both conditions in prewhere)';
SELECT position(prewhere_line, 'greater') > 0 AS has_range, position(prewhere_line, '.null') > 0 AS has_null_check FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_fallback_no_nullcount
        WHERE a > 9900 AND b IS NULL
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Test 3c: Verify actual counts
SELECT 'Test 3c: Actual counts for validation';
SELECT count() FROM test_fallback_no_nullcount WHERE b IS NULL;
SELECT count() FROM test_fallback_no_nullcount WHERE b IS NOT NULL;

DROP TABLE test_fallback_no_nullcount;

-- =============================================================================
-- Test 4: Fallback selectivity uses non-null row count (nullcount-only columns)
-- =============================================================================
DROP TABLE IF EXISTS test_fallback_selectivity;

CREATE TABLE test_fallback_selectivity (
    a Nullable(Int64),
    b Nullable(Int64)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

-- a: 10% NULL (9000 non-null), b: 90% NULL (1000 non-null)
INSERT INTO test_fallback_selectivity
SELECT
    if(number % 10 = 0, NULL, number),
    if(number % 10 != 0, NULL, number)
FROM numbers(10000);

-- Only nullcount statistics, no minmax/tdigest, forcing fallback path
ALTER TABLE test_fallback_selectivity ADD STATISTICS a TYPE nullcount;
ALTER TABLE test_fallback_selectivity ADD STATISTICS b TYPE nullcount;
ALTER TABLE test_fallback_selectivity MATERIALIZE STATISTICS a, b SETTINGS mutations_sync = 1;

-- Verify that b (90% NULL, lower estimated rows) appears before a (10% NULL) in prewhere ordering.
-- This proves fallback selectivity uses getNonNullRowCount() instead of getNumRows().
SELECT 'Test 4: Fallback selectivity uses non-null row count';
SELECT count() FROM (
    EXPLAIN actions=1 SELECT count() FROM test_fallback_selectivity WHERE a = 1 AND b = 1
) WHERE explain LIKE '%Prewhere filter column%b%a%';

DROP TABLE test_fallback_selectivity;
