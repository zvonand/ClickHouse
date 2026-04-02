-- Test range selectivity bias correction using null_count statistics

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS test_nullcount_bias;

-- =============================================================================
-- Test 1: Nullable column range bias correction
-- =============================================================================
CREATE TABLE test_nullcount_bias (
    -- Column a: no NULLs, values 0-999, uniform distribution
    a Int64 STATISTICS(tdigest),
    -- Column b: 50% NULL, non-NULL values 0-499, same density as 'a' for < 500
    b Nullable(Int64) STATISTICS(tdigest)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_nullcount_bias
SELECT
    number % 1000,
    if(number % 2 = 0, NULL, (number % 1000) / 2)
FROM system.numbers LIMIT 10000;

-- Without null_count: b < 250 biased to 25% → b first (suboptimal)
SELECT 'Without null_count: b < 250 biased to 25% selectivity';
SELECT position(prewhere_line, 'b') < position(prewhere_line, 'a') AS b_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_bias WHERE a < 500 AND b < 250
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_bias ADD STATISTICS b TYPE nullcount;
ALTER TABLE test_nullcount_bias MATERIALIZE STATISTICS b;

-- With null_count: corrected to ~50% → column order decides
SELECT 'With null_count: b < 250 corrected to ~50% selectivity';
SELECT position(prewhere_line, 'b') < position(prewhere_line, 'a') AS b_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_bias WHERE a < 500 AND b < 250
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- =============================================================================
-- Test 2: High NULL ratio column
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_bias2;

CREATE TABLE test_nullcount_bias2 (
    a Int64 STATISTICS(tdigest),
    -- 90% NULL, non-NULL values 0-999
    b Nullable(Int64) STATISTICS(tdigest)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO test_nullcount_bias2
SELECT
    number % 1000,
    if(number % 10 != 0, NULL, number % 1000)
FROM system.numbers LIMIT 10000;

-- Without null_count: b < 500 biased to 5% → b first (suboptimal)
SELECT 'High NULL ratio without null_count: b < 500 biased to 5%';
SELECT position(prewhere_line, 'b') < position(prewhere_line, 'a') AS b_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_bias2 WHERE a < 500 AND b < 500
    ) WHERE explain LIKE '%Prewhere filter column%'
);

ALTER TABLE test_nullcount_bias2 ADD STATISTICS b TYPE nullcount;
ALTER TABLE test_nullcount_bias2 MATERIALIZE STATISTICS b;

-- With null_count: corrected to ~50% → column order decides
SELECT 'High NULL ratio with null_count: b < 500 corrected to ~50%';
SELECT position(prewhere_line, 'b') < position(prewhere_line, 'a') AS b_first FROM (
    SELECT extractAll(explain, 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions=1 SELECT count(*) FROM test_nullcount_bias2 WHERE a < 500 AND b < 500
    ) WHERE explain LIKE '%Prewhere filter column%'
);

-- Actual result counts for validation
SELECT 'Actual counts for bias test 1';
SELECT count() FROM test_nullcount_bias WHERE a < 500 AND b < 250;

SELECT 'Actual counts for bias test 2';
SELECT count() FROM test_nullcount_bias2 WHERE a < 500 AND b < 500;

DROP TABLE test_nullcount_bias;
DROP TABLE test_nullcount_bias2;

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
