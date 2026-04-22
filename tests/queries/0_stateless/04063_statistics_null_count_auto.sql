-- Test auto-enable of NullCount statistics for Nullable columns
-- All cases use a single table with multiple columns to verify different scenarios

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_nullcount_auto;

-- Single table with multiple columns covering all test cases:
--   a: Nullable(Int64)  STATISTICS(tdigest)        - should auto-add NullCount
--   b: Nullable(Float64)                          - should auto-add NullCount
--   c: Int64 (non-Nullable)                       - should NOT get NullCount
--   d: LowCardinality(Nullable(Int64))             - should auto-add NullCount (with allow_suspicious_low_cardinality_types)

CREATE TABLE test_nullcount_auto (
    a Nullable(Int64) STATISTICS(tdigest),
    b Nullable(Float64),
    c Int64,
    d LowCardinality(Nullable(Int64))
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

-- Insert data for all columns
-- a: ~50% NULL (every other row)
-- b: ~20% NULL (every 5th row)
-- c: no NULLs
-- d: ~50% NULL (every other row)
INSERT INTO test_nullcount_auto SELECT
    if(number % 2 = 0, NULL, number),
    if(number % 5 = 0, NULL, toFloat64(number)),
    number,
    if(number % 2 = 0, NULL, number % 100)
FROM numbers(1000);

SELECT 'Test 1: Nullable columns should get nullcount auto-added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto' AND statistics != '[]'
ORDER BY column, name;

-- Test 2: Verify non-Nullable column c does NOT get nullcount
SELECT 'Test 2: Non-Nullable column should NOT get nullcount';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto' AND column = 'c' AND statistics != '[]';

DROP TABLE test_nullcount_auto;

-- =============================================================================
-- Test 3: Disable nullcount auto-enable via settings
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_auto4;

CREATE TABLE test_nullcount_auto4 (
    a Nullable(Int64) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq';  -- nullcount disabled

INSERT INTO test_nullcount_auto4 SELECT if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT 'Test 3: With nullcount disabled in auto_statistics_types';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto4' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_nullcount_auto4;
