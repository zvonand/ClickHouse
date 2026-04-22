-- Test basic DDL and materialization of NullCount statistics

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET mutations_sync = 1;
SET materialize_statistics_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_nullcount_ddl;

-- =============================================================================
-- Test 1: CREATE TABLE with explicit nullcount statistics
-- =============================================================================
CREATE TABLE test_nullcount_ddl (
    a Nullable(Int64) STATISTICS(nullcount),
    b Nullable(String) STATISTICS(nullcount),
    c Nullable(Float64),
    d Int64,
    e LowCardinality(Nullable(Int64)) STATISTICS(nullcount)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = '';

-- Nullable columns 'a', 'b' and 'e' should get nullcount, 'c' and 'd' should not
INSERT INTO test_nullcount_ddl SELECT
    if(number % 3 = 0, NULL, number),
    if(number % 4 = 0, NULL, toString(number)),
    if(number % 5 = 0, NULL, toFloat64(number)),
    number,
    if(number % 2 = 0, NULL, number % 100)
FROM numbers(1000);

-- Verify statistics exist in system.parts_columns (only active parts, a, b and e)
SELECT 'After insert: statistics for Nullable columns';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column IN ('a', 'b', 'e')
ORDER BY column;

-- =============================================================================
-- Test 2: ALTER TABLE ADD/DROP STATISTICS
-- =============================================================================
ALTER TABLE test_nullcount_ddl DROP STATISTICS a;

SELECT 'After DROP STATISTICS a';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column IN ('a', 'b', 'e')
ORDER BY column;

ALTER TABLE test_nullcount_ddl ADD STATISTICS a TYPE nullcount;
ALTER TABLE test_nullcount_ddl MATERIALIZE STATISTICS a;

SELECT 'After ADD + MATERIALIZE STATISTICS a';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column IN ('a', 'b', 'e')
ORDER BY column;

-- =============================================================================
-- Test 3: OPTIMIZE TABLE preserves statistics
-- =============================================================================
OPTIMIZE TABLE test_nullcount_ddl FINAL;

SELECT 'After OPTIMIZE TABLE FINAL';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column IN ('a', 'b', 'e')
ORDER BY column;

-- Verify null_count value after merge: original had 334 NULLs out of 1000 rows for column 'a'
-- After FINAL merge, should have exactly one part with null_count = 334
SELECT 'NullCount value after OPTIMIZE FINAL';
SELECT name, column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column = 'a'
ORDER BY name;

-- =============================================================================
-- Test 4: Multiple inserts merge correctly
-- =============================================================================
INSERT INTO test_nullcount_ddl SELECT
    NULL,
    NULL,
    NULL,
    number,
    NULL
FROM numbers(500);

SELECT 'After second insert (all NULL for a, b, c, e)';
SELECT DISTINCT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_ddl' AND active AND column IN ('a', 'b', 'e')
ORDER BY column, name;

-- =============================================================================
-- Test 5: Non-Nullable column should reject nullcount statistics
-- =============================================================================
ALTER TABLE test_nullcount_ddl ADD STATISTICS d TYPE nullcount; -- { serverError ILLEGAL_STATISTICS }

DROP TABLE test_nullcount_ddl;
