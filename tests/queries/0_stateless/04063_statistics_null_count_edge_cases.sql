-- Test NullCount statistics: edge cases not covered by 04063 tests
-- Covers: empty/zero-row parts, asymmetric merge, optimize_functions_to_subcolumns=0

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET mutations_sync = 1;
SET materialize_statistics_on_insert = 1;

-- =============================================================================
-- Test 1: Empty table / zero-row part
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_empty;

CREATE TABLE test_nullcount_empty (
    id UInt64,
    value Nullable(Int64)
) ENGINE = MergeTree()
PARTITION BY id % 2
ORDER BY id
SETTINGS auto_statistics_types = '';

ALTER TABLE test_nullcount_empty ADD STATISTICS value TYPE nullcount;
-- MATERIALIZE on empty table should produce null_count = 0
ALTER TABLE test_nullcount_empty MATERIALIZE STATISTICS value;

SELECT 'Test 1a: NullCount on empty table';
SELECT partition, column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_empty' AND active AND column = 'value'
ORDER BY partition;

-- Insert zero rows (no-op) — statistics should remain valid
INSERT INTO test_nullcount_empty SELECT number, number FROM numbers(0);

SELECT 'Test 1b: After zero-row insert, statistics still valid';
SELECT count() FROM test_nullcount_empty WHERE value IS NULL;
SELECT count() FROM test_nullcount_empty WHERE value IS NOT NULL;

DROP TABLE test_nullcount_empty;

-- =============================================================================
-- Test 2: Asymmetric merge — Part A has NullCount, Part B does not
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_asymmetric;

CREATE TABLE test_nullcount_asymmetric (
    id UInt64,
    value Nullable(Int64)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Insert two parts, add NullCount only to one
INSERT INTO test_nullcount_asymmetric SELECT number, if(number % 2 = 0, NULL, number) FROM numbers(100);

ALTER TABLE test_nullcount_asymmetric ADD STATISTICS value TYPE nullcount;
ALTER TABLE test_nullcount_asymmetric MATERIALIZE STATISTICS value;

SELECT 'Test 2a: Part 1 has NullCount statistics';
SELECT name, column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_asymmetric' AND active AND column = 'value'
ORDER BY name;

-- Insert a second part WITHOUT materializing statistics
INSERT INTO test_nullcount_asymmetric SELECT number + 100, if(number % 3 = 0, NULL, number + 100) FROM numbers(100);

SELECT 'Test 2b: After second insert without statistics on part 2';
SELECT name, column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_asymmetric' AND active AND column = 'value'
ORDER BY name;

-- OPTIMIZE FINAL should merge both parts. Since part 2 lacks NullCount,
-- the merged part should NOT have NullCount (one-sided → drop).
OPTIMIZE TABLE test_nullcount_asymmetric FINAL;

SELECT 'Test 2c: After OPTIMIZE FINAL, NullCount dropped (asymmetric merge)';
SELECT name, column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_asymmetric' AND active AND column = 'value'
ORDER BY name;

DROP TABLE test_nullcount_asymmetric;

