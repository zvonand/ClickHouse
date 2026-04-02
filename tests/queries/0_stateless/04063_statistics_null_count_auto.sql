-- Test auto-enable of NullCount statistics for Nullable columns

SET allow_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS test_nullcount_auto;

-- =============================================================================
-- Test 1: Nullable column with tdigest - nullcount should be auto-added
-- =============================================================================
-- auto_statistics_types default includes nullcount in this version
CREATE TABLE test_nullcount_auto (
    a Nullable(Int64) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

INSERT INTO test_nullcount_auto SELECT if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT 'Nullable column with tdigest: nullcount should be auto-added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto' AND statistics != '[]'
ORDER BY column, name;

-- =============================================================================
-- Test 2: Nullable column without explicit statistics - nullcount auto-added
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_auto2;

CREATE TABLE test_nullcount_auto2 (
    a Nullable(Int64) STATISTICS(tdigest),
    b Nullable(Float64)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

INSERT INTO test_nullcount_auto2 SELECT
    if(number % 3 = 0, NULL, number),
    if(number % 5 = 0, NULL, toFloat64(number))
FROM numbers(1000);

SELECT 'Both Nullable columns should get nullcount auto-added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto2' AND statistics != '[]'
ORDER BY column, name;

-- =============================================================================
-- Test 3: Non-Nullable column should NOT get nullcount
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_auto3;

CREATE TABLE test_nullcount_auto3 (
    a Int64 STATISTICS(tdigest),
    b String
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

INSERT INTO test_nullcount_auto3 SELECT number, toString(number) FROM numbers(1000);

SELECT 'Non-Nullable columns should NOT get nullcount';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto3' AND statistics != '[]'
ORDER BY column, name;

-- =============================================================================
-- Test 4: Disable nullcount auto-enable via settings
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_auto4;

CREATE TABLE test_nullcount_auto4 (
    a Nullable(Int64) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq';

INSERT INTO test_nullcount_auto4 SELECT if(number % 2 = 0, NULL, number) FROM numbers(1000);

SELECT 'With nullcount disabled in auto_statistics_types';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto4' AND statistics != '[]'
ORDER BY column, name;

-- =============================================================================
-- Test 5: LowCardinality(Nullable) auto-enable
-- =============================================================================
DROP TABLE IF EXISTS test_nullcount_auto5;

SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE test_nullcount_auto5 (
    a LowCardinality(Nullable(Int64)) STATISTICS(tdigest)
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS auto_statistics_types = 'minmax, uniq, nullcount';

INSERT INTO test_nullcount_auto5 SELECT
    if(number % 2 = 0, NULL, number % 100)
FROM numbers(1000);

SELECT 'LowCardinality(Nullable) should also get nullcount auto-added';
SELECT column, statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_auto5' AND statistics != '[]'
ORDER BY column, name;

DROP TABLE test_nullcount_auto;
DROP TABLE test_nullcount_auto2;
DROP TABLE test_nullcount_auto3;
DROP TABLE test_nullcount_auto4;
DROP TABLE test_nullcount_auto5;
