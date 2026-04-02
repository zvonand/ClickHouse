-- Test NullCount statistics: Part Pruning with IS NULL / IS NOT NULL
-- Demonstrates before/after benefit: with NullCount, IS NULL and IS NOT NULL
-- can prune parts that have zero or all NULLs respectively.

DROP TABLE IF EXISTS test_nullcount_pruning;

CREATE TABLE test_nullcount_pruning (
    id UInt64,
    value Nullable(Int64)
) ENGINE = MergeTree()
PARTITION BY id % 5
ORDER BY id
SETTINGS auto_statistics_types = '';

SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

-- Part 0 (partition 0): all NULL
INSERT INTO test_nullcount_pruning SELECT 0, NULL;
-- Part 1 (partition 1): all NULL
INSERT INTO test_nullcount_pruning SELECT 1, NULL;
-- Part 2 (partition 2): no NULL
INSERT INTO test_nullcount_pruning SELECT 2, 100;
-- Part 3 (partition 3): no NULL
INSERT INTO test_nullcount_pruning SELECT 3, 200;
-- Part 4 (partition 4): mixed NULL
INSERT INTO test_nullcount_pruning SELECT 4 + number, if(number % 2 = 0, NULL, number + 300) FROM numbers(100);

-- =============================================================================
-- Test 1: IS NULL without NullCount - no pruning possible (5/5 parts read)
-- =============================================================================
SELECT 'Test 1a: IS NULL without NullCount (no pruning)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL;

SELECT 'Test 1b: IS NOT NULL without NullCount (no pruning)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL;

-- =============================================================================
-- Test 2: Add NullCount statistics
-- =============================================================================
ALTER TABLE test_nullcount_pruning ADD STATISTICS value TYPE nullcount;
ALTER TABLE test_nullcount_pruning MATERIALIZE STATISTICS value SETTINGS mutations_sync = 1;

SELECT 'After adding NullCount statistics:';
SELECT partition, column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_nullcount_pruning' AND active AND column = 'value'
ORDER BY partition;

-- =============================================================================
-- Test 3: IS NULL with NullCount - prunes Parts 2,3 (no NULLs)
-- =============================================================================
-- Parts 0,1: null_count == rows (all NULL) → kept
-- Parts 2,3: null_count == 0 → pruned
-- Part 4: mixed → kept
SELECT 'Test 3a: IS NULL with NullCount (Parts 2,3 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL;

-- =============================================================================
-- Test 4: IS NOT NULL with NullCount - prunes Parts 0,1 (all NULL)
-- =============================================================================
-- Parts 0,1: null_count == rows (all NULL) → pruned
-- Parts 2,3: null_count == 0 → kept
-- Part 4: mixed → kept
SELECT 'Test 4a: IS NOT NULL with NullCount (Parts 0,1 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL;

-- =============================================================================
-- Test 5: IS NULL combined with Range - mixed part filtered by range
-- =============================================================================
SELECT 'Test 5: IS NULL AND value >= 150 (Parts 2,3 pruned + range filter on Part 4)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NULL AND value >= 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NULL AND value >= 150;

-- =============================================================================
-- Test 6: IS NOT NULL combined with Range
-- =============================================================================
SELECT 'Test 6: IS NOT NULL AND value >= 150 (Parts 0,1 pruned + range filter)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL AND value >= 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value IS NOT NULL AND value >= 150;

-- =============================================================================
-- Test 7: Range condition on Nullable still works
-- =============================================================================
SELECT 'Test 7: Range condition (value >= 150) with NullCount';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value >= 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value >= 150;

-- =============================================================================
-- Test 8: Range on all-NULL part should be pruned by NullCount
-- =============================================================================
SELECT 'Test 8: Range (value >= 0 AND value <= 5000) prunes all-NULL parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_nullcount_pruning WHERE value >= 0 AND value <= 5000)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_nullcount_pruning WHERE value >= 0 AND value <= 5000;

DROP TABLE test_nullcount_pruning;

-- =============================================================================
-- Test 9: MinMax + NullCount combined — NullCount==0 enables precise [min,max]
-- =============================================================================
-- Case 4: When NullCount==0, the range should be precise [min,max] (not [min,+Inf]),
-- allowing tighter range pruning than the old behavior.
DROP TABLE IF EXISTS test_minmax_nullcount;
CREATE TABLE test_minmax_nullcount (
    id UInt64,
    val Nullable(Int64)
) ENGINE = MergeTree()
PARTITION BY id % 4
ORDER BY id
SETTINGS auto_statistics_types = '';

SET use_statistics_for_part_pruning = 1;

-- Part 0: val all NULL (range N/A, null_count=1)
INSERT INTO test_minmax_nullcount SELECT 0, NULL;
-- Part 1: val=[100,100], no NULLs (min=max=100, null_count=0)
INSERT INTO test_minmax_nullcount SELECT 1, 100;
-- Part 2: val=[200,200], no NULLs (min=max=200, null_count=0)
INSERT INTO test_minmax_nullcount SELECT 2, 200;
-- Part 3: val=[50,150], mixed NULLs (min=50, max=150, null_count>0)
INSERT INTO test_minmax_nullcount SELECT 3, if(number % 2, 50 + number, NULL) FROM numbers(100);

ALTER TABLE test_minmax_nullcount ADD STATISTICS val TYPE minmax;
ALTER TABLE test_minmax_nullcount ADD STATISTICS val TYPE nullcount;
ALTER TABLE test_minmax_nullcount MATERIALIZE STATISTICS val SETTINGS mutations_sync = 1;

-- With NullCount==0, part 1 (val=100) should be pruned by val > 150
SELECT 'Test 9a: MinMax+NullCount, val > 150 (Part 0,1 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_minmax_nullcount WHERE val > 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_minmax_nullcount WHERE val > 150;

-- IS NULL should prune parts 1,2 (no NULLs at all)
SELECT 'Test 9b: IS NULL with MinMax+NullCount (Parts 1,2 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_minmax_nullcount WHERE val IS NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_minmax_nullcount WHERE val IS NULL;

-- IS NOT NULL should prune part 0 (all NULL)
SELECT 'Test 9c: IS NOT NULL with MinMax+NullCount (Part 0 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_minmax_nullcount WHERE val IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_minmax_nullcount WHERE val IS NOT NULL;

DROP TABLE test_minmax_nullcount;

-- =============================================================================
-- Test 10: LowCardinality(Nullable) column IS NULL / IS NOT NULL pruning
-- =============================================================================
DROP TABLE IF EXISTS test_lc_null;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE test_lc_null (
    id UInt64,
    val LowCardinality(Nullable(Int64))
) ENGINE = MergeTree()
PARTITION BY id % 4
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Part 0: all NULL
INSERT INTO test_lc_null SELECT 0, NULL;
-- Part 1: no NULL
INSERT INTO test_lc_null SELECT 1, 100;
-- Part 2: no NULL
INSERT INTO test_lc_null SELECT 2, 200;
-- Part 3: mixed
INSERT INTO test_lc_null SELECT 3, if(number % 2, number * 10, NULL) FROM numbers(100);

ALTER TABLE test_lc_null ADD STATISTICS val TYPE nullcount;
ALTER TABLE test_lc_null MATERIALIZE STATISTICS val SETTINGS mutations_sync = 1;

SELECT 'Test 10a: LowCardinality IS NULL (Parts 1,2 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_lc_null WHERE val IS NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_lc_null WHERE val IS NULL;

SELECT 'Test 10b: LowCardinality IS NOT NULL (Part 0 pruned)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_lc_null WHERE val IS NOT NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_lc_null WHERE val IS NOT NULL;

DROP TABLE test_lc_null;

-- =============================================================================
-- Test 11: Multi-column conditions — IS NULL on one column + range on another
-- =============================================================================
DROP TABLE IF EXISTS test_multi;
CREATE TABLE test_multi (
    id UInt64,
    a Nullable(Int64),
    b Int64
) ENGINE = MergeTree()
PARTITION BY id % 3
ORDER BY id
SETTINGS auto_statistics_types = '';

-- Part 0: a all NULL, b=[1,10]
INSERT INTO test_multi SELECT 0, NULL, number FROM numbers(10);
-- Part 1: a no NULL, a=100, b=[100,109]
INSERT INTO test_multi SELECT 1, 100, 100 + number FROM numbers(10);
-- Part 2: a mixed NULL, b=[200,209]
INSERT INTO test_multi SELECT 2, if(number % 3, number, NULL), 200 + number FROM numbers(10);

ALTER TABLE test_multi ADD STATISTICS a TYPE nullcount;
ALTER TABLE test_multi ADD STATISTICS b TYPE minmax;
ALTER TABLE test_multi MATERIALIZE STATISTICS a SETTINGS mutations_sync = 1;
ALTER TABLE test_multi MATERIALIZE STATISTICS b SETTINGS mutations_sync = 1;

-- IS NULL on a: prunes Part 1 (no NULLs in a)
SELECT 'Test 11a: IS NULL on col a (Part 1 pruned by NullCount)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi WHERE a IS NULL)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_multi WHERE a IS NULL;

-- b >= 150: prunes Part 0 (b max=10) — KeyCondition handles this
SELECT 'Test 11b: b >= 150 (Part 0 pruned by MinMax)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi WHERE b >= 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_multi WHERE b >= 150;

-- a IS NULL AND b >= 150: prunes Part 0 (a all NULL kept) + Part 1 (a no NULL pruned) = Part 0,2 kept, but Part 0 b<150 pruned by range
SELECT 'Test 11c: a IS NULL AND b >= 150 (compound pruning)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_multi WHERE a IS NULL AND b >= 150)
WHERE explain LIKE '%Parts:%' OR explain LIKE '%Ranges:%';
SELECT count() FROM test_multi WHERE a IS NULL AND b >= 150;

DROP TABLE test_multi;

-- =============================================================================
-- Test 12: Tautology — IS NULL OR IS NOT NULL should return all rows
-- =============================================================================
DROP TABLE IF EXISTS test_tautology;
CREATE TABLE test_tautology (
    id UInt64,
    value Nullable(Int64)
) ENGINE = MergeTree()
PARTITION BY id % 3
ORDER BY id
SETTINGS auto_statistics_types = '';

SET use_statistics_for_part_pruning = 1;

-- Part 0: all NULL
INSERT INTO test_tautology SELECT 0, NULL;
-- Part 1: no NULL
INSERT INTO test_tautology SELECT 1, 100;
-- Part 2: mixed
INSERT INTO test_tautology SELECT 2, if(number % 2, number, NULL) FROM numbers(100);

ALTER TABLE test_tautology ADD STATISTICS value TYPE nullcount;
ALTER TABLE test_tautology MATERIALIZE STATISTICS value SETTINGS mutations_sync = 1;

SELECT 'Test 12a: IS NULL OR IS NOT NULL tautology (all rows, no pruning)';
SELECT count() FROM test_tautology WHERE value IS NULL OR value IS NOT NULL;

SELECT 'Test 12b: id IS NULL OR id IS NOT NULL on non-nullable column (all rows)';
SELECT count() FROM test_tautology WHERE id IS NULL OR id IS NOT NULL;

DROP TABLE test_tautology;
