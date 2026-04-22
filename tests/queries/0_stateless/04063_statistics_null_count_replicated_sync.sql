-- Tags: zookeeper
-- Test NullCount statistics on ReplicatedMergeTree: replica synchronization
-- Verifies that NullCount statistics are correctly propagated between replicas.

SET allow_statistics = 1;
SET use_statistics_for_part_pruning = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS rmt_nc1 SYNC;
DROP TABLE IF EXISTS rmt_nc2 SYNC;

CREATE TABLE rmt_nc1 (
    id UInt64,
    value Nullable(Int64)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_nullcount_replicated', '1')
ORDER BY id
PARTITION BY id % 2
SETTINGS auto_statistics_types = '';

CREATE TABLE rmt_nc2 (
    id UInt64,
    value Nullable(Int64)
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_nullcount_replicated', '2')
ORDER BY id
PARTITION BY id % 2
SETTINGS auto_statistics_types = '';

-- Add NullCount statistics on replica 1
ALTER TABLE rmt_nc1 ADD STATISTICS value TYPE nullcount;

-- Insert on replica 1: partition 0 has all NULLs, partition 1 has no NULLs
INSERT INTO rmt_nc1 SELECT 0, NULL;
INSERT INTO rmt_nc1 SELECT 1, 100;

-- Materialize NullCount on replica 1
ALTER TABLE rmt_nc1 MATERIALIZE STATISTICS value;

-- Verify NullCount exists on replica 1
SELECT 'Replica 1: NullCount statistics';
SELECT partition, column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'rmt_nc1' AND active AND column = 'value'
ORDER BY partition;

-- Note: IS NULL part pruning via nullcount was removed in Part 1.
-- We keep the EXPLAIN for coverage, but all parts are read now.
SELECT 'Replica 1: IS NULL (no part pruning)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM rmt_nc1 WHERE value IS NULL)
WHERE explain LIKE '%Parts:%';

-- Sync replica 2
SYSTEM SYNC REPLICA rmt_nc2;

-- Verify NullCount statistics propagated to replica 2
SELECT 'Replica 2: NullCount statistics after sync';
SELECT partition, column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'rmt_nc2' AND active AND column = 'value'
ORDER BY partition;

-- Note: IS NULL part pruning via nullcount was removed in Part 1.
SELECT 'Replica 2: IS NULL (no part pruning)';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM rmt_nc2 WHERE value IS NULL)
WHERE explain LIKE '%Parts:%';

-- Verify query results on both replicas
SELECT 'Replica 1: query results';
SELECT count() FROM rmt_nc1 WHERE value IS NULL;
SELECT count() FROM rmt_nc1 WHERE value IS NOT NULL;

SELECT 'Replica 2: query results';
SELECT count() FROM rmt_nc2 WHERE value IS NULL;
SELECT count() FROM rmt_nc2 WHERE value IS NOT NULL;

-- Insert on replica 2 and materialize
INSERT INTO rmt_nc2 SELECT 2, NULL;
ALTER TABLE rmt_nc2 MATERIALIZE STATISTICS value;

-- Sync replica 1
SYSTEM SYNC REPLICA rmt_nc1;

-- Verify both replicas have correct data after bidirectional sync
SELECT 'After bidirectional sync';
SELECT count() FROM rmt_nc1 WHERE value IS NULL;
SELECT count() FROM rmt_nc2 WHERE value IS NULL;

-- Verify NullCount statistics still exist on both replicas
SELECT 'Replica 1: NullCount after merge';
SELECT partition, column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'rmt_nc1' AND active AND column = 'value'
ORDER BY partition;

SELECT 'Replica 2: NullCount after merge';
SELECT partition, column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'rmt_nc2' AND active AND column = 'value'
ORDER BY partition;

DROP TABLE IF EXISTS rmt_nc1 SYNC;
DROP TABLE IF EXISTS rmt_nc2 SYNC;
