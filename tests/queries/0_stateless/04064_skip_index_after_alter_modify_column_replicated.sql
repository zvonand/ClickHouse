-- Tags: zookeeper

-- Test that skip indexes are properly skipped when ALTER MODIFY COLUMN changes
-- the column type and the mutation has not yet been applied (ReplicatedMergeTree variant).
-- This verifies that `ReplicatedMergeTreeQueue::MutationsSnapshot::getAllUpdatedColumns`
-- correctly reports columns changed by ALTER mutations, not just data mutations.
--
-- Uses UInt64 → Float64 conversion because both types use 8-byte fixed-width
-- serialization: the old UInt64 bytes are reinterpreted as Float64 without
-- deserialization errors, producing tiny denormalized values that silently cause
-- the set index to incorrectly skip granules (returning 0 rows instead of 128).

DROP TABLE IF EXISTS test_skip_index_alter_replicated;

CREATE TABLE test_skip_index_alter_replicated
(
    id UInt64,
    value UInt64,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_skip_index_alter_replicated', '1')
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough rows across multiple granules so the skip index is populated and used.
-- Granule 0 (rows 0..127) has value 200, granule 1 (rows 128..255) has value 300.
INSERT INTO test_skip_index_alter_replicated SELECT number, if(number < 128, 200, 300) FROM numbers(256);

-- Verify initial index usage works
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300 SETTINGS force_data_skipping_indices = 'idx_value';

-- Stop merges so the mutation doesn't get applied
SYSTEM STOP MERGES test_skip_index_alter_replicated;

-- Change column type; creates an ALTER mutation (READ_COLUMN), not a data mutation
SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE test_skip_index_alter_replicated MODIFY COLUMN value Float64;

-- The index data is now incompatible with the new type.
-- Without the fix, the skip index is used on old parts and the UInt64 data
-- is reinterpreted as Float64 (same byte width but different encoding),
-- producing tiny denormalized values that don't match 300.0, so the index
-- incorrectly skips all granules and returns 0 rows.
-- With the fix, the index is correctly skipped for old parts.
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value';

SYSTEM START MERGES test_skip_index_alter_replicated;
OPTIMIZE TABLE test_skip_index_alter_replicated FINAL SETTINGS mutations_sync = 2;

-- After mutation completes, the index should work with the new type
SELECT count() FROM test_skip_index_alter_replicated WHERE value = 300.0 SETTINGS force_data_skipping_indices = 'idx_value';

DROP TABLE test_skip_index_alter_replicated;
