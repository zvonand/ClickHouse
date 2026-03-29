-- Test that skip indexes are properly skipped when ALTER MODIFY COLUMN changes
-- the column type and the mutation has not yet been applied.
-- This verifies that `getAllUpdatedColumns` correctly reports columns changed by
-- ALTER mutations, not just data mutations (UPDATE/DELETE).

DROP TABLE IF EXISTS test_skip_index_alter;

CREATE TABLE test_skip_index_alter
(
    id UInt64,
    value String,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 128, alter_column_secondary_index_mode = 'rebuild';

-- Insert enough rows across multiple granules so the skip index is populated and used.
-- Granule 1..128 has value '10', granule 129..256 has value '300'.
-- A query for '300' should skip the first granule via the set index.
INSERT INTO test_skip_index_alter SELECT number, if(number < 128, '10', '300') FROM numbers(256);

-- Verify initial index usage works
SELECT count() FROM test_skip_index_alter WHERE value = '300';

-- Stop merges so the mutation doesn't get applied
SYSTEM STOP MERGES test_skip_index_alter;

-- Change column type; creates an ALTER mutation (READ_COLUMN), not a data mutation
SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE test_skip_index_alter MODIFY COLUMN value Nullable(UInt64);

-- The index data is now incompatible with the new type.
-- This should NOT crash, the index should be skipped for old parts.
SELECT count() FROM test_skip_index_alter WHERE value = 300;

SYSTEM START MERGES test_skip_index_alter;
OPTIMIZE TABLE test_skip_index_alter FINAL SETTINGS mutations_sync = 2;

-- After mutation completes, the index should work with the new type
SELECT count() FROM test_skip_index_alter WHERE value = 300;

DROP TABLE test_skip_index_alter;
