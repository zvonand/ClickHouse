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
SETTINGS alter_column_secondary_index_mode = 'rebuild';

INSERT INTO test_skip_index_alter VALUES (1, '10'), (2, '20'), (3, '300');

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
SYSTEM SYNC MUTATIONS test_skip_index_alter;

-- After mutation completes, the index should work with the new type
SELECT count() FROM test_skip_index_alter WHERE value = 300;

DROP TABLE test_skip_index_alter;
