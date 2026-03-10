-- Tags: no-parallel-replicas, no-replicated-database, long
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: fails due to additional shard.

-- Regression test for https://github.com/ClickHouse/clickhouse-core-incidents/issues/1021
-- When multiple patch parts (Merge + Join mode) update the same columns,
-- the column ordering in patch blocks must be deterministic to avoid
-- LOGICAL_ERROR "Block structure mismatch in patch parts stream".

SET insert_keeper_fault_injection_probability = 0.0;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_patch_order SYNC;

-- Create a table with several columns (more columns = more likely to trigger non-deterministic ordering)
CREATE TABLE t_patch_order (id UInt64, a_col String, b_col UInt64, c_col Float64, d_col UInt32, e_col String)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_patch_order/', '1')
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 0;

-- Insert two separate blocks to create two base parts
INSERT INTO t_patch_order VALUES (1, 'hello', 10, 1.5, 100, 'world');
INSERT INTO t_patch_order VALUES (2, 'foo', 20, 2.5, 200, 'bar');

-- First UPDATE: creates Merge-mode patch parts for both base parts
UPDATE t_patch_order SET a_col = 'updated1', b_col = 99, c_col = 9.9, d_col = 999, e_col = 'upd1' WHERE 1;

-- Verify before merge
SELECT * FROM t_patch_order ORDER BY id;

-- Merge base parts into one part; patches become Join-mode
OPTIMIZE TABLE t_patch_order PARTITION ID 'all' FINAL;

-- Second UPDATE: creates new Merge-mode patch parts for the merged base part
UPDATE t_patch_order SET a_col = 'updated2', b_col = 88, c_col = 8.8, d_col = 888, e_col = 'upd2' WHERE 1;

-- This SELECT must apply both Join-mode (from before merge) and Merge-mode (after merge) patches.
-- Before the fix, this could fail with LOGICAL_ERROR due to non-deterministic column ordering
-- in patch blocks from different modes.
SELECT * FROM t_patch_order ORDER BY id;

-- Also verify with APPLY PATCHES
ALTER TABLE t_patch_order APPLY PATCHES SETTINGS mutations_sync = 2;
SELECT * FROM t_patch_order ORDER BY id SETTINGS apply_patch_parts = 0;

DROP TABLE t_patch_order SYNC;
