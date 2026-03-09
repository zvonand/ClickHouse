-- Tags: no-replicated-database
-- Test that view dependencies are properly cleaned up in `updateDependencies`
-- when `new_view_dependencies` is empty, preventing stale edges from accumulating.
-- Reproduces assertion failure from https://github.com/ClickHouse/ClickHouse/issues/98706

DROP DATABASE IF EXISTS test_mv_deps;
CREATE DATABASE test_mv_deps ENGINE = Memory;

CREATE TABLE test_mv_deps.src1 (c0 Int) ENGINE = Memory;
CREATE TABLE test_mv_deps.src2 (c0 Int) ENGINE = Memory;
CREATE TABLE test_mv_deps.target (c0 Int) ENGINE = Memory;
CREATE MATERIALIZED VIEW test_mv_deps.mv TO test_mv_deps.target AS SELECT c0 FROM test_mv_deps.src1;

-- BACKUP/RESTORE allows us to re-add the src1->mv edge later
BACKUP VIEW test_mv_deps.mv TO Memory('04027_backup.zip') SYNC;

-- Replace source: src1->mv becomes src2->mv
ALTER TABLE test_mv_deps.mv MODIFY QUERY SELECT c0 FROM test_mv_deps.src2;

-- This should clean up the src2->mv edge because new_view_dependencies is empty,
-- but previously it leaked the stale edge due to the removal being inside
-- `if (!new_view_dependencies.empty())`
ALTER TABLE test_mv_deps.mv MODIFY QUERY SELECT 1 AS c0;

TRUNCATE DATABASE test_mv_deps;

-- Restore re-adds src1->mv via addDependencies.
-- Without the fix, stale src2->mv is still present, giving us {src2, src1}.
RESTORE VIEW test_mv_deps.mv FROM Memory('04027_backup.zip') SYNC FORMAT Null;

-- This triggers updateDependencies; with the stale edge it would hit
-- assert(tables_from.size() == 1) in debug builds.
ALTER TABLE test_mv_deps.mv MODIFY COMMENT '';

-- Verify the MV still works correctly
INSERT INTO test_mv_deps.src1 VALUES (1), (2), (3);
SELECT c0 FROM test_mv_deps.target ORDER BY c0;

DROP DATABASE test_mv_deps;
