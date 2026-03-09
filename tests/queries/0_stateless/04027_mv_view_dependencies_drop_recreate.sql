-- Tags: no-replicated-database
-- Test that modifying a materialized view's query does not leak stale view dependency edges.
-- The root cause: in updateDependencies, view dependency removal was inside
-- if (!new_view_dependencies.empty()), so ALTER TABLE mv MODIFY QUERY SELECT 1 c0
-- (with no view dependencies) would leak stale edges. Combined with BACKUP/RESTORE
-- this accumulated multiple dependents, violating the assertion.

DROP DATABASE IF EXISTS d0;
CREATE DATABASE d0 ENGINE = Memory;
CREATE TABLE d0.src1 (c0 Int) ENGINE = Memory;
CREATE TABLE d0.src2 (c0 Int) ENGINE = Memory;
CREATE TABLE d0.target (c0 Int) ENGINE = Memory;
CREATE MATERIALIZED VIEW d0.mv TO d0.target AS SELECT c0 FROM d0.src1;

BACKUP VIEW d0.mv TO Memory('04027_backup.tgz') SETTINGS id='04027_backup' FORMAT Null;

ALTER TABLE d0.mv MODIFY QUERY SELECT c0 FROM d0.src2;
ALTER TABLE d0.mv MODIFY QUERY SELECT 1 c0; -- leaks stale edge without the fix

TRUNCATE DATABASE d0;

RESTORE VIEW d0.mv FROM Memory('04027_backup.tgz') SETTINGS id='04027_restore' FORMAT Null;

-- This triggers updateDependencies and would hit assert(tables_from.size() == 1) without the fix
ALTER TABLE d0.mv MODIFY COMMENT '';

SELECT 'OK';

DROP DATABASE d0;
