DROP TABLE IF EXISTS 03760_src1;
DROP TABLE IF EXISTS 03760_src2;
DROP VIEW IF EXISTS 03760_view1;
DROP VIEW IF EXISTS 03760_view2;
DROP VIEW IF EXISTS 03760_view3;
DROP VIEW IF EXISTS 03760_mview1;

CREATE TABLE 03760_src1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE 03760_src2 (id UInt64, data String) ENGINE = MergeTree ORDER BY id;

CREATE VIEW 03760_view1 AS SELECT * FROM 03760_src1;
CREATE VIEW 03760_view2 AS SELECT 03760_src1.id, 03760_src1.value, 03760_src2.data FROM 03760_src1 JOIN 03760_src2 ON 03760_src1.id = 03760_src2.id;
CREATE VIEW 03760_view3 AS SELECT * FROM 03760_view1;

CREATE MATERIALIZED VIEW 03760_mview1 ENGINE = MergeTree ORDER BY id AS SELECT * FROM 03760_src1;

-- 03760_src1 should show 03760_view1, 03760_view2, and 03760_mview1 as dependents (direct only; 03760_view3 depends on 03760_view1, not src1)
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';

-- 03760_src2 should show 03760_view2 as dependent
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

-- 03760_view1 should show 03760_view3 as dependent
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view1';

-- 03760_view2 and 03760_view3 themselves must not have dependents
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view2';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_view3';

-- Check all tables and their dependencies (excluding internal MV storage tables)
SELECT name, engine, arraySort(dependencies_table) as deps
FROM system.tables
WHERE database = currentDatabase() AND NOT name LIKE '.inner%'
ORDER BY name;

-- CREATE OR REPLACE VIEW: dependencies must reflect the new query (old source loses view, new source gains it)
DROP VIEW IF EXISTS 03760_repl_view;
CREATE VIEW 03760_repl_view AS SELECT * FROM 03760_src1;
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

CREATE OR REPLACE VIEW 03760_repl_view AS SELECT id, data FROM 03760_src2;
-- After replace: 03760_src1 should no longer list 03760_repl_view; 03760_src2 should list it
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP VIEW 03760_repl_view;

-- ALTER VIEW ... MODIFY QUERY (Materialized View): dependencies must reflect the new query
DROP TABLE IF EXISTS 03760_mv_dest;
DROP VIEW IF EXISTS 03760_mv_alter;
CREATE TABLE 03760_mv_dest (id UInt64, data String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW 03760_mv_alter TO 03760_mv_dest AS SELECT id, value AS data FROM 03760_src1;
-- 03760_src1 should list 03760_mv_alter
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';

SET allow_experimental_alter_materialized_view_structure = 1;
ALTER TABLE 03760_mv_alter MODIFY QUERY SELECT id, data FROM 03760_src2;
-- After alter: 03760_src1 should no longer list 03760_mv_alter; 03760_src2 should list it
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP VIEW 03760_mv_alter;
DROP TABLE 03760_mv_dest;

-- Cross-database
CREATE DATABASE IF NOT EXISTS db_03760_x;
CREATE TABLE db_03760_x.remote_t (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_local_view_of_x AS SELECT * FROM db_03760_x.remote_t;
CREATE VIEW db_03760_x.remote_view_of_local AS SELECT * FROM 03760_src1;

-- 03760_src1 should list db_03760_x.remote_view_of_local among dependents (view in other db depends on current db table)
SELECT concat(dependencies_database, '.', dependencies_table) AS dep
FROM system.tables
ARRAY JOIN dependencies_database, dependencies_table
WHERE database = currentDatabase() AND name = '03760_src1'
ORDER BY dep;

-- db_03760_x.remote_t should list 03760_local_view_of_x as dependent (view in current db depends on other db table)
SELECT DISTINCT dependencies_table AS dep
FROM system.tables
ARRAY JOIN dependencies_table
WHERE database = 'db_03760_x' AND name = 'remote_t'
ORDER BY dep;

DROP VIEW db_03760_x.remote_view_of_local;
DROP VIEW 03760_local_view_of_x;
DROP TABLE db_03760_x.remote_t;
DROP DATABASE db_03760_x;

DROP VIEW 03760_view3;
DROP VIEW 03760_view2;
DROP VIEW 03760_view1;
DROP VIEW 03760_mview1;

-- Verify dropped views are no longer present in dependencies
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src1';
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_src2';

DROP TABLE 03760_src2;
DROP TABLE 03760_src1;

-- RENAME DATABASE: plain-view dependencies must be migrated to the new database name.
-- Three cases are exercised:
--   (a) single-source plain view: v_simple reads from src1 (same DB, renamed together)
--   (b) multi-source plain view: v_join reads from src1 JOIN src2 (same DB, renamed together)
--   (c) cross-db plain view: v_xdb lives in an external DB and reads from src1 (source DB renamed)
DROP DATABASE IF EXISTS db_03760_rename_before;
DROP DATABASE IF EXISTS db_03760_rename_after;
DROP DATABASE IF EXISTS db_03760_rename_ext;

CREATE DATABASE db_03760_rename_before ENGINE = Atomic;
CREATE TABLE db_03760_rename_before.src1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE db_03760_rename_before.src2 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW db_03760_rename_before.v_simple AS SELECT * FROM db_03760_rename_before.src1;
CREATE VIEW db_03760_rename_before.v_join   AS SELECT a.id FROM db_03760_rename_before.src1 a JOIN db_03760_rename_before.src2 b ON a.id = b.id;

CREATE DATABASE db_03760_rename_ext ENGINE = Atomic;
CREATE VIEW db_03760_rename_ext.v_xdb AS SELECT * FROM db_03760_rename_before.src1;

-- Before rename: verify all three views appear as dependents.
SELECT arraySort(arrayMap((x, y) -> concat(x, '.', y), dependencies_database, dependencies_table))
FROM system.tables WHERE database = 'db_03760_rename_before' AND name = 'src1';
SELECT arraySort(dependencies_table)
FROM system.tables WHERE database = 'db_03760_rename_before' AND name = 'src2';

RENAME DATABASE db_03760_rename_before TO db_03760_rename_after;

-- After rename: the same three views must still be reported under the new DB name.
-- v_simple and v_join have been renamed together with their source; v_xdb stays in its own DB.
SELECT arraySort(arrayMap((x, y) -> concat(x, '.', y), dependencies_database, dependencies_table))
FROM system.tables WHERE database = 'db_03760_rename_after' AND name = 'src1';
SELECT arraySort(dependencies_table)
FROM system.tables WHERE database = 'db_03760_rename_after' AND name = 'src2';

DROP DATABASE db_03760_rename_after;
DROP DATABASE db_03760_rename_ext;

-- DROP source table must remove plain_view_dependencies edges where it appears as source.
DROP TABLE IF EXISTS 03760_stale_src;
DROP VIEW  IF EXISTS 03760_stale_view;

CREATE TABLE 03760_stale_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_stale_view AS SELECT * FROM 03760_stale_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_stale_src';

DROP VIEW 03760_stale_view;
DROP TABLE 03760_stale_src;

-- Recreate the source table under the same name (no view exists any more).
CREATE TABLE 03760_stale_src (id UInt64) ENGINE = MergeTree ORDER BY id;

-- The dropped view must not appear as a dependent of the new table.
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_stale_src';

DROP TABLE 03760_stale_src;

-- RENAME TABLE (source): plain_view_dependencies outgoing edges must be re-added under the new name.
DROP TABLE IF EXISTS 03760_rename_src;
DROP TABLE IF EXISTS 03760_rename_src2;
DROP VIEW  IF EXISTS 03760_rename_view;

CREATE TABLE 03760_rename_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW 03760_rename_view AS SELECT * FROM 03760_rename_src;

SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_rename_src';

RENAME TABLE 03760_rename_src TO 03760_rename_src2;

-- After rename, 03760_rename_src2 must have the same plain view dependency.
SELECT arraySort(dependencies_table) FROM system.tables WHERE database = currentDatabase() AND name = '03760_rename_src2';

DROP VIEW 03760_rename_view;
DROP TABLE 03760_rename_src2;
