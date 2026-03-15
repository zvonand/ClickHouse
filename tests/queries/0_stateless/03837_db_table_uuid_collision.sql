-- Tags: no-parallel, no-replicated-database, no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/98615
-- Creating a table via ON CLUSTER with the same UUID as an existing database should give
-- a proper error, not trigger an assertion failure in DatabaseCatalog::getTableImpl.
--
-- The ON CLUSTER path causes DDLWorker::processTask to call tryGetTable (via getTableImpl)
-- before executing the DDL. Without the fix, getTableImpl hits:
--   assert(!db_and_table.first && !db_and_table.second)
-- because the UUID maps to a database entry {db_ptr, nullptr} rather than a missing entry.

DROP DATABASE IF EXISTS d_uuid_collision_98615;

CREATE DATABASE d_uuid_collision_98615 UUID '10000000-0000-0000-0000-000000000001' ENGINE = Atomic;

-- Attempt to create a table with the same UUID as the database via ON CLUSTER.
-- This should fail with TABLE_ALREADY_EXISTS, not abort the server.
SET distributed_ddl_output_mode = 'throw';
CREATE TABLE d_uuid_collision_98615.t0 UUID '10000000-0000-0000-0000-000000000001'
    ON CLUSTER test_shard_localhost
    (c0 Int32) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError TABLE_ALREADY_EXISTS }

DROP DATABASE d_uuid_collision_98615;
