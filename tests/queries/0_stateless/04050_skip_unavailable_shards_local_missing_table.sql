-- Tags: shard, no-fasttest

-- Test that skip_unavailable_shards skips local shards with missing tables
-- but still throws when all shards end up being skipped.
-- https://github.com/ClickHouse/ClickHouse/issues/100134

DROP TABLE IF EXISTS dist_04050;

CREATE TABLE dist_04050 (x UInt32)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), non_existent_table_04050);

-- Without skip_unavailable_shards, the query should fail with UNKNOWN_TABLE
SELECT * FROM dist_04050 SETTINGS prefer_localhost_replica = 1; -- { serverError UNKNOWN_TABLE }

-- With skip_unavailable_shards, the shard is skipped, but since it is the only shard,
-- there are zero available shards and the query should still fail.
SELECT * FROM dist_04050 SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DROP TABLE dist_04050;
