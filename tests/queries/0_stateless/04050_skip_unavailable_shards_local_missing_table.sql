-- Tags: shard, no-fasttest

-- Test that skip_unavailable_shards works for local shards with missing tables
-- https://github.com/ClickHouse/ClickHouse/issues/100134

DROP TABLE IF EXISTS dist_04050;

CREATE TABLE dist_04050 (x UInt32)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), non_existent_table_04050);

-- Without skip_unavailable_shards, the query should fail
SELECT * FROM dist_04050; -- { serverError UNKNOWN_TABLE }

-- With skip_unavailable_shards, the query should succeed (returning empty result)
SELECT * FROM dist_04050 SETTINGS skip_unavailable_shards = 1;

DROP TABLE dist_04050;
