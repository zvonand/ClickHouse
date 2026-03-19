-- Tags: replica

-- Verify that parallel_replicas_prefer_local_replica = 0 allows queries to be
-- directed to another replica even with max_parallel_replicas = 1.

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t SELECT number, toString(number) FROM numbers(1000);

-- With prefer_local_replica = 0 and max_parallel_replicas = 1, the query should
-- use the parallel replicas path (i.e., the query is sent to a replica selected
-- by load balancing, not necessarily the local one).
SELECT count()
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- With prefer_local_replica = 0 and max_parallel_replicas = 2, the query should
-- also work and use 2 replicas (but local is not guaranteed to be among them).
SELECT count()
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- Default behavior (prefer_local_replica = 1) with max_parallel_replicas = 1
-- should NOT use parallel replicas (backward compatibility).
SELECT count()
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE t;
