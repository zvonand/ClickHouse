-- Tags: distributed
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/56208.

DROP DATABASE IF EXISTS test_query_node;
DROP DATABASE IF EXISTS test_data_node;
CREATE DATABASE test_query_node;
CREATE DATABASE test_data_node;

CREATE TABLE test_data_node.local_data (name String) ENGINE = MergeTree ORDER BY name AS SELECT toString(number) FROM numbers(10);

CREATE TABLE test_data_node.local_merged_data (name String) ENGINE = Merge(test_data_node, '^local_data$');

CREATE TABLE test_query_node.distributed_data (name String) ENGINE = Distributed(test_shard_localhost, test_data_node, local_merged_data);

CREATE TABLE test_query_node.local_merged_data (name String) ENGINE = Merge(test_query_node, '^distributed_data$');

-- `prefer_localhost_replica = 0` forces the Distributed query to go through the network layer
-- (and  through the remote-parse path  that was broken), even though the shard is on localhost.
SET prefer_localhost_replica = 0;

SELECT '-- enable_analyzer=1 --';
SET enable_analyzer = 1;
SELECT count() FROM test_query_node.local_merged_data;
SELECT DISTINCT _table FROM test_query_node.local_merged_data;

SELECT '-- enable_analyzer=0 --';
SET enable_analyzer = 0;
SELECT count() FROM test_query_node.local_merged_data;
SELECT DISTINCT _table FROM test_query_node.local_merged_data;

DROP DATABASE test_query_node;
DROP DATABASE test_data_node;
