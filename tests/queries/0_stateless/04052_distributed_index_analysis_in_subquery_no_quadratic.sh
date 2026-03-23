#!/usr/bin/env bash
# Tags: no-replicated-database
# - no-replicated-database - it uses cluster of multiple nodes, while we use different clusters

# Verify that distributed index analysis with parallel replicas does not produce
# a quadratic number of queries when the predicate contains an IN subquery.
# The `distributed_index_analysis_only_on_coordinator` setting restricts distributed
# index analysis to the coordinator and disables `enable_parallel_replicas` for the
# remote index analysis queries, preventing O(N^2) recursive spawning.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_in_dia;

    CREATE TABLE test_in_dia (key Int, value Int)
    ENGINE = MergeTree()
    ORDER BY key
    SETTINGS
        distributed_index_analysis_min_parts_to_activate = 0,
        distributed_index_analysis_min_indexes_bytes_to_activate = 0;

    SYSTEM STOP MERGES test_in_dia;

    INSERT INTO test_in_dia
    SELECT number, number * 100
    FROM numbers(100000)
    SETTINGS
        max_block_size = 10000,
        min_insert_block_size_rows = 10000,
        max_insert_threads = 1;
"

query_id="$RANDOM-$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --query_id $query_id -q "
    SELECT sum(key)
    FROM test_in_dia
    WHERE key IN (SELECT key FROM test_in_dia WHERE key > 50000)
    SETTINGS
        automatic_parallel_replicas_mode = 0,
        parallel_replicas_for_non_replicated_merge_tree = 1,
        parallel_replicas_index_analysis_only_on_coordinator = 1,
        parallel_replicas_local_plan = 1,
        use_query_condition_cache = 0,
        distributed_index_analysis_for_non_shared_merge_tree = 1,
        enable_parallel_replicas = 1,
        distributed_index_analysis = 1,
        distributed_index_analysis_only_on_coordinator = 1,
        cluster_for_parallel_replicas = 'parallel_replicas',
        send_logs_level = 'error';
"

# Verify the total number of spawned queries is bounded (not quadratic).
# For the parallel_replicas cluster:
#   with fix: O(N) queries
#   without fix: O(N^2) queries
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;

    SELECT
        if (count() >= 3 AND count() <= 20, 'OK', format('Expected 3-20 queries, got {}', count())) AS queries_with_subqueries,
        anyIf(ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] > 0, is_initial_query) AS used_distributed_index_analysis
    FROM system.query_log
    WHERE
        event_date >= yesterday() AND event_time >= now() - 600
        AND type = 'QueryFinish'
        AND query_kind = 'Select'
        AND initial_query_id = '$query_id'
        -- Bypass style check. Database name is 'default' for queries on workers.
        -- Database name is embedded in the query_id.
        AND (current_database = currentDatabase() OR 1)
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_in_dia"
