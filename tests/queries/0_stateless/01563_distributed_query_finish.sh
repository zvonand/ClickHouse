#!/usr/bin/env bash
# Tags: distributed, no-parallel, no-fasttest
# Tag no-fasttest: Checks system.errors

# query finish should not produce any NETWORK_ERROR
# (NETWORK_ERROR will be in case of connection reset)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m <<EOL
drop table if exists dist_01247;
drop table if exists data_01247;

create table data_01247 engine=Memory() as select * from numbers(2);
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, '$CLICKHOUSE_DATABASE', data_01247, number);

select * from dist_01247 format Null;
EOL

# NOTE: it is possible to got NETWORK_ERROR even with no-parallel, at least due to system.*_log_sender to the cloud
for ((i = 0; i < 100; ++i)); do
    network_errors_before=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.errors WHERE name = 'NETWORK_ERROR'")

    opts=(
        "--max_distributed_connections=1"
        "--optimize_skip_unused_shards=1"
        "--optimize_distributed_group_by_sharding_key=1"
        "--prefer_localhost_replica=0"
    )
    $CLICKHOUSE_CLIENT "${opts[@]}" --format CSV -m -q "select count(), * from dist_01247 group by number order by number limit 1 format Null"

    # expect zero new network errors
    network_errors_after=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.errors WHERE name = 'NETWORK_ERROR'")

    if [[ $((network_errors_after-network_errors_before)) -eq 0 ]]; then
        break
    fi
done
echo NETWORK_ERROR=$(( network_errors_after-network_errors_before ))

$CLICKHOUSE_CLIENT -q "drop table data_01247"
$CLICKHOUSE_CLIENT -q "drop table dist_01247"
