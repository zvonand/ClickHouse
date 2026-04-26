#!/usr/bin/env bash
# Tags: race, no-fasttest, use-rocksdb

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit
set -o pipefail

# Test for race conditions when querying system.rocksdb / reading / writing /
# optimizing an EmbeddedRocksDB table while it is being dropped and recreated.
#
# The reader/writer/optimize loops on the user table can race naturally with
# DROP TABLE in create_drop_thread, so transient failures like
# "Table ... does not exist" are expected. Tolerate them with `|| true` so the
# workers keep running for the full timeout instead of exiting on the first
# such error under errexit.
#
# `read_stat_thread` is intentionally NOT tolerant: querying `system.rocksdb`
# is the path that exercises the original race, so any exception there must
# fail the test rather than be silently swallowed.

function create_drop_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.rocksdb_race"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.rocksdb_race (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key)"
        $CLICKHOUSE_CLIENT -q "INSERT INTO ${CLICKHOUSE_DATABASE}.rocksdb_race SELECT toString(number), number FROM numbers(100)"
    done
}

function read_stat_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SELECT * FROM system.rocksdb FORMAT Null"
    done
}

function select_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.rocksdb_race FORMAT Null" || true
    done
}

function optimize_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${CLICKHOUSE_DATABASE}.rocksdb_race" || true
    done
}

TIMEOUT=10

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.rocksdb_race"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.rocksdb_race (key String, value UInt32) Engine=EmbeddedRocksDB PRIMARY KEY(key)"

create_drop_thread 2>/dev/null &
read_stat_thread 2>/dev/null &
select_thread 2>/dev/null &
optimize_thread 2>/dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.rocksdb_race"
echo "OK"
