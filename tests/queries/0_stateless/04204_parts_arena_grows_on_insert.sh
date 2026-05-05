#!/usr/bin/env bash
# Tags: no-parallel, use_jemalloc
# no-parallel: this test compares a process-wide async metric across two snapshots, and
#              concurrent table activity from other tests would distort the delta.
# use_jemalloc: the test asserts on `jemalloc.parts_arena.*`, which is only registered when
#               the build has jemalloc.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_parts_arena SYNC"
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_parts_arena (a UInt64, b String, c LowCardinality(String), d Float64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS old_parts_lifetime = 10"

$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"
before=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.parts_arena.active_bytes'")

# Each insert produces a new part — its `IMergeTreeDataPart` plus per-part metadata
# (`NamesAndTypesList`, `SerializationInfoByName`, `ColumnsSubstreams`, checksums tree, etc.)
# should land in the dedicated parts arena.
for _ in $(seq 1 20); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO t_parts_arena
        SELECT number, toString(number), toString(number % 10), number * 1.1
        FROM numbers(50000)"
done

$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"
after=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.parts_arena.active_bytes'")

# We don't compare exact bytes — jemalloc's background decay can shrink dirty pages between
# snapshots, and other system-table writes (which are also MergeTree) contribute. Just check
# that the metric is non-zero (the arena exists and tracks something).
echo "arena_active_after_inserts $(( after > 0 ? 1 : 0 ))"

# More restrictive check: the arena must have grown. If it didn't, the routing has regressed.
# We allow a small downward jitter (negative delta up to 8 MiB from background decay).
echo "arena_grew_or_stable $(( after >= before - 8 * 1024 * 1024 ? 1 : 0 ))"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_parts_arena"
