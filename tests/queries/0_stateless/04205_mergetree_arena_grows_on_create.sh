#!/usr/bin/env bash
# Tags: no-parallel, use_jemalloc
# no-parallel: this test compares a process-wide async metric across two snapshots, and
#              concurrent table activity from other tests would distort the delta.
# use_jemalloc: the test asserts on `jemalloc.mergetree_arena.*`, which is only registered when
#               the build has jemalloc.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cover the table-level (not per-part) routing: `MergeTreeData::setProperties` clones
# `StorageInMemoryMetadata`/`ColumnsDescription`/`VirtualColumnsDescription`/key+index ASTs
# under the MergeTree arena scope, so creating a `MergeTree` table with no inserts must still
# grow `jemalloc.mergetree_arena.active_bytes`.

$CLICKHOUSE_CLIENT -q "SYSTEM JEMALLOC PURGE"
$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"
before=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.active_bytes'")

# A few dozen tables with realistic-width schemas — one CREATE per table, no inserts.
# The columns are deliberately a mix of complex types so each table's `ColumnsDescription` /
# `serializations` map / virtuals / key ASTs are non-trivial.
for i in $(seq 1 50); do
    $CLICKHOUSE_CLIENT -q "
        CREATE TABLE t_create_arena_$i (
            a UInt64, b String, c LowCardinality(String), d Float64,
            e Array(Tuple(UInt64, String)), f Map(String, Float64),
            g Nullable(DateTime), h Decimal(38, 4), i UUID, j IPv6
        ) ENGINE = MergeTree ORDER BY a"
done

$CLICKHOUSE_CLIENT -q "SYSTEM RELOAD ASYNCHRONOUS METRICS"
after=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.asynchronous_metrics WHERE metric = 'jemalloc.mergetree_arena.active_bytes'")

# CREATE TABLE alone (with no inserts) must increase the arena. If the table-level scope
# guard ever stops covering `setProperties`, this delta will collapse to ~0 and the test
# will catch it. We require at least 1 MiB total across 50 tables (well below the empirical
# ~3.5 MiB so we have margin against future schema evolution / setting churn).
echo "arena_grew_on_create $(( after - before >= 1024 * 1024 ? 1 : 0 ))"

for i in $(seq 1 50); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_create_arena_$i SYNC"
done
