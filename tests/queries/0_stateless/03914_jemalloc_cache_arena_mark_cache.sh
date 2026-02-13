#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings

# Test that mark cache allocations use the dedicated jemalloc cache arena
# and that SYSTEM DROP MARK CACHE properly reclaims arena pages.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function clear_all_arena_caches()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP MARK CACHE"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP INDEX MARK CACHE"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP UNCOMPRESSED CACHE"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP INDEX UNCOMPRESSED CACHE"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP PAGE CACHE"
}

function get_pactive()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM RELOAD ASYNCHRONOUS METRICS;
        SELECT value FROM system.asynchronous_metrics
        WHERE metric = 'jemalloc.cache_arena.pactive'"
}

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_cache_arena_marks"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_cache_arena_marks (a UInt64, b String, c Float64)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, prewarm_mark_cache = 0"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO t_cache_arena_marks
    SELECT number, toString(number), number * 1.1 FROM numbers(5000000)"

clear_all_arena_caches

# Verify cache is empty
$CLICKHOUSE_CLIENT -q "
    SELECT 'before_select', value
    FROM system.metrics WHERE metric = 'MarkCacheBytes'"

# Force mark loading
$CLICKHOUSE_CLIENT -q "
    SELECT count() FROM t_cache_arena_marks WHERE NOT ignore(*) FORMAT Null"

# Verify marks are cached
$CLICKHOUSE_CLIENT -q "
    SELECT 'after_select', value > 0
    FROM system.metrics WHERE metric = 'MarkCacheBytes'"

# Check if jemalloc is enabled
jemalloc_enabled=$($CLICKHOUSE_CLIENT -q "
    SELECT value IN ('ON', '1')
    FROM system.build_options WHERE name = 'USE_JEMALLOC'")

if [ "$jemalloc_enabled" = "1" ]; then
    pactive_loaded=$(get_pactive)

    echo "arena_active	$(( pactive_loaded > 0 ? 1 : 0 ))"

    # Retry loop: clear caches and check that pactive decreased.
    # Background merges on tables can reload marks into the cache arena
    # between the clear and the measurement, so we retry a few times.
    reclaimed=0
    for _ in $(seq 1 5); do
        clear_all_arena_caches

        pactive_cleared=$(get_pactive)

        if [ "$pactive_cleared" -lt "$pactive_loaded" ]; then
            reclaimed=1
            break
        fi
    done

    $CLICKHOUSE_CLIENT -q "
        SELECT 'after_clear', value
        FROM system.metrics WHERE metric = 'MarkCacheBytes'"

    echo "arena_reclaimed	${reclaimed}"
else
    echo "arena_active	1"
    $CLICKHOUSE_CLIENT -q "
        SELECT 'after_clear', value
        FROM system.metrics WHERE metric = 'MarkCacheBytes'"
    echo "arena_reclaimed	1"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_arena_marks"
