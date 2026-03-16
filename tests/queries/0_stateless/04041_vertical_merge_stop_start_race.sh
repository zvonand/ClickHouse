#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/99578
#
# A TOCTOU race between SYSTEM STOP MERGES / SYSTEM START MERGES could let a
# vertical merge continue after the horizontal stage processed zero rows,
# triggering:
#   "Number of rows in source parts ... differs from number of bytes written
#    to rows_sources file ... It is a bug."
#
# The fix latches the cancellation into the per-task `is_cancelled` flag so
# that `checkOperationIsNotCanceled` reliably throws even when the global
# blocker is cleared concurrently.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="test_vertical_merge_race_${CLICKHOUSE_DATABASE}"

# 12 value columns + 1 key column → triggers vertical merge algorithm
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (
        key UInt64,
        v1 String, v2 String, v3 String, v4 String,
        v5 String, v6 String, v7 String, v8 String,
        v9 String, v10 String, v11 String, v12 String
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS
        vertical_merge_algorithm_min_rows_to_activate = 1,
        vertical_merge_algorithm_min_columns_to_activate = 1,
        min_bytes_for_wide_part = 0;
"

# Record the current count of LOGICAL_ERROR so we can detect new ones.
errors_before=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.errors WHERE name = 'LOGICAL_ERROR'
" 2>/dev/null)
errors_before=${errors_before:-0}

ITERATIONS=30

for i in $(seq 1 $ITERATIONS); do
    # Stop merges so we accumulate several parts.
    $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${TABLE}"

    # Insert a few small parts.
    for j in $(seq 1 3); do
        ROW=$(( (i - 1) * 3 + j ))
        $CLICKHOUSE_CLIENT -q "
            INSERT INTO ${TABLE}
            SELECT number + ${ROW} * 1000,
                   'a','b','c','d','e','f','g','h','i','j','k','l'
            FROM numbers(100)
        "
    done

    # Rapidly toggle STOP / START to maximise the chance of hitting the
    # TOCTOU window inside MergeTask::executeImpl.  Background merges are
    # triggered automatically each time merges are re-enabled.
    for _k in $(seq 1 5); do
        $CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES ${TABLE}" &>/dev/null
        $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES ${TABLE}" &>/dev/null
    done
done

# Final optimize to make sure all parts are merged and data is consistent.
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE ${TABLE} FINAL" 2>/dev/null

# Verify no new LOGICAL_ERROR appeared.
errors_after=$($CLICKHOUSE_CLIENT -q "
    SELECT value FROM system.errors WHERE name = 'LOGICAL_ERROR'
" 2>/dev/null)
errors_after=${errors_after:-0}

if [ "$errors_after" -gt "$errors_before" ]; then
    echo "FAIL: new LOGICAL_ERROR detected during vertical merge stop/start race"
else
    echo "OK"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
