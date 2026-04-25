#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: creates and detaches a database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e -o pipefail

DB="db_04117_${CLICKHOUSE_DATABASE}"
SELECT_QID="select_04117_${CLICKHOUSE_DATABASE}"
DETACH_QID="detach_04117_${CLICKHOUSE_DATABASE}"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$DETACH_QID' SYNC FORMAT Null" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$SELECT_QID' SYNC FORMAT Null" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "ATTACH DATABASE IF NOT EXISTS \`$DB\`" 2>/dev/null || true
    $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 --query "DROP DATABASE IF EXISTS \`$DB\` SYNC" 2>/dev/null || true
}
trap cleanup EXIT

wait_for_query_to_start()
{
    local qid="$1"
    local timeout=30
    local start=$EPOCHSECONDS
    while [[ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$qid'")" == "0" ]]; do
        if (( EPOCHSECONDS - start > timeout )); then
            echo "Timeout waiting for query $qid to start" >&2
            exit 1
        fi
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS \`$DB\` SYNC"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE \`$DB\` ENGINE = Atomic"
$CLICKHOUSE_CLIENT --query "CREATE TABLE \`$DB\`.t (x UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "INSERT INTO \`$DB\`.t SELECT number FROM numbers(10)"

# Start a long-running SELECT that holds a reference to the table's StoragePtr via a snapshot.
# This keeps `waitDetachedTableNotInUse` busy-waiting indefinitely.
$CLICKHOUSE_CLIENT --query_id "$SELECT_QID" \
    --query "SELECT count() FROM \`$DB\`.t WHERE NOT ignore(sleepEachRow(3))" >/dev/null 2>&1 &

wait_for_query_to_start "$SELECT_QID"

# Trigger synchronous DETACH DATABASE: it will reach `waitDetachedTableNotInUse` and busy-wait
# because the SELECT above keeps an extra `shared_ptr` reference to the table.
$CLICKHOUSE_CLIENT --query_id "$DETACH_QID" \
    --database_atomic_wait_for_drop_and_detach_synchronously=1 \
    --query "DETACH DATABASE \`$DB\`" >/dev/null 2>&1 &
DETACH_BG_PID=$!

wait_for_query_to_start "$DETACH_QID"

# Cancel the synchronous DETACH. With the fix, the busy-wait observes the cancellation
# on its next iteration and the query exits promptly. Without the fix, the busy-wait
# loop ignores `KILL QUERY` and the DETACH would hang until the SELECT finishes.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$DETACH_QID' SYNC FORMAT Null"

# Allow up to a few seconds for the cancelled DETACH client to exit, but well under
# the SELECT's total sleep budget (10 rows * 3 seconds = 30 seconds).
detach_exited=0
for _ in $(seq 1 100); do
    if ! kill -0 "$DETACH_BG_PID" 2>/dev/null; then
        detach_exited=1
        break
    fi
    sleep 0.1
done

if (( detach_exited == 1 )); then
    echo "DETACH cancelled"
else
    echo "DETACH still running after KILL QUERY"
fi

wait "$DETACH_BG_PID" 2>/dev/null || true
