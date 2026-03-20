#!/usr/bin/env bash
# Test that clickhouse-local can cancel scalar subqueries via SIGINT.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Start a query with an infinite scalar subquery
${CLICKHOUSE_LOCAL} --query="SELECT (SELECT max(number) FROM system.numbers) + 1 SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0" >/dev/null 2>&1 &
local_pid=$!

sleep 0.5

# Send SIGINT to cancel the query
kill -INT $local_pid 2>/dev/null

# Wait with a timeout — if cancellation works, it should exit quickly
for _ in {0..20}
do
    if ! kill -0 $local_pid 2>/dev/null; then
        echo "CANCELLED"
        exit 0
    fi
    sleep 0.1
done

# If still running after 2 seconds, cancellation failed
kill -9 $local_pid 2>/dev/null
wait $local_pid 2>/dev/null
echo "HUNG"
