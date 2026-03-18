#!/usr/bin/env bash
# Test the `ls` command in clickhouse-local.
#
# `ls` is a client-side meta-command (not SQL). It lists files in the current
# working directory and is internally rewritten to a query using `file()`.
#
# The test checks:
# - `ls` works
# - `ls;` works
# - `ls` with arguments is not supported and results in an error.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test ls command
echo "-- ls"
(
    cd "$CURDIR"
    test "$($CLICKHOUSE_LOCAL -q "ls" 2>/dev/null | wc -l)" -gt 0
)
echo "OK"

echo "-- ls;"
(
    cd "$CURDIR"
    test "$($CLICKHOUSE_LOCAL -q "ls;" 2>/dev/null | wc -l)" -gt 0
)
echo "OK"

echo "-- ls foo (should fail)"
(
    cd "$CURDIR"
    $CLICKHOUSE_LOCAL -q "ls foo" 2>&1 || true
) | grep -F "Unknown expression identifier"