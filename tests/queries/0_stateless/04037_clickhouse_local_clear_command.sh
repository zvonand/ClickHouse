#!/usr/bin/env bash
# Test the `clear` client metacommand (clickhouse-local / same ClientBase path as clickhouse-client).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -o errexit

echo "-- clear variants exit successfully"
$CLICKHOUSE_LOCAL -q "clear"
$CLICKHOUSE_LOCAL -q "CLEAR"
$CLICKHOUSE_LOCAL -q "/clear"
$CLICKHOUSE_LOCAL -q "/CLEAR;"
echo "OK"

echo "-- clear is not parsed as a bare SQL identifier"
if err="$($CLICKHOUSE_LOCAL -q "clear" 2>&1)"; then
    if echo "$err" | grep -qF 'UNKNOWN_IDENTIFIER'; then
        echo "FAIL: clear was interpreted as SQL: $err" >&2
        exit 1
    fi
else
    echo "FAIL: clear command returned non-zero" >&2
    exit 1
fi
echo "OK"

echo "-- select clear is still SQL"
$CLICKHOUSE_LOCAL -q "select clear" 2>&1 | grep -F 'UNKNOWN_IDENTIFIER' > /dev/null
echo "OK"
