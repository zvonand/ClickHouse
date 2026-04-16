#!/usr/bin/env bash
# Tags: long, no-debug, no-asan, no-tsan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings
# Test is used by ci/tests

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

sleep_sql="SELECT sleep(3);"

$CLICKHOUSE_CLIENT --query="$sleep_sql $sleep_sql $sleep_sql $sleep_sql;" &
$CLICKHOUSE_CLIENT --query="$sleep_sql $sleep_sql $sleep_sql $sleep_sql;" &
$CLICKHOUSE_CLIENT --query="$sleep_sql $sleep_sql $sleep_sql $sleep_sql;" &
$CLICKHOUSE_CLIENT --query="$sleep_sql $sleep_sql $sleep_sql $sleep_sql;" &
$CLICKHOUSE_CLIENT --query="$sleep_sql $sleep_sql $sleep_sql $sleep_sql;" &
wait
