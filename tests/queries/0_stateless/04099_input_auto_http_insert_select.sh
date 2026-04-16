#!/usr/bin/env bash
# Tags: no-parallel-replicas

# Regression test: input('auto') in INSERT ... SELECT via HTTP used to fail
# with CANNOT_EXTRACT_TABLE_STRUCTURE because setInsertionTable() had not been
# called yet when the early input() resolution ran for HTTP requests.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_source_04099"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS t_target_04099"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_source_04099 (a UInt32, b String) ENGINE=Memory"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE t_target_04099 (a UInt32, b String, extra String) ENGINE=Memory"

${CLICKHOUSE_CLIENT} --query="INSERT INTO t_source_04099 VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')"

# Export source rows in Native format (only columns a, b).
${CLICKHOUSE_CLIENT} --query="SELECT * FROM t_source_04099 ORDER BY a FORMAT Native" > "${CLICKHOUSE_TMP}"/04099_native_data.bin

# INSERT via HTTP using input('auto') with an extra constant column.
# This is the exact pattern that failed before the fix.
cat "${CLICKHOUSE_TMP}"/04099_native_data.bin | ${CLICKHOUSE_CURL} -sS \
    "${CLICKHOUSE_URL}&query=INSERT+INTO+t_target_04099+SELECT+*,%27const%27+AS+extra+FROM+input(%27auto%27)+FORMAT+Native" \
    --data-binary @-

${CLICKHOUSE_CLIENT} --query="SELECT * FROM t_target_04099 ORDER BY a"

${CLICKHOUSE_CLIENT} --query="DROP TABLE t_source_04099"
${CLICKHOUSE_CLIENT} --query="DROP TABLE t_target_04099"
