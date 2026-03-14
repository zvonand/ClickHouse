#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/99326
# Avro output should throw BAD_ARGUMENTS instead of logical error
# when an Enum column contains a value not in the enum definition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test Enum8: insert valid value, then narrow enum definition via ALTER.
# The value survives the ALTER unchecked.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS enum8_avro_test"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum8_avro_test (e Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO enum8_avro_test VALUES ('c')"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE enum8_avro_test MODIFY COLUMN e Enum8('a' = 1, 'b' = 2)"

# Avro output writes a binary header to stdout before failing, so discard stdout.
# The error message goes to stderr. Check that it contains BAD_ARGUMENTS.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum8_avro_test FORMAT Avro" > /dev/null 2>&1 && echo "UNEXPECTED_OK" || echo "OK"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum8_avro_test FORMAT Avro" > /dev/null 2> "${CLICKHOUSE_TMP}/avro_enum8_err.txt"
grep -q 'BAD_ARGUMENTS' "${CLICKHOUSE_TMP}/avro_enum8_err.txt" && echo "BAD_ARGUMENTS" || cat "${CLICKHOUSE_TMP}/avro_enum8_err.txt"

${CLICKHOUSE_CLIENT} -q "DROP TABLE enum8_avro_test"

# Test Enum16: same approach
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS enum16_avro_test"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE enum16_avro_test (e Enum16('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO enum16_avro_test VALUES ('c')"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE enum16_avro_test MODIFY COLUMN e Enum16('a' = 1, 'b' = 2)"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum16_avro_test FORMAT Avro" > /dev/null 2>&1 && echo "UNEXPECTED_OK" || echo "OK"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM enum16_avro_test FORMAT Avro" > /dev/null 2> "${CLICKHOUSE_TMP}/avro_enum16_err.txt"
grep -q 'BAD_ARGUMENTS' "${CLICKHOUSE_TMP}/avro_enum16_err.txt" && echo "BAD_ARGUMENTS" || cat "${CLICKHOUSE_TMP}/avro_enum16_err.txt"

${CLICKHOUSE_CLIENT} -q "DROP TABLE enum16_avro_test"
