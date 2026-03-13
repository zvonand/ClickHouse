#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/99326
# Avro output should throw BAD_ARGUMENTS instead of logical error
# when an Enum column contains a value not in the enum definition.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create a table with a wide Enum8 definition and insert 'c' (value = 3)
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.enum_wide (e Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.enum_wide VALUES ('c')"

# Create a table with a narrow Enum8 definition (missing 'c' = 3)
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.enum_narrow (e Enum8('a' = 1, 'b' = 2)) ENGINE = Memory"

# Pipe data via RowBinary: binary deserialization of Enum8 is inherited from
# SerializationNumber<Int8> and does NOT validate that the value is in the
# enum definition, so value 3 is inserted into enum_narrow unchecked.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.enum_wide FORMAT RowBinary" | \
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.enum_narrow FORMAT RowBinary"

# Avro output must produce BAD_ARGUMENTS, not a logical error (std::out_of_range)
# Redirect stdout to /dev/null so binary Avro header bytes don't corrupt grep
${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.enum_narrow FORMAT Avro" 2>&1 >/dev/null | grep -o 'BAD_ARGUMENTS'

# Same test for Enum16
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.enum16_wide (e Enum16('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.enum16_wide VALUES ('c')"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.enum16_narrow (e Enum16('a' = 1, 'b' = 2)) ENGINE = Memory"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.enum16_wide FORMAT RowBinary" | \
    ${CLICKHOUSE_CLIENT} -q "INSERT INTO ${CLICKHOUSE_DATABASE}.enum16_narrow FORMAT RowBinary"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${CLICKHOUSE_DATABASE}.enum16_narrow FORMAT Avro" 2>&1 >/dev/null | grep -o 'BAD_ARGUMENTS'

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.enum_wide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.enum_narrow"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.enum16_wide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.enum16_narrow"
