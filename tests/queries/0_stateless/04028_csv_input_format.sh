#!/usr/bin/env bash

# NOTE: this sh wrapper is required because of shell_config

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String)"
$CLICKHOUSE_CLIENT -q "SET input_format_csv_detect_header=true;
                       SET input_format_with_names_case_insensitive_column_matching='auto';
                       INSERT INTO test FROM INFILE '$CURDIR/data_csv/csv_with_names.csv' FORMAT CSVWithNames;"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String)"
$CLICKHOUSE_CLIENT -q "SET input_format_csv_detect_header=true;
                       SET input_format_with_names_case_insensitive_column_matching='match_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_csv/csv_with_names.csv' FORMAT CSVWithNames"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test"
$CLICKHOUSE_CLIENT -q "CREATE TABLE test (id Int, ID Int, name String)"
$CLICKHOUSE_CLIENT -q "SET input_format_csv_detect_header=true;
                       SET input_format_with_names_case_insensitive_column_matching='ignore_case';
                       INSERT INTO test FROM INFILE '$CURDIR/data_csv/csv_with_names.csv' FORMAT CSVWithNames; -- { clientError 117 }"
$CLICKHOUSE_CLIENT -q "SELECT * FROM test"
$CLICKHOUSE_CLIENT -q "DROP TABLE test"