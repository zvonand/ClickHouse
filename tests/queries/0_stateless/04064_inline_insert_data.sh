#!/usr/bin/env bash
# Tags: no-fasttest

# Test that INSERT with inline data works when the --inline-insert-data flag is used.
# In this mode, the client sends the data as is in the query text instead of converting it to blocks,
# and the server parses the inline data itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_inline_insert"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_inline_insert (x UInt64, y String) ENGINE = MergeTree ORDER BY x"

# Test with --inline-insert-data flag
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert VALUES (1, 'hello'), (2, 'world')"
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT Values (3, 'foo')"
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT JSONEachRow {\"x\": 4, \"y\": \"bar\"}"

# Test with send_table_structure_on_insert_with_inline_data setting directly (without --inline-insert-data)
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert VALUES (5, 'baz')"

$CLICKHOUSE_CLIENT --query "SELECT * FROM test_inline_insert ORDER BY x"

$CLICKHOUSE_CLIENT --query "DROP TABLE test_inline_insert"
