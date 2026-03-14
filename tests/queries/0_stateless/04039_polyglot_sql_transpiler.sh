#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

POLYGLOT_OPTS="--allow_experimental_polyglot_dialect 1 --dialect polyglot"

# SQLite: GROUP_CONCAT is an SQLite-specific aggregate function
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect sqlite \
    -q "SELECT GROUP_CONCAT(x, ',') FROM (SELECT 'a' AS x UNION ALL SELECT 'b' UNION ALL SELECT 'c')"

# MySQL: backtick-quoted identifiers, including reserved words as column names
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect mysql \
    -q 'SELECT `select` FROM (SELECT 1 AS `select`) AS t'

# PostgreSQL: :: cast operator and POSITION(x IN y) syntax
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect postgresql \
    -q "SELECT 42::INTEGER, POSITION('world' IN 'hello world')"

# Snowflake: IFF (Snowflake-specific conditional) and DATEADD
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect snowflake \
    -q "SELECT IFF(1 > 0, 'yes', 'no')"

# DuckDB: TRY_CAST
$CLICKHOUSE_CLIENT $POLYGLOT_OPTS --polyglot_dialect duckdb \
    -q "SELECT TRY_CAST('123' AS INTEGER)"

# Test that polyglot dialect requires the experimental setting
$CLICKHOUSE_CLIENT --dialect polyglot --polyglot_dialect sqlite -q "SELECT 1" 2>&1 | grep -o "SUPPORT_IS_DISABLED"
