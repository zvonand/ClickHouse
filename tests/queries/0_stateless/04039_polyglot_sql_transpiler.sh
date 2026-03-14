#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: polyglot requires Rust build

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test SQLite dialect
$CLICKHOUSE_CLIENT --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect sqlite -q "SELECT IFNULL(1, 2)"

# Test MySQL dialect
$CLICKHOUSE_CLIENT --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect mysql -q "SELECT IFNULL(1, 2)"

# Test PostgreSQL dialect
$CLICKHOUSE_CLIENT --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect postgresql -q "SELECT COALESCE(1, 2)"

# Test DuckDB dialect
$CLICKHOUSE_CLIENT --allow_experimental_polyglot_dialect 1 --dialect polyglot --polyglot_dialect duckdb -q "SELECT COALESCE(1, 2)"

# Test that polyglot dialect requires the experimental setting
$CLICKHOUSE_CLIENT --dialect polyglot --polyglot_dialect sqlite -q "SELECT 1" 2>&1 | grep -o "SUPPORT_IS_DISABLED"
