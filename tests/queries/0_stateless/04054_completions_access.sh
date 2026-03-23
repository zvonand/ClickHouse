#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: create user

# Test that system.completions respects per-table grants correctly.
# A user with only per-table SHOW TABLES/SHOW COLUMNS grants (no global grant)
# should see completions for their permitted tables, not others.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS completions_access_db_04054;"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE completions_access_db_04054;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE completions_access_db_04054.visible_table (id UInt32, name String) ENGINE = Memory;"
$CLICKHOUSE_CLIENT --query "CREATE TABLE completions_access_db_04054.hidden_table (id UInt32, value Float64) ENGINE = Memory;"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS completions_access_user_04054;"
$CLICKHOUSE_CLIENT --query "CREATE USER completions_access_user_04054 IDENTIFIED WITH no_password;"
# Grant only per-table access (no global SHOW TABLES/SHOW COLUMNS grant)
$CLICKHOUSE_CLIENT --query "GRANT SHOW TABLES ON completions_access_db_04054.visible_table TO completions_access_user_04054;"
$CLICKHOUSE_CLIENT --query "GRANT SHOW COLUMNS ON completions_access_db_04054.visible_table TO completions_access_user_04054;"

echo "=== Per-table grant user sees the permitted table ==="
$CLICKHOUSE_CLIENT --user=completions_access_user_04054 \
    --query "SELECT word FROM system.completions WHERE context = 'table' AND belongs = 'completions_access_db_04054' ORDER BY word;"

echo "=== Per-table grant user sees columns of the permitted table ==="
$CLICKHOUSE_CLIENT --user=completions_access_user_04054 \
    --query "SELECT word FROM system.completions WHERE context = 'column' AND belongs = 'visible_table' ORDER BY word;"

echo "=== Per-table grant user does NOT see the hidden table ==="
$CLICKHOUSE_CLIENT --user=completions_access_user_04054 \
    --query "SELECT count() FROM system.completions WHERE context = 'table' AND word = 'hidden_table';"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS completions_access_user_04054;"

# Case A: per-db SHOW DATABASES grant controls context = 'database' rows
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS completions_db_user_04054;"
$CLICKHOUSE_CLIENT --query "CREATE USER completions_db_user_04054 IDENTIFIED WITH no_password;"
$CLICKHOUSE_CLIENT --query "GRANT SHOW DATABASES ON completions_access_db_04054.* TO completions_db_user_04054;"

echo "=== Per-db grant user sees the permitted database ==="
$CLICKHOUSE_CLIENT --user=completions_db_user_04054 \
    --query "SELECT word FROM system.completions WHERE context = 'database' AND word = 'completions_access_db_04054';"

echo "=== Per-db grant user does NOT see other databases ==="
$CLICKHOUSE_CLIENT --user=completions_db_user_04054 \
    --query "SELECT count() FROM system.completions WHERE context = 'database' AND word = 'default';"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS completions_db_user_04054;"

# Case B: external tables (session temporary tables) branch
echo "=== Temporary table appears in completions ==="
$CLICKHOUSE_CLIENT -nm --query "
CREATE TEMPORARY TABLE tmp_completions_04054 (x UInt32, y String);
SELECT word FROM system.completions WHERE context = 'table' AND word = 'tmp_completions_04054';
"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS completions_access_db_04054;"
