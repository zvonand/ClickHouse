#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Use unique directory name
TEST_DIR="03902_filesystem_test_${CLICKHOUSE_DATABASE}"

# Get the actual user_files path from the server
USER_FILES_DIR=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.server_settings WHERE name = 'user_files_path'" 2>/dev/null)
# Resolve relative path: use the server's working directory
REAL_USER_FILES=$($CLICKHOUSE_CLIENT -q "SELECT concat(if(substring('${USER_FILES_DIR}', 1, 1) = '/', '', currentDatabase()), '${USER_FILES_DIR}')" 2>/dev/null)

# Extract the actual user_files directory from an error message (reliable way)
REAL_USER_FILES=$($CLICKHOUSE_CLIENT -q "SELECT _path FROM file('nonexist_03902_probe', 'CSV', 'c String') LIMIT 0" 2>&1 | grep -oP '/\S+/user_files' | head -1)

# Clean up before test
rm -rf "${REAL_USER_FILES:?}/${TEST_DIR}"

# Create test files
mkdir -p "${REAL_USER_FILES}/${TEST_DIR}/subdir"
echo -n 'hello' > "${REAL_USER_FILES}/${TEST_DIR}/a.txt"
echo -n 'world' > "${REAL_USER_FILES}/${TEST_DIR}/b.txt"
echo -n 'nested' > "${REAL_USER_FILES}/${TEST_DIR}/subdir/c.txt"

# List files and check basic columns
$CLICKHOUSE_CLIENT --query "
    SELECT name, type, is_symlink
    FROM filesystem('${TEST_DIR}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt', 'subdir')
    ORDER BY name
"

# Check size column for regular files
$CLICKHOUSE_CLIENT --query "
    SELECT name, size
    FROM filesystem('${TEST_DIR}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt')
    ORDER BY name
"

# Check content column
$CLICKHOUSE_CLIENT --query "
    SELECT name, content
    FROM filesystem('${TEST_DIR}')
    WHERE name IN ('a.txt', 'b.txt', 'c.txt')
    ORDER BY name
"

# LIMIT works
$CLICKHOUSE_CLIENT --query "
    SELECT name
    FROM filesystem('${TEST_DIR}')
    WHERE type = 'regular'
    ORDER BY name
    LIMIT 1
"

# Check that content is NULL for directories
$CLICKHOUSE_CLIENT --query "
    SELECT name, content IS NULL
    FROM filesystem('${TEST_DIR}')
    WHERE name = 'subdir'
"

# Clean up
rm -rf "${REAL_USER_FILES:?}/${TEST_DIR}"
