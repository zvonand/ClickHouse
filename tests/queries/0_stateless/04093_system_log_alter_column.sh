#!/usr/bin/env bash
# Tags: no-parallel
# Verify that system log tables can be flushed after ALTER TABLE adds a column.
# This reproduces the bug where SystemLog::flushImpl would fail with
# "Invalid number of columns in chunk pushed to OutputPort" because the INSERT
# did not specify an explicit column list.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test with processors_profile_log
$CLICKHOUSE_CLIENT -q "SET log_processors_profiles = 1; SELECT 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS processors_profile_log"

$CLICKHOUSE_CLIENT -q "ALTER TABLE system.processors_profile_log ADD COLUMN IF NOT EXISTS extra_test_column UInt64 DEFAULT 0"

$CLICKHOUSE_CLIENT -q "SET log_processors_profiles = 1; SELECT 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS processors_profile_log"

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.processors_profile_log"

# Cleanup: DROP COLUMN may fail if materialized views reference the table, ignore the error.
$CLICKHOUSE_CLIENT -q "ALTER TABLE system.processors_profile_log DROP COLUMN IF EXISTS extra_test_column" 2>/dev/null || true

# Test with query_log
$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "ALTER TABLE system.query_log ADD COLUMN IF NOT EXISTS extra_test_column UInt64 DEFAULT 0"

$CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.query_log"

# Cleanup
$CLICKHOUSE_CLIENT -q "ALTER TABLE system.query_log DROP COLUMN IF EXISTS extra_test_column" 2>/dev/null || true
