#!/usr/bin/env bash
# Test: DELETE FROM requires ALTER DELETE privilege; denied without it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Suppress server log forwarding for all calls: LWD mutations and ACCESS_DENIED
# errors emit WARNING/ERROR-level server log messages that get forwarded to
# --client_logs_file and trigger the "having stderror" check.
# Replace the log level in-place (appending doesn't work; --allow_repeated_settings
# applies to query settings only, not client-level options like --send_logs_level).
CLICKHOUSE_CLIENT=$(echo "${CLICKHOUSE_CLIENT}" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=fatal/g")

# Use unique user names per test run to avoid conflicts in parallel execution
USER_NO_PRIV="lwd_no_priv_${CLICKHOUSE_DATABASE}"
USER_WITH_PRIV="lwd_with_priv_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_lwd_rbac;
DROP USER IF EXISTS '${USER_NO_PRIV}';
DROP USER IF EXISTS '${USER_WITH_PRIV}';

CREATE TABLE t_lwd_rbac (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_lwd_rbac SELECT number, toString(number) FROM numbers(100);

CREATE USER '${USER_NO_PRIV}' IDENTIFIED WITH plaintext_password BY 'test123';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_lwd_rbac TO '${USER_NO_PRIV}';

CREATE USER '${USER_WITH_PRIV}' IDENTIFIED WITH plaintext_password BY 'test123';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_lwd_rbac TO '${USER_WITH_PRIV}';
GRANT ALTER DELETE ON ${CLICKHOUSE_DATABASE}.t_lwd_rbac TO '${USER_WITH_PRIV}';
"

# User without ALTER DELETE privilege must be denied
${CLICKHOUSE_CLIENT} --user "${USER_NO_PRIV}" --password test123 -q "
DELETE FROM ${CLICKHOUSE_DATABASE}.t_lwd_rbac WHERE a < 10;
" 2>&1 | grep -o "ACCESS_DENIED" | head -1

# User with ALTER DELETE privilege must succeed
${CLICKHOUSE_CLIENT} --user "${USER_WITH_PRIV}" --password test123 -q "
DELETE FROM ${CLICKHOUSE_DATABASE}.t_lwd_rbac WHERE a < 10;
"

${CLICKHOUSE_CLIENT} -q "
SELECT count() = 90 FROM ${CLICKHOUSE_DATABASE}.t_lwd_rbac;
REVOKE ALTER DELETE ON ${CLICKHOUSE_DATABASE}.t_lwd_rbac FROM '${USER_WITH_PRIV}';
"

# After revoke: same user must be denied
${CLICKHOUSE_CLIENT} --user "${USER_WITH_PRIV}" --password test123 -q "
DELETE FROM ${CLICKHOUSE_DATABASE}.t_lwd_rbac WHERE a < 20;
" 2>&1 | grep -o "ACCESS_DENIED" | head -1

${CLICKHOUSE_CLIENT} -q "
DROP USER '${USER_NO_PRIV}';
DROP USER '${USER_WITH_PRIV}';
DROP TABLE ${CLICKHOUSE_DATABASE}.t_lwd_rbac;
"
