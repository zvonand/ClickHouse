#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Parametric grantees syntax is not supported
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER} GRANTEES {grantees:Identifier}" 2>&1 | grep -m1 -o 'SYNTAX_ERROR'
# User must not have been created
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${USER}" 2>&1 | grep -m1 -o 'UNKNOWN_USER'
