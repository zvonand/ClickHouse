#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ROLE="role_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_ALLOWED="u_allowed_${CLICKHOUSE_TEST_UNIQUE_NAME}"
USER_DENIED="u_denied_${CLICKHOUSE_TEST_UNIQUE_NAME}"

url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=1"

${CLICKHOUSE_CLIENT} -n -q "
    DROP USER IF EXISTS ${USER_ALLOWED};
    DROP USER IF EXISTS ${USER_DENIED};
    DROP ROLE IF EXISTS ${ROLE};
    CREATE ROLE ${ROLE};
    CREATE USER ${USER_ALLOWED};
    CREATE USER ${USER_DENIED};
    REVOKE ALL ON *.* FROM ${USER_ALLOWED};
    REVOKE ALL ON *.* FROM ${USER_DENIED};
    DROP TABLE IF EXISTS async_inserts;
    CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    GRANT INSERT ON async_inserts TO ${ROLE};
    GRANT ${ROLE} to ${USER_ALLOWED};
"

${CLICKHOUSE_CURL} -sS "$url&user=${USER_DENIED}" \
    -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 1, "s": "a"} {"id": 2, "s": "b"}' \
    | grep -o "Not enough privileges"

${CLICKHOUSE_CURL} -sS "$url&user=${USER_ALLOWED}" \
    -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 1, "s": "a"} {"id": 2, "s": "b"}'

${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts ORDER BY id"

${CLICKHOUSE_CLIENT} -n -q "
    DROP TABLE async_inserts;
    DROP USER IF EXISTS ${USER_ALLOWED};
    DROP USER IF EXISTS ${USER_DENIED};
    DROP ROLE IF EXISTS ${ROLE};
"
