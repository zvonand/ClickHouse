#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# https://github.com/ClickHouse/ClickHouse/issues/100614
#
# When session_timezone is set after CREATE TABLE, the DataTypeDateTime
# stored in table metadata has the server default timezone baked in.
# Inserts that parse DateTime from text on the server side must still
# respect the current session_timezone, not the one from table creation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="test_async_tz_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${TABLE} (d DateTime) ENGINE = Memory"

# ── TCP (clickhouse-client) ──────────────────────────────────────────────
# The client parses VALUES locally, so session_timezone is applied on the
# client side.  Both sync and async paths send the already-parsed numeric
# value; the server just stores it.  This is the baseline.

echo "--- TCP sync"
${CLICKHOUSE_CLIENT} --session_timezone='Asia/Novosibirsk' -q \
    "INSERT INTO ${TABLE} SETTINGS async_insert=0 VALUES ('2000-01-01 01:00:00')"
${CLICKHOUSE_CLIENT} --session_timezone='Asia/Novosibirsk' -q \
    "SELECT toUnixTimestamp(d) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

echo "--- TCP async"
${CLICKHOUSE_CLIENT} --session_timezone='Asia/Novosibirsk' -q \
    "INSERT INTO ${TABLE} SETTINGS async_insert=1, wait_for_async_insert=1 VALUES ('2000-01-01 01:00:00')"
${CLICKHOUSE_CLIENT} --session_timezone='Asia/Novosibirsk' -q \
    "SELECT toUnixTimestamp(d) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

# ── HTTP ─────────────────────────────────────────────────────────────────
# Over HTTP the server parses VALUES text itself.  session_timezone must
# be respected during that parsing.

# Strip any randomized session_timezone from the base URL.
CLEAN_URL=$(echo "${CLICKHOUSE_URL}" \
    | sed 's/\&session_timezone=[A-Za-z0-9\/\%\_\-\+]*//g' \
    | sed 's/\?session_timezone=[A-Za-z0-9\/\%\_\-\+]*\&/\?/g')

URL_TZ="${CLEAN_URL}&session_timezone=Asia%2FNovosibirsk"

echo "--- HTTP sync"
${CLICKHOUSE_CURL} -sS "${URL_TZ}" -d \
    "INSERT INTO ${TABLE} SETTINGS async_insert=0 VALUES ('2000-01-01 01:00:00')"
${CLICKHOUSE_CURL} -sS "${URL_TZ}" -d \
    "SELECT toUnixTimestamp(d) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

echo "--- HTTP async"
${CLICKHOUSE_CURL} -sS "${URL_TZ}&async_insert=1&wait_for_async_insert=1" -d \
    "INSERT INTO ${TABLE} VALUES ('2000-01-01 01:00:00')"
${CLICKHOUSE_CURL} -sS "${URL_TZ}" -d \
    "SELECT toUnixTimestamp(d) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${TABLE}"
