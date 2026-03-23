#!/usr/bin/env bash


# Verify that CRLF characters in user-supplied HTTP parameters (like query_id)
# cannot be used to inject additional HTTP response headers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "--- CRLF in query_id must not inject headers ---"
# Try injecting a Location header via query_id containing \r\n
${CLICKHOUSE_CURL} -sS --globoff -v "${CLICKHOUSE_URL}&query=SELECT+1&query_id=inject%0d%0aLocation:%20evil.com" 2>&1 \
    | grep -ci '< Location:' \
    | sed 's/^0$/No injected headers/' \
    | sed '/^[1-9]/s/.*/FAIL: Header injection detected/'

echo "--- CRLF in query_id via X-ClickHouse-Query-Id header ---"
${CLICKHOUSE_CURL} -sS --globoff -v "${CLICKHOUSE_URL}&query=SELECT+1" \
    -H "X-ClickHouse-Query-Id: inject$(printf '\r\n')Evil-Header: evil-value" 2>&1 \
    | grep -ci '< Evil-Header:' \
    | sed 's/^0$/No injected headers/' \
    | sed '/^[1-9]/s/.*/FAIL: Header injection detected/'

echo "--- Verify query still works with a normal query_id ---"
${CLICKHOUSE_CURL} -sS --globoff -v "${CLICKHOUSE_URL}&query=SELECT+42&query_id=normal_test_id_04055" 2>&1 \
    | grep '< X-ClickHouse-Query-Id:' \
    | sed 's/\r$//' \
    | sed 's/< X-ClickHouse-Query-Id: .*/query_id present/'
