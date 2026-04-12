#!/usr/bin/env bash

# Test url_base setting for resolving relative URLs.
# The queries will fail with connection errors, but the debug-level log from
# ReadWriteBufferFromHTTP contains the resolved URL in the message
# "Failed to make request to '<resolved_url>'".
# All domains use .invalid TLD (RFC 2606) to ensure DNS failure.
# We use --send_logs_level=debug to capture the log line with the resolved URL.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Common settings to avoid slow DNS resolution retries.
FAST="http_connection_timeout = 1, http_max_tries = 1"

# Helper: run a query with debug logs enabled and extract the resolved URL.
# The ReadWriteBufferFromHTTP debug log contains "Failed to make request to '<url>'".
run_and_check() {
    local query="$1"
    local expected="$2"
    $CLICKHOUSE_CLIENT --send_logs_level=debug --query "$query" 2>&1 | grep -oF "$expected" | head -1
}

# Path-relative URL: data.csv with base https://base.invalid/def/ → https://base.invalid/def/data.csv
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/', $FAST" 'https://base.invalid/def/data.csv'

# Host-relative URL: /test/data.csv with base https://base.invalid/def/ → https://base.invalid/test/data.csv
run_and_check "SELECT * FROM url('/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/', $FAST" 'https://base.invalid/test/data.csv'

# Scheme-relative URL: //other.invalid/test/data.csv with base https://base.invalid/def/ → https://other.invalid/test/data.csv
run_and_check "SELECT * FROM url('//other.invalid/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/', $FAST" 'https://other.invalid/test/data.csv'

# Absolute URL (url_base should be ignored): https://other.invalid/absolute.csv remains as-is
run_and_check "SELECT * FROM url('https://other.invalid/absolute.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/', $FAST" 'https://other.invalid/absolute.csv'

# Path-relative without trailing slash: data.csv with base https://base.invalid/def → https://base.invalid/data.csv (RFC 3986 merge)
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def', $FAST" 'https://base.invalid/data.csv'

# url_base with query string should be handled correctly
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?token=123', $FAST" 'https://base.invalid/def/data.csv'

# url_base without path: https://base.invalid + /test → https://base.invalid/test
run_and_check "SELECT * FROM url('/test', CSV, 'c String') SETTINGS url_base = 'https://base.invalid', $FAST" 'https://base.invalid/test'

# Scheme-relative URL with http base
run_and_check "SELECT * FROM url('//other.invalid/other.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/path/', $FAST" 'http://other.invalid/other.csv'

# url_base with query string containing special characters (slashes, colons)
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?redirect=http://other.invalid/path&x=1', $FAST" 'https://base.invalid/def/data.csv'

# url_base with fragment identifier containing special characters
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/#section/sub/http://foo', $FAST" 'https://base.invalid/def/data.csv'

# Host-relative URL with url_base that has a query string with slashes
run_and_check "SELECT * FROM url('/other/file.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?redirect=http://x.invalid/', $FAST" 'https://base.invalid/other/file.csv'

# Path-relative URL with authority-only base (no path): https://base.invalid + data.csv → https://base.invalid/data.csv
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid', $FAST" 'https://base.invalid/data.csv'

# Query-only relative reference: ?x=1 with base https://base.invalid/dir/file.csv → https://base.invalid/dir/file.csv?x=1
run_and_check "SELECT * FROM url('?x=1', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv', $FAST" 'https://base.invalid/dir/file.csv?x=1'

# Fragment-only relative reference: #frag with base https://base.invalid/dir/file.csv → https://base.invalid/dir/file.csv#frag
run_and_check "SELECT * FROM url('#frag', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv', $FAST" 'https://base.invalid/dir/file.csv#frag'

# Fragment-only reference with base that has a query string: query must be preserved
run_and_check "SELECT * FROM url('#frag', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv?token=abc', $FAST" 'https://base.invalid/dir/file.csv?token=abc#frag'

# Path-relative URL with embedded absolute URL in query parameter (should not be treated as absolute)
run_and_check "SELECT * FROM url('data.csv?next=https://other.invalid/a', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/', $FAST" 'https://base.invalid/dir/data.csv?next=https://other.invalid/a'

# Path-relative URL with dot-segment (../) — normalized per RFC 3986
run_and_check "SELECT * FROM url('../a.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/', $FAST" 'https://base.invalid/a.csv'

# Path-relative URL with dot-segment (./) — normalized per RFC 3986
run_and_check "SELECT * FROM url('./a.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/', $FAST" 'https://base.invalid/dir/a.csv'

# Multiple dot-segments: ../../ from a deeper path
run_and_check "SELECT * FROM url('../../a.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/a/b/c/', $FAST" 'https://base.invalid/a/a.csv'

# Dot-segment with query parameter in the relative URL
run_and_check "SELECT * FROM url('../a.csv?foo=bar', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/', $FAST" 'https://base.invalid/a.csv?foo=bar'

# URL engine: path-relative URL with url_base should resolve correctly
$CLICKHOUSE_CLIENT --send_logs_level=debug -n -q "
SET url_base = 'https://base.invalid/dir/', http_connection_timeout = 1, http_max_tries = 1;
CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_url (c String) ENGINE = URL('data.csv', CSV);
SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_url;
" 2>&1 | grep -oF 'https://base.invalid/dir/data.csv' | head -1
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_url" 2>/dev/null

# Empty relative reference: should return base without fragment
run_and_check "SELECT * FROM url('', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv?token=abc#frag', $FAST" 'https://base.invalid/dir/file.csv?token=abc'

# Invalid url_base (no scheme) should produce an error
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'example.invalid/def/', $FAST" 2>&1 | grep -oF 'must contain a scheme'
