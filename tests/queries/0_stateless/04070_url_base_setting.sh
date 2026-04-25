#!/usr/bin/env bash

# Test url_base setting for resolving relative URLs.
# The queries will fail with connection errors, but the debug-level log from
# ReadWriteBufferFromHTTP contains the resolved URL in the message
# "Failed to make request to '<resolved_url>'".
# All domains use .invalid TLD (RFC 2606) to ensure DNS failure.
# We use --send_logs_level=debug to capture the log line with the resolved URL.
# Use http:// (not https://) because the fast-test build has no SSL support
# and HTTPS requests would fail with SUPPORT_IS_DISABLED before the URL is logged.

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

# Path-relative URL: data.csv with base http://base.invalid/def/ → http://base.invalid/def/data.csv
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/', $FAST" 'http://base.invalid/def/data.csv'

# Host-relative URL: /test/data.csv with base http://base.invalid/def/ → http://base.invalid/test/data.csv
run_and_check "SELECT * FROM url('/test/data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/', $FAST" 'http://base.invalid/test/data.csv'

# Scheme-relative URL: //other.invalid/test/data.csv with base http://base.invalid/def/ → http://other.invalid/test/data.csv
run_and_check "SELECT * FROM url('//other.invalid/test/data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/', $FAST" 'http://other.invalid/test/data.csv'

# Absolute URL (url_base should be ignored): http://other.invalid/absolute.csv remains as-is
run_and_check "SELECT * FROM url('http://other.invalid/absolute.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/', $FAST" 'http://other.invalid/absolute.csv'

# Path-relative without trailing slash: data.csv with base http://base.invalid/def → http://base.invalid/data.csv (RFC 3986 merge)
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def', $FAST" 'http://base.invalid/data.csv'

# url_base with query string should be handled correctly
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/?token=123', $FAST" 'http://base.invalid/def/data.csv'

# url_base without path: http://base.invalid + /test → http://base.invalid/test
run_and_check "SELECT * FROM url('/test', CSV, 'c String') SETTINGS url_base = 'http://base.invalid', $FAST" 'http://base.invalid/test'

# Scheme-relative URL with http base
run_and_check "SELECT * FROM url('//other.invalid/other.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/path/', $FAST" 'http://other.invalid/other.csv'

# url_base with query string containing special characters (slashes, colons)
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/?redirect=http://other.invalid/path&x=1', $FAST" 'http://base.invalid/def/data.csv'

# url_base with fragment identifier containing special characters
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/#section/sub/http://foo', $FAST" 'http://base.invalid/def/data.csv'

# Host-relative URL with url_base that has a query string with slashes
run_and_check "SELECT * FROM url('/other/file.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/def/?redirect=http://x.invalid/', $FAST" 'http://base.invalid/other/file.csv'

# Path-relative URL with authority-only base (no path): http://base.invalid + data.csv → http://base.invalid/data.csv
run_and_check "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid', $FAST" 'http://base.invalid/data.csv'

# Query-only relative reference: ?x=1 with base http://base.invalid/dir/file.csv → http://base.invalid/dir/file.csv?x=1
run_and_check "SELECT * FROM url('?x=1', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/file.csv', $FAST" 'http://base.invalid/dir/file.csv?x=1'

# Fragment-only relative reference: #frag with base http://base.invalid/dir/file.csv.
# The HTTP buffer strips the fragment before logging the URL, so we look for the URL without the fragment.
run_and_check "SELECT * FROM url('#frag', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/file.csv', $FAST" 'http://base.invalid/dir/file.csv'

# Fragment-only reference with base that has a query string: query must be preserved.
# The fragment is stripped before logging, so we expect the URL with query but without fragment.
run_and_check "SELECT * FROM url('#frag', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/file.csv?token=abc', $FAST" 'http://base.invalid/dir/file.csv?token=abc'

# Path-relative URL with embedded absolute URL in query parameter (should not be treated as absolute)
run_and_check "SELECT * FROM url('data.csv?next=http://other.invalid/a', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/', $FAST" 'http://base.invalid/dir/data.csv?next=http://other.invalid/a'

# Path-relative URL with dot-segment (../) — normalized per RFC 3986
run_and_check "SELECT * FROM url('../a.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/', $FAST" 'http://base.invalid/a.csv'

# Path-relative URL with dot-segment (./) — normalized per RFC 3986
run_and_check "SELECT * FROM url('./a.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/', $FAST" 'http://base.invalid/dir/a.csv'

# Multiple dot-segments: ../../ from a deeper path
run_and_check "SELECT * FROM url('../../a.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/a/b/c/', $FAST" 'http://base.invalid/a/a.csv'

# Dot-segment with query parameter in the relative URL
run_and_check "SELECT * FROM url('../a.csv?foo=bar', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/', $FAST" 'http://base.invalid/a.csv?foo=bar'

# URL engine: path-relative URL with url_base should resolve correctly
$CLICKHOUSE_CLIENT --send_logs_level=debug -n -q "
SET url_base = 'http://base.invalid/dir/', http_connection_timeout = 1, http_max_tries = 1;
CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_url (c String) ENGINE = URL('data.csv', CSV);
SELECT * FROM ${CLICKHOUSE_TEST_UNIQUE_NAME}_url;
" 2>&1 | grep -oF 'http://base.invalid/dir/data.csv' | head -1
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_url" 2>/dev/null

# Empty relative reference: should return base without fragment
run_and_check "SELECT * FROM url('', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/dir/file.csv?token=abc#frag', $FAST" 'http://base.invalid/dir/file.csv?token=abc'

# URL engine: resolved URL must be materialized into engine args so the table survives
# DETACH/ATTACH (and server restart) after url_base is unset or changed.
$CLICKHOUSE_CLIENT -n -q "
SET url_base = 'http://base.invalid/dir/';
DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_url_persist;
CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_url_persist (c String) ENGINE = URL('persist.csv', CSV);
SET url_base = '';
SHOW CREATE TABLE ${CLICKHOUSE_TEST_UNIQUE_NAME}_url_persist;
DROP TABLE IF EXISTS ${CLICKHOUSE_TEST_UNIQUE_NAME}_url_persist;
" 2>&1 | grep -oF "URL('http://base.invalid/dir/persist.csv'" | head -1

# Invalid url_base (no scheme) should produce an error
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'example.invalid/def/', $FAST" 2>&1 | grep -oF 'must contain a scheme' | head -1
