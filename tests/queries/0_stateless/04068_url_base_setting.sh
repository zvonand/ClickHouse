#!/usr/bin/env bash

# Test url_base setting for resolving relative URLs.
# The queries will fail with connection errors, but the error messages contain the resolved URLs.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Path-relative URL: data.csv with base https://example.com/def/ → https://example.com/def/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/'" 2>&1 | grep -oF 'https://example.com/def/data.csv'

# Host-relative URL: /test/data.csv with base https://example.com/def/ → https://example.com/test/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/'" 2>&1 | grep -oF 'https://example.com/test/data.csv'

# Scheme-relative URL: //example.invalid/test/data.csv with base https://example.com/def/ → https://example.invalid/test/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('//example.invalid/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/'" 2>&1 | grep -oF 'https://example.invalid/test/data.csv'

# Absolute URL (url_base should be ignored): https://example.invalid/absolute.csv remains as-is
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('https://example.invalid/absolute.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/'" 2>&1 | grep -oF 'https://example.invalid/absolute.csv'

# Path-relative without trailing slash: data.csv with base https://example.com/def → https://example.com/data.csv (RFC 3986 merge)
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def'" 2>&1 | grep -oF 'https://example.com/data.csv'

# url_base with query string should be handled correctly
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/?token=123'" 2>&1 | grep -oF 'https://example.com/def/data.csv'

# url_base without path: https://example.com + /test → https://example.com/test
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/test', CSV, 'c String') SETTINGS url_base = 'https://example.com'" 2>&1 | grep -oF 'https://example.com/test'

# Scheme-relative URL with http base
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('//example.invalid/other.csv', CSV, 'c String') SETTINGS url_base = 'http://example.com/path/'" 2>&1 | grep -oF 'http://example.invalid/other.csv'

# url_base with query string containing special characters (slashes, colons)
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/?redirect=http://other.com/path&x=1'" 2>&1 | grep -oF 'https://example.com/def/data.csv'

# url_base with fragment identifier containing special characters
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/#section/sub/http://foo'" 2>&1 | grep -oF 'https://example.com/def/data.csv'

# Host-relative URL with url_base that has a query string with slashes
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/other/file.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com/def/?redirect=http://x.com/'" 2>&1 | grep -oF 'https://example.com/other/file.csv'

# Path-relative URL with authority-only base (no path): https://example.com + data.csv → https://example.com/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://example.com'" 2>&1 | grep -oF 'https://example.com/data.csv'

# Invalid url_base (no scheme) should produce an error
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'example.com/def/'" 2>&1 | grep -oF 'must contain a scheme'
