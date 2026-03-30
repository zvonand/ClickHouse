#!/usr/bin/env bash

# Test url_base setting for resolving relative URLs.
# The queries will fail with connection errors, but the error messages contain the resolved URLs.
# All domains use .invalid TLD (RFC 2606) to ensure immediate DNS failure without timeouts.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Path-relative URL: data.csv with base https://base.invalid/def/ → https://base.invalid/def/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/'" 2>&1 | grep -oF 'https://base.invalid/def/data.csv'

# Host-relative URL: /test/data.csv with base https://base.invalid/def/ → https://base.invalid/test/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/'" 2>&1 | grep -oF 'https://base.invalid/test/data.csv'

# Scheme-relative URL: //other.invalid/test/data.csv with base https://base.invalid/def/ → https://other.invalid/test/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('//other.invalid/test/data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/'" 2>&1 | grep -oF 'https://other.invalid/test/data.csv'

# Absolute URL (url_base should be ignored): https://other.invalid/absolute.csv remains as-is
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('https://other.invalid/absolute.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/'" 2>&1 | grep -oF 'https://other.invalid/absolute.csv'

# Path-relative without trailing slash: data.csv with base https://base.invalid/def → https://base.invalid/data.csv (RFC 3986 merge)
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def'" 2>&1 | grep -oF 'https://base.invalid/data.csv'

# url_base with query string should be handled correctly
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?token=123'" 2>&1 | grep -oF 'https://base.invalid/def/data.csv'

# url_base without path: https://base.invalid + /test → https://base.invalid/test
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/test', CSV, 'c String') SETTINGS url_base = 'https://base.invalid'" 2>&1 | grep -oF 'https://base.invalid/test'

# Scheme-relative URL with http base
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('//other.invalid/other.csv', CSV, 'c String') SETTINGS url_base = 'http://base.invalid/path/'" 2>&1 | grep -oF 'http://other.invalid/other.csv'

# url_base with query string containing special characters (slashes, colons)
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?redirect=http://other.invalid/path&x=1'" 2>&1 | grep -oF 'https://base.invalid/def/data.csv'

# url_base with fragment identifier containing special characters
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/#section/sub/http://foo'" 2>&1 | grep -oF 'https://base.invalid/def/data.csv'

# Host-relative URL with url_base that has a query string with slashes
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('/other/file.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/def/?redirect=http://x.invalid/'" 2>&1 | grep -oF 'https://base.invalid/other/file.csv'

# Path-relative URL with authority-only base (no path): https://base.invalid + data.csv → https://base.invalid/data.csv
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'https://base.invalid'" 2>&1 | grep -oF 'https://base.invalid/data.csv'

# Query-only relative reference: ?x=1 with base https://base.invalid/dir/file.csv → https://base.invalid/dir/file.csv?x=1
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('?x=1', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv'" 2>&1 | grep -oF 'https://base.invalid/dir/file.csv?x=1'

# Fragment-only relative reference: #frag with base https://base.invalid/dir/file.csv → https://base.invalid/dir/file.csv#frag
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('#frag', CSV, 'c String') SETTINGS url_base = 'https://base.invalid/dir/file.csv'" 2>&1 | grep -oF 'https://base.invalid/dir/file.csv#frag'

# Invalid url_base (no scheme) should produce an error
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('data.csv', CSV, 'c String') SETTINGS url_base = 'example.invalid/def/'" 2>&1 | grep -oF 'must contain a scheme'
