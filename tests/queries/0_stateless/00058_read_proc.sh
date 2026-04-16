#!/usr/bin/env bash
# Tags: no-darwin

# Verify that clickhouse-local can read files from /proc (Linux procfs).
# /proc is not available on Darwin, hence the no-darwin tag.
# The file() table function in server mode restricts paths to user_files_path,
# so clickhouse-local is used here instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# /proc/version always exists on Linux and contains a non-empty string.
$CLICKHOUSE_LOCAL -q "SELECT length(line) > 0 FROM file('/proc/version', 'LineAsString') LIMIT 1"
