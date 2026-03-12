#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

tmp_dir="$(mktemp -d "${CLICKHOUSE_TMP:-/tmp}/clickhouse-local-ls.0")"
trap 'rm -rf "$tmp_dir"' EXIT # make sure the tmp_dir is deleted if this script fails

#create two files on tmp_dir and a directory
touch "$tmp_dir/alpha.tsv"
touch "$tmp_dir/beta.csv"
mkdir "$tmp_dir/subdir"

# Test ls command
echo "-- ls"
(
    cd "$tmp_dir"
    $CLICKHOUSE_LOCAL -q "ls"
)

echo "-- ls;"
(
    cd "$tmp_dir"
    $CLICKHOUSE_LOCAL -q "ls;"
)

echo "-- ls foo"
(
    cd "$tmp_dir"
    $CLICKHOUSE_LOCAL -q "ls foo" 2>&1 || true
)