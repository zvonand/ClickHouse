#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

path="/test-keeper-client-$CLICKHOUSE_DATABASE/lsr"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'" >& /dev/null || true

$CLICKHOUSE_KEEPER_CLIENT -q "create '$path' 'root'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/A' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/A/B' 'x'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '$path/T/C' 'x'"

echo 'lsr explicit path'
$CLICKHOUSE_KEEPER_CLIENT -q "lsr '$path' 100"

echo 'lsr path 2'
## Server returns ZNOTEMPTY when children_nodes_limit is exceeded; the client maps it to a generic
## "Not empty" message which is misleading here — only check that we get a coordination error.
out=$($CLICKHOUSE_KEEPER_CLIENT -q "lsr '$path' 2" 2>&1) || true
if [[ -z "$out" ]] || ! grep -q 'Coordination error:' <<<"$out"; then
    printf 'unexpected output for lsr with limit 2:\n%s\n' "$out" >&2
    exit 1
fi
echo 'ok (coordination error when recursive listing hits the limit)'

echo 'lsr from cwd'
$CLICKHOUSE_KEEPER_CLIENT -q "cd '$path'; lsr 100"

echo 'cleanup'
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '$path'"
