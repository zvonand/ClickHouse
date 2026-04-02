#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

obf="$CLICKHOUSE_FORMAT --obfuscate"

echo "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Atomic" | $obf
echo "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Memory" | $obf
echo "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = MaterializedPostgreSQL('host:5432', 'mydb', 'user', 'pass')" | $obf
