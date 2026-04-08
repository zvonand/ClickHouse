#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: ALTER TABLE UPDATE on an Iceberg table used to crash with
# "Logical error: Can't extract iceberg table state from storage snapshot"
# when no SELECT or INSERT preceded the mutation in the same server lifetime.
# The root cause was that StorageObjectStorage::mutate() did not call
# updateExternalDynamicMetadataIfExists() before reading data, so the storage
# snapshot lacked the datalake_table_state required by IcebergMetadata::iterate().

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES ('a')"

# This mutation triggers the read pipeline inside the mutation interpreter,
# which calls IcebergMetadata::iterate(). Without the fix, it crashes.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} UPDATE c0 = 'b' WHERE TRUE"

${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
