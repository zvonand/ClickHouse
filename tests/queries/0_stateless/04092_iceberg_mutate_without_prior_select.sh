#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test: ALTER TABLE UPDATE on an Iceberg table used to throw a
# LOGICAL_ERROR exception ("Can't extract iceberg table state from storage
# snapshot") when no SELECT or INSERT preceded the mutation in the same server
# lifetime.
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

# Populate the Iceberg data files on disk.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES ('a')"

# Drop and re-create the table to get a fresh storage object whose in-memory
# metadata has no datalake_table_state loaded (simulates a server restart).
# The Iceberg data files remain on disk, so the second CREATE must use
# IF NOT EXISTS to skip writing initial metadata and reuse the existing files.
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE IF NOT EXISTS ${TABLE} (c0 String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# This mutation is the first operation on the freshly created table — no prior
# SELECT or INSERT has called updateExternalDynamicMetadataIfExists().
# Without the fix, this throws a LOGICAL_ERROR exception.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} UPDATE c0 = 'b' WHERE TRUE"

${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
