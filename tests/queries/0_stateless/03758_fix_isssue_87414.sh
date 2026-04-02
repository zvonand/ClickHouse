#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

cp -r "${CUR_DIR}/data_minio/issue87414/test/t0/" "${TABLE_PATH}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path = 'metadata/v2.metadata.json'"
$CLICKHOUSE_CLIENT -q "SELECT count(*), sum(c0) FROM ${TABLE}"

echo "INSERT INTO TABLE ${TABLE} (c0) SETTINGS write_full_path_in_iceberg_metadata = 1, allow_insert_into_iceberg=1 VALUES (1)" | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_metadata_file_path = 'metadata/v3.metadata.json'"
$CLICKHOUSE_CLIENT -q "SELECT count(*), sum(c0) FROM ${TABLE}"
