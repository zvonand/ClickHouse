#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104295
#
# When `iceberg_metadata_staleness_ms` is non-zero, the metadata files cache
# may serve a pre-`ALTER` schema view to readers and to the upstream `INSERT`
# pipeline, while `IcebergStorageSink` always reads fresh metadata. The two
# views disagree on the column count, and `DataFileStatistics::getColumnSizes`
# accesses `field_ids` (sized by the post-`ALTER` schema) using an index from
# `column_sizes` (sized by the pre-`ALTER` chunk), which causes an
# out-of-bounds vector access and aborts the server.
#
# `ALTER` writes a new metadata file but did not invalidate the cache, while
# `IcebergStorageSink::initializeMetadata` did invalidate it. The fix is to
# invalidate the cache in `Iceberg::alter` after writing the new metadata
# file, mirroring the existing invalidation in `IcebergStorageSink`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (c0 Int, c1 Int)
    ENGINE = IcebergLocal('${TABLE_PATH}')
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} (c1, c0) VALUES (1, 1)
"

# `iceberg_metadata_staleness_ms = 60000` keeps the cached pre-`ALTER` schema
# alive across the subsequent `ALTER` and `INSERT`. Before the fix this caused
# a SIGABRT in `DataFileStatistics::getColumnSizes`. After the fix, `ALTER`
# invalidates the cache, so the follow-up `INSERT` sees the post-`ALTER`
# schema (one column) consistently and succeeds.
${CLICKHOUSE_CLIENT} \
    --allow_insert_into_iceberg=1 \
    --iceberg_metadata_staleness_ms=60000 \
    --query "ALTER TABLE ${TABLE} DROP COLUMN c0"

${CLICKHOUSE_CLIENT} \
    --allow_insert_into_iceberg=1 \
    --iceberg_metadata_staleness_ms=60000 \
    --query "INSERT INTO ${TABLE} (c1) SELECT 2"

${CLICKHOUSE_CLIENT} \
    --iceberg_metadata_staleness_ms=60000 \
    --query "SELECT c1 FROM ${TABLE} ORDER BY c1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
rm -rf "${TABLE_PATH}"
