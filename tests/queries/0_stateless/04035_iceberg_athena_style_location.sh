#!/usr/bin/env bash
# Tags: no-fasttest
# Positive test: dbt-Athena-created Iceberg table where data files live under
# a different UUID subdirectory than the current metadata (s3_data_naming=schema_table_unique).
# Layout:
#   viewers/
#   ├── <uuid_old>/data/*.parquet        ← data from previous dbt run
#   └── <uuid_new>/metadata/             ← current metadata referencing old data
# ClickHouse points at <uuid_new>, but manifests reference data under <uuid_old>.
# IcebergPathResolver::resolve must strip the scheme://bucket/ prefix correctly.
# Fixes https://github.com/ClickHouse/ClickHouse/issues/97234

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

BASE_PATH="04035_iceberg_athena/${CLICKHOUSE_TEST_UNIQUE_NAME}"
UUID_OLD="f6e94b2e-f9b8-4601-9896-5ccfac55f7bb"
UUID_NEW="74653213-cf9a-429d-818f-2d167eed0d0f"
OLD_PATH="${BASE_PATH}/${UUID_OLD}"
NEW_PATH="${BASE_PATH}/${UUID_NEW}"

NORMALIZE="replaceRegexpAll(replaceRegexpAll(replaceOne(replaceOne(replaceOne(_path, '${BASE_PATH}', '<base>'), '${UUID_OLD}', '<uuid_old>'), '${UUID_NEW}', '<uuid_new>'), '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<uuid>'), 'snap-[0-9]+-[0-9]+-', 'snap-<id>-')"

# Create the "old" dbt run: a real Iceberg table with data under UUID_OLD.
${CLICKHOUSE_CLIENT} --allow_experimental_insert_into_iceberg 1 -q "
    DROP TABLE IF EXISTS t_iceberg_athena;
    CREATE TABLE t_iceberg_athena (c0 Int) ENGINE = IcebergS3(s3_conn, filename = '${OLD_PATH}');
    INSERT INTO t_iceberg_athena VALUES (99);
    DROP TABLE IF EXISTS t_iceberg_athena;
"

# Show layout before modification: only old UUID has files.
echo "-- before modification"
${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT ${NORMALIZE} AS path
    FROM s3(s3_conn, filename='${BASE_PATH}/**', structure='dummy String', format='LineAsString')
    ORDER BY path
" 2>/dev/null

# Read the old metadata to get the manifest-list path and location.
METADATA=$(${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM s3(s3_conn, filename='${OLD_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
")

OLD_LOCATION=$(echo "${METADATA}" | python3 -c "
import json, sys
m = json.load(sys.stdin)
print(m['location'])
")

# Derive the scheme://bucket/ prefix from the old location.
BUCKET_PREFIX=$(echo "${OLD_LOCATION}" | python3 -c "
import sys
loc = sys.stdin.read().strip()
table_path = '${OLD_PATH}'
idx = loc.find(table_path)
print(loc[:idx] if idx >= 0 else loc)
")

# Copy all .avro files (manifest-lists and manifests) to new UUID's metadata dir.
# The manifests contain references to data files under UUID_OLD.
AVRO_FILES=$(${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT _file FROM s3(s3_conn, filename='${OLD_PATH}/metadata/*.avro', structure='dummy String', format='LineAsString')
")
while IFS= read -r fname; do
    [ -z "${fname}" ] && continue
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${NEW_PATH}/metadata/${fname}')
        SELECT * FROM s3(s3_conn, filename='${OLD_PATH}/metadata/${fname}')
    "
done <<< "${AVRO_FILES}"

# Create metadata JSON for the new UUID: update location and all manifest-list paths.
echo "${METADATA}" | python3 -c "
import json, sys
m = json.load(sys.stdin)
bucket_prefix = '${BUCKET_PREFIX}'
old_path = '${OLD_PATH}'
new_path = '${NEW_PATH}'
m['location'] = bucket_prefix + new_path
for s in m.get('snapshots', []):
    ml = s['manifest-list']
    s['manifest-list'] = ml.replace(old_path, new_path)
print(json.dumps(m))
" | ${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${NEW_PATH}/metadata/v2.metadata.json', structure='line String', format='LineAsString')
    SELECT * FROM input('line String') FORMAT LineAsString
"

# Also need v1.metadata.json for Iceberg to find the table.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${NEW_PATH}/metadata/v1.metadata.json')
    SELECT * FROM s3(s3_conn, filename='${OLD_PATH}/metadata/v1.metadata.json')
"

# Show layout after modification: data under old UUID, metadata under new UUID.
echo "-- after modification"
${CLICKHOUSE_CLIENT} -q "
    SELECT DISTINCT ${NORMALIZE} AS path
    FROM s3(s3_conn, filename='${BASE_PATH}/**', structure='dummy String', format='LineAsString')
    ORDER BY path
" 2>/dev/null

# Query via the NEW_PATH — manifests reference data under OLD UUID.
# IcebergPathResolver::resolve strips scheme://bucket/ prefix to resolve cross-UUID paths.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 -q "
    SELECT * FROM icebergS3(s3_conn, filename='${NEW_PATH}');
"
