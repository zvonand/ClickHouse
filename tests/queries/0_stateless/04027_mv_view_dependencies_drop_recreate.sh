#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest
# Test that modifying a materialized view's query does not leak stale view dependency edges.
# The root cause: in updateDependencies, view dependency removal was inside
# if (!new_view_dependencies.empty()), so ALTER TABLE mv MODIFY QUERY SELECT 1 c0
# (with no view dependencies) would leak stale edges. Combined with BACKUP/RESTORE
# this accumulated multiple dependents, violating the assertion.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_04027"
BACKUP_NAME="${CLICKHOUSE_DATABASE}_04027_backup"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

# Use --send_logs_level=error: the RESTORE emits expected Warning messages from
# BackupMetadataFinder because the view's referenced tables are not in the backup
# (only the view itself is) and have been truncated from the database.
${CLICKHOUSE_CLIENT} --send_logs_level=error -nm -q "
    CREATE DATABASE ${DB} ENGINE = Memory;
    CREATE TABLE ${DB}.src1 (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB}.src2 (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB}.target (c0 Int) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${DB}.mv TO ${DB}.target AS SELECT c0 FROM ${DB}.src1;

    BACKUP VIEW ${DB}.mv TO Memory('${BACKUP_NAME}') SETTINGS id='${BACKUP_NAME}' FORMAT Null;

    -- Cleanly replaces: src1->mv becomes src2->mv
    ALTER TABLE ${DB}.mv MODIFY QUERY SELECT c0 FROM ${DB}.src2;
    -- Leaks stale edge src2->mv without the fix (empty new_view_dependencies skipped removal)
    ALTER TABLE ${DB}.mv MODIFY QUERY SELECT 1 c0;

    TRUNCATE DATABASE ${DB};

    RESTORE VIEW ${DB}.mv FROM Memory('${BACKUP_NAME}') SETTINGS id='${BACKUP_NAME}_restore' FORMAT Null;

    -- This triggers updateDependencies and would hit assert(tables_from.size() == 1) without the fix
    ALTER TABLE ${DB}.mv MODIFY COMMENT '';

    SELECT 'OK';
"
