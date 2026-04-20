#!/usr/bin/env bash
# Tags: no-replicated-database
# Regression test for view dependencies being silently dropped during `RENAME TABLE`
# and `EXCHANGE TABLES` for a materialized view.
# The root cause: `addDependencies` returned early when both referential and loading
# dependencies were empty, without checking view dependencies. After rename, the MV
# lost its edge in the view dependencies graph, so inserts into the source table
# would stop triggering the renamed MV.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_04104"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB} ENGINE = Atomic;
    CREATE TABLE ${DB}.src (c0 Int) ENGINE = Memory;
    CREATE TABLE ${DB}.target (c0 Int) ENGINE = Memory;
    CREATE MATERIALIZED VIEW ${DB}.mv TO ${DB}.target AS SELECT c0 FROM ${DB}.src;

    -- RENAME TABLE path: without fix, addDependencies early-returns when ref/loading
    -- dependencies are empty and silently drops the mv's view dependency edge.
    RENAME TABLE ${DB}.mv TO ${DB}.mv_renamed;

    INSERT INTO ${DB}.src VALUES (1);
    SELECT 'rename', count() FROM ${DB}.target;

    -- EXCHANGE TABLES path: same early-return bug when ref/loading dependencies are empty.
    CREATE MATERIALIZED VIEW ${DB}.mv_other TO ${DB}.target AS SELECT c0 + 100 AS c0 FROM ${DB}.src;
    EXCHANGE TABLES ${DB}.mv_renamed AND ${DB}.mv_other;

    TRUNCATE TABLE ${DB}.target;
    INSERT INTO ${DB}.src VALUES (2);
    -- Both MVs must still receive the insert after the exchange.
    SELECT 'exchange', count() FROM ${DB}.target;
"
