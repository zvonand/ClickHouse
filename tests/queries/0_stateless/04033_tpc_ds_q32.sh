#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: TPC-DS tables use web disk (S3) which is not available in fasttest.
# no-random-settings: random session_timezone, query_plan_join_swap_table, etc. change query results.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SETTINGS=(
    --data_type_default_nullable=1
    --format_tsv_null_representation ''
    --group_by_use_nulls=1
    --intersect_default_mode=DISTINCT
    --joined_subquery_requires_alias=0
    --join_use_nulls=1
    --union_default_mode=DISTINCT
    --format=TabSeparatedRaw
    -m
)

{ echo "USE datasets;"; cat "$CURDIR/../../benchmarks/tpc-ds/queries/query_32.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}"
