#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database
# no-fasttest: the `tpch` database is not created in fasttest.
# no-replicated-database: the `tpch` database is not created in DatabaseReplicated mode.
# no-random-settings: these tests verify correctness, not behavior under random settings that may cause memory issues.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./04040_tpc_h.lib
. "$CURDIR"/04040_tpc_h.lib

{ echo "USE tpch;"; cat "$CURDIR/../../benchmarks/tpc-h/queries/query_16.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}"
