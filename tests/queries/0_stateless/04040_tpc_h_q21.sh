#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the `datasets` database is not created in fasttest.
# no-replicated-database: the `datasets` database is not created in DatabaseReplicated mode.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# shellcheck source=./04040_tpc_h.lib
. "$CURDIR"/04040_tpc_h.lib

{ echo "USE datasets;"; cat "$CURDIR/../../benchmarks/tpc-h/queries/query_21.sql"; } | $CLICKHOUSE_CLIENT "${SETTINGS[@]}"
