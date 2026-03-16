#!/usr/bin/env bash
# Tags: replica

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test: attaching a partition whose parts contain a projection that has since
# been dropped from the table metadata must not mark the part as broken or
# lost.  Covers two code-paths:
#   1) checkDataPart  (ReplicatedMergeTreePartCheckThread)
#   2) sendPartFromDisk (DataPartsExchange -- inter-replica fetches)

run() { ${CLICKHOUSE_CLIENT} --query "$@"; }

# ── ReplicatedMergeTree test ────────────────────────────────────────────────
run "DROP TABLE IF EXISTS t_unknown_proj SYNC"
run "CREATE TABLE t_unknown_proj (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
     ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_unknown_proj', '1')
     PARTITION BY intDiv(y, 100) ORDER BY y
     SETTINGS max_parts_to_merge_at_once = 1"

run "INSERT INTO t_unknown_proj SELECT number, number FROM numbers(7)"

run "ALTER TABLE t_unknown_proj ADD PROJECTION pp (SELECT x, count() GROUP BY x)"
run "ALTER TABLE t_unknown_proj MATERIALIZE PROJECTION pp"

# Detach the partition so that parts with pp.proj are moved to detached/.
run "ALTER TABLE t_unknown_proj DETACH PARTITION 0"

# Drop projection pp from the table metadata while the partition is detached.
run "ALTER TABLE t_unknown_proj CLEAR PROJECTION pp"
run "ALTER TABLE t_unknown_proj DROP PROJECTION pp"

# Re-attach: the part still has pp.proj on disk, but the table no longer
# knows about projection pp.
run "ALTER TABLE t_unknown_proj ATTACH PARTITION 0"

# The part must be usable: CHECK TABLE should pass and data should be intact.
# Use send_logs_level=error to suppress expected warnings about unknown
# projection directories during CHECK TABLE.
echo "=== ReplicatedMergeTree ==="
run "SELECT count() FROM t_unknown_proj"
${CLICKHOUSE_CLIENT} --send_logs_level=error --query "CHECK TABLE t_unknown_proj" | grep -o "^1"

run "SELECT sum(x), sum(y) FROM t_unknown_proj"

# Force a merge to make sure the part with the unknown projection can merge.
run "ALTER TABLE t_unknown_proj MODIFY SETTING max_parts_to_merge_at_once = 100"
${CLICKHOUSE_CLIENT} --send_logs_level=error --query "OPTIMIZE TABLE t_unknown_proj FINAL"
run "SELECT count() FROM t_unknown_proj"
run "SELECT sum(x), sum(y) FROM t_unknown_proj"

run "DROP TABLE t_unknown_proj SYNC"

# ── Plain MergeTree test ────────────────────────────────────────────────────
run "DROP TABLE IF EXISTS t_unknown_proj_mt SYNC"
run "CREATE TABLE t_unknown_proj_mt (x Int32, y Int32, PROJECTION p (SELECT x, y ORDER BY x))
     ENGINE = MergeTree()
     PARTITION BY intDiv(y, 100) ORDER BY y
     SETTINGS max_parts_to_merge_at_once = 1"

run "INSERT INTO t_unknown_proj_mt SELECT number, number FROM numbers(7)"

run "ALTER TABLE t_unknown_proj_mt ADD PROJECTION pp (SELECT x, count() GROUP BY x)"
run "ALTER TABLE t_unknown_proj_mt MATERIALIZE PROJECTION pp"

run "ALTER TABLE t_unknown_proj_mt DETACH PARTITION 0"

run "ALTER TABLE t_unknown_proj_mt CLEAR PROJECTION pp"
run "ALTER TABLE t_unknown_proj_mt DROP PROJECTION pp"

run "ALTER TABLE t_unknown_proj_mt ATTACH PARTITION 0"

echo "=== MergeTree ==="
run "SELECT count() FROM t_unknown_proj_mt"
${CLICKHOUSE_CLIENT} --send_logs_level=error --query "CHECK TABLE t_unknown_proj_mt" | grep -o "^1"

# Force a merge.
run "ALTER TABLE t_unknown_proj_mt MODIFY SETTING max_parts_to_merge_at_once = 100"
${CLICKHOUSE_CLIENT} --send_logs_level=error --query "OPTIMIZE TABLE t_unknown_proj_mt FINAL"
run "SELECT count() FROM t_unknown_proj_mt"

run "DROP TABLE t_unknown_proj_mt SYNC"
