#!/usr/bin/env bash
# Tags: long, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel, no-fasttest, no-tsan, no-asan, no-msan, no-ubsan
# no sanitizers -- memory consumption is unpredictable with sanitizers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Verifies that `max_bytes_ratio_before_external_join` actually triggers
# spilling, both locally and across the serialized query plan path that
# distributed queries take.
#
# The local check covers the request to verify spilling via profile events.
# The serialized-plan check addresses the request that the ratio must
# travel through `QueryPlanSerializationSettings` as a ratio (rather than
# being collapsed into an absolute byte value once on the coordinator),
# so that each executor recomputes the spill threshold from its own
# memory limits.

USER="u04201_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS max_memory_usage_for_user = '256Mi'"
$CLICKHOUSE_CLIENT -q "GRANT ALL ON *.* TO ${USER}"

LOG_LOCAL="04201_local_${CLICKHOUSE_DATABASE}"
LOG_DIST="04201_serialized_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE IF EXISTS t_left_04201 SYNC"
$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE IF EXISTS t_right_04201 SYNC"
$CLICKHOUSE_CLIENT --user "${USER}" -q "CREATE TABLE t_left_04201  (k UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --user "${USER}" -q "CREATE TABLE t_right_04201 (k UInt64) ENGINE = MergeTree ORDER BY k"
$CLICKHOUSE_CLIENT --user "${USER}" -q "INSERT INTO t_left_04201  SELECT number FROM numbers(100000)"
$CLICKHOUSE_CLIENT --user "${USER}" -q "INSERT INTO t_right_04201 SELECT number FROM numbers(100000)"

# 1. Non-distributed join with the ratio set: spilling must happen.
#    The ratio is intentionally tiny (0.0001 of ~256MiB ≈ 26KiB) so the
#    spill threshold is well below the right-side hash table.
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    SELECT count()
    FROM t_left_04201 AS t1
    INNER JOIN t_right_04201 AS t2 ON t1.k = t2.k
    SETTINGS
        join_algorithm = 'hash',
        max_threads = 1,
        max_bytes_before_external_join = 0,
        max_bytes_ratio_before_external_join = 0.0001,
        log_comment = '${LOG_LOCAL}'
    FORMAT Null
"

# 2. Distributed query with `serialize_query_plan = 1`: the entire plan,
#    including the JOIN step, is serialized and sent to remote shards via
#    `QueryPlanSerializationSettings`. The ratio must round-trip through
#    those settings as a ratio (not as an absolute byte value collapsed
#    on the coordinator) so that each shard recomputes the spill
#    threshold from its own memory limits and actually spills.
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    SELECT count()
    FROM cluster('test_cluster_two_shards', currentDatabase(), t_left_04201) AS t1
    INNER JOIN cluster('test_cluster_two_shards', currentDatabase(), t_right_04201) AS t2 ON t1.k = t2.k
    SETTINGS
        enable_analyzer = 1,
        serialize_query_plan = 1,
        join_algorithm = 'hash',
        max_threads = 1,
        max_bytes_before_external_join = 0,
        max_bytes_ratio_before_external_join = 0.0001,
        log_comment = '${LOG_DIST}'
    FORMAT Null
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Verify spilling via profile events. We look at every QueryFinish row
# whose log_comment matches (the initiator's row plus any remote leaves
# inheriting the same log_comment), and require at least one of them to
# have `JoinSpillingHashJoinSwitchedToGraceJoin > 0`.
$CLICKHOUSE_CLIENT -q "
    SELECT
        'local',
        countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) > 0
    FROM system.query_log
    WHERE log_comment = '${LOG_LOCAL}'
        AND type = 'QueryFinish'
        AND event_date >= yesterday()
"

$CLICKHOUSE_CLIENT -q "
    SELECT
        'distributed',
        countIf(ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0) > 0
    FROM system.query_log
    WHERE log_comment = '${LOG_DIST}'
        AND type = 'QueryFinish'
        AND event_date >= yesterday()
"

$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE t_left_04201 SYNC"
$CLICKHOUSE_CLIENT --user "${USER}" -q "DROP TABLE t_right_04201 SYNC"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
