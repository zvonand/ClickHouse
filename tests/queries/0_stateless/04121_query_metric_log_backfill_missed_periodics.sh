#!/usr/bin/env bash
# Tags: long
#
# Regression test for `system.query_metric_log` correctness under heavy
# `BackgroundSchedulePool` load.
#
# Background: when periodic `collectMetric` tasks fire after the query has
# already finished, they used to be silently dropped (`getQueryInfo` returns
# null for a query no longer in the `ProcessList`). On TSan builds and in
# highly parallel CI runs this could leave `system.query_metric_log` without
# the documented number of rows for a finished query — including, sometimes,
# without the final "query finished" row when a periodic happened to share
# the finish moment's timestamp.
#
# `QueryMetricLog::finishQuery` now backfills missed periodic events and
# always emits the final event. This test exercises both paths by running a
# query that lasts well beyond its `query_metric_log_interval` and asserting
# that the row count and the final-event presence match the documented
# behavior.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

readonly query_id="${CLICKHOUSE_DATABASE}_backfill_test_$$"
readonly interval_ms=100
readonly sleep_seconds=1.5

$CLICKHOUSE_CLIENT --query-id="${query_id}" -q "SELECT sleep(${sleep_seconds}) SETTINGS query_metric_log_interval=${interval_ms}, enable_parallel_replicas=0, function_sleep_max_microseconds_per_block=2000000 FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_metric_log"

# For a 1.5-second query with 100 ms interval we expect 15 periodic rows plus
# 1 final row. With backfill in place, the count is at least 15 even if the
# `BackgroundSchedulePool` was so loaded that no periodic ever fired.
$CLICKHOUSE_CLIENT -q "
SELECT
    count() >= 15 AS has_at_least_15_rows,
    countDistinct(event_time_microseconds) >= 15 AS has_distinct_timestamps
FROM system.query_metric_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_id}'
"

# The final event must be present: `ProfileEvent_SleepFunctionMicroseconds`
# accumulated across all rows must equal the total sleep duration. Without
# the `is_final` bypass added to `createLogMetricElement`, a time-collision
# between the last periodic and the finish could drop the final and the sum
# would be short.
$CLICKHOUSE_CLIENT -q "
SELECT
    sum(ProfileEvent_SleepFunctionCalls) = 1 AS one_sleep_call,
    sum(ProfileEvent_SleepFunctionMicroseconds) = ${sleep_seconds} * 1000000 AS full_sleep_microseconds_accounted,
    sum(ProfileEvent_Query) = 1 AS one_query_event
FROM system.query_metric_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '${query_id}'
"
