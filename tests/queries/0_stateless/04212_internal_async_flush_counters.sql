-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104309
--
-- `FailedInternalQuery` / `FailedInternalInsertQuery` / `FailedInternalSelectQuery` were
-- only incremented in `logQueryException` (failures during execution) but not in
-- `logExceptionBeforeStart` (failures before execution starts), undercounting internal
-- failures: async insert flushes, materialized view refreshes, and parse errors with
-- `internal=true`.
--
-- This test triggers an async insert flush failure (which calls
-- `logExceptionBeforeStart` from `AsynchronousInsertQueue::processData` with
-- `internal=true`) and verifies that all three internal counters are bumped alongside
-- the existing user-visible `FailedInsertQuery` / `FailedQuery` increments.

DROP TABLE IF EXISTS t_failed_internal_async_flush;
CREATE TABLE t_failed_internal_async_flush (a UInt32) ENGINE = MergeTree ORDER BY a;

-- Snapshot the relevant counters. `system.events` only exposes events that have been
-- incremented at least once, so absent rows are treated as zero in the delta below.
DROP TEMPORARY TABLE IF EXISTS t_snap_before;
CREATE TEMPORARY TABLE t_snap_before AS
SELECT event, value FROM system.events
WHERE event IN (
    'FailedQuery',
    'FailedInsertQuery',
    'FailedInternalQuery',
    'FailedInternalInsertQuery'
);

-- Submit several async inserts with a tight busy timeout, then drop the table so the
-- queued data has nowhere to land. The async flush thread will report
-- `UNKNOWN_TABLE` via `logExceptionBeforeStart(..., internal=true)`.
SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_busy_timeout_min_ms = 100;
SET async_insert_busy_timeout_max_ms = 100;

INSERT INTO t_failed_internal_async_flush VALUES (1);
INSERT INTO t_failed_internal_async_flush VALUES (2);
INSERT INTO t_failed_internal_async_flush VALUES (3);

DROP TABLE t_failed_internal_async_flush;

-- Force the queue to drain so the failure is observed before we read the counters.
SYSTEM FLUSH ASYNC INSERT QUEUE;

-- Each counter should have been bumped at least once by our async flush failure.
-- Other tests running in parallel only inflate the deltas; they cannot mask a
-- regression that would leave `FailedInternalQuery` or `FailedInternalInsertQuery`
-- at zero.
WITH
    after_value AS (
        SELECT event, value FROM system.events
        WHERE event IN (
            'FailedQuery',
            'FailedInsertQuery',
            'FailedInternalQuery',
            'FailedInternalInsertQuery'
        )
    ),
    expected AS (
        SELECT 'FailedQuery' AS event UNION ALL
        SELECT 'FailedInsertQuery' UNION ALL
        SELECT 'FailedInternalQuery' UNION ALL
        SELECT 'FailedInternalInsertQuery'
    )
SELECT
    e.event AS counter,
    (ifNull(a.value, 0) - ifNull(b.value, 0)) > 0 AS bumped
FROM expected AS e
LEFT JOIN after_value AS a ON a.event = e.event
LEFT JOIN t_snap_before AS b ON b.event = e.event
ORDER BY counter;
