-- Tags: atomic-database, no-parallel, no-random-settings
-- no-parallel: the APPEND cross-check below creates an inner table with a fixed,
-- server-wide unique UUID ('11111111-2222-3333-4444-555555555556'). When the
-- flaky-check runner schedules this test to multiple parallel workers (50 reruns,
-- workers = nproc-1), two concurrent runs would hit the global UUID registry at
-- the same time and one CREATE would fail with TABLE_ALREADY_EXISTS (Code 57)
-- before the test's `serverError BAD_ARGUMENTS` assertion is reached. Forcing
-- sequential execution removes the cross-run UUID collision while keeping the
-- assertion observable across reruns (Atomic engine releases the UUID on
-- `DROP TABLE … SYNC`, which is verified to allow safe sequential reuse).
-- no-random-settings: this test asserts a parse-time BAD_ARGUMENTS from CREATE
-- MATERIALIZED VIEW with REFRESH + TO INNER UUID (without APPEND). The flaky-check
-- runner randomizes `session_timezone` from a list that includes `get_localzone()`,
-- which on hosts where /etc/localtime is a direct symlink returns the malformed
-- string "zoneinfo/UTC". The server then rejects the very first query with an
-- unrelated `Invalid time zone: zoneinfo/UTC. (BAD_ARGUMENTS)` (verified in the
-- failing stderr) and the targeted assertion is never reached. Disabling random
-- settings keeps the timezone valid so the test exercises only the parse-time check.
-- Test: refreshable materialized view (without APPEND) must reject explicit `TO INNER UUID`.
-- Refresh in non-APPEND mode swaps the inner table on each refresh (different UUID each time),
-- so a user-fixed inner UUID is nonsensical. The check guards against silent confusion or
-- inconsistent UUIDs in the refresh task's EXCHANGE/DROP path.
-- Covers: src/Storages/StorageMaterializedView.cpp:263 -- if (to_inner_uuid != UUIDHelpers::Nil) throw BAD_ARGUMENTS
--         The branch fires only when refresh_strategy is set AND fixed_uuid (=APPEND) is false.

DROP TABLE IF EXISTS src_for_refresh_uuid SYNC;
DROP TABLE IF EXISTS rmv_with_inner_uuid SYNC;

CREATE TABLE src_for_refresh_uuid (x Int64) ENGINE = Memory;
INSERT INTO src_for_refresh_uuid VALUES (1);

-- Non-APPEND REFRESH + TO INNER UUID: rejected.
CREATE MATERIALIZED VIEW rmv_with_inner_uuid
    REFRESH EVERY 1 YEAR
    TO INNER UUID '11111111-2222-3333-4444-555555555555'
    (x Int64) ENGINE = Memory
    AS SELECT x FROM src_for_refresh_uuid; -- { serverError BAD_ARGUMENTS }

-- Cross-check: the branch is gated by `!fixed_uuid` (i.e. !APPEND). With APPEND, TO INNER UUID is permitted.
CREATE MATERIALIZED VIEW rmv_with_inner_uuid
    REFRESH EVERY 1 YEAR APPEND
    TO INNER UUID '11111111-2222-3333-4444-555555555556'
    (x Int64) ENGINE = Memory
    EMPTY
    AS SELECT x FROM src_for_refresh_uuid;

-- Confirm the APPEND branch did not trigger the new check.
SELECT 'append_with_inner_uuid_ok';

DROP TABLE rmv_with_inner_uuid SYNC;
DROP TABLE src_for_refresh_uuid SYNC;
