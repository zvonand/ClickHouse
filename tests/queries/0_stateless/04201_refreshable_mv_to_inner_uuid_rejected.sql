-- Tags: atomic-database
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
