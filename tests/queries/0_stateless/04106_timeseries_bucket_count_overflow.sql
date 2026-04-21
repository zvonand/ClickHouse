-- Regression test for STID 2508-3c50 / 2508-3f3c / 2508-20f4:
-- `Logical error: 'index < bucket_count'` in `AggregateFunctionTimeseriesBase::bucketIndexForTimestamp`
-- when the grid parameters cause signed integer overflow in the bucket count calculation.
--
-- Triggered by AST fuzzer on PR #96504, #103189, etc. across 90 days — 3+ unrelated PRs.
-- Example fuzzer query:
--   SELECT timeSeriesResampleToGridWithStaleness(-9223372036854775808, 256, 2147483646, 2147483648)(timestamp, value)
--   FROM ts_data_overflow (timestamp DateTime64(0), ...);
--
-- With a very negative `start_timestamp` the signed `end - start` subtraction overflowed
-- to a negative value, producing a corrupted (huge) `bucket_count`, and
-- `bucketIndexForTimestamp` then computed an `index` that was interpreted as an absurd
-- `size_t`, failing the `chassert(index < bucket_count)` and aborting the server in debug /
-- sanitizer builds.
--
-- The fix uses unsigned 64-bit arithmetic for both the bucket count and the per-timestamp
-- index computation, and throws `BAD_ARGUMENTS` when the resulting grid exceeds
-- `MAX_BUCKET_COUNT` (16M buckets).

SET allow_experimental_ts_to_grid_aggregate_function = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_data_overflow_idx;
CREATE TABLE ts_data_overflow_idx (timestamp DateTime64(0) NOT NULL, value Float64 NOT NULL)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_data_overflow_idx VALUES ('2020-01-01 00:00:00', 1.0), ('2020-01-01 00:00:01', 2.0);

-- Case 1: start_timestamp near `INT64_MIN` with a reasonable-looking step. Prior to the fix,
-- this triggered `chassert(index < bucket_count)` failure (LOGICAL_ERROR abort).
SELECT timeSeriesResampleToGridWithStaleness(-9223372036854775808, 256, 2147483646, 2147483648)(timestamp, value)
FROM ts_data_overflow_idx FORMAT Null;  -- { serverError BAD_ARGUMENTS }

-- Case 2: Large positive start greater than end. Should be caught by the pre-existing
-- `end < start` guard without touching the new arithmetic.
SELECT timeSeriesChangesToGrid(9223372036854775807, 1, 256, 2147483648)(timestamp, value)
FROM ts_data_overflow_idx FORMAT Null;  -- { serverError BAD_ARGUMENTS }

-- Case 3: Grid size slightly over the hard cap (16M + 1 buckets). Must be rejected before
-- any allocation happens.
SELECT length(timeSeriesResampleToGridWithStaleness(toDateTime64(0, 0), toDateTime64(16777215, 0), 1, 1)(timestamp, value))
FROM ts_data_overflow_idx FORMAT Null;  -- { serverError BAD_ARGUMENTS }

-- Case 4: Grid size exactly at the cap (16M buckets). Must succeed — verifies the limit is
-- inclusive and no normal-range regression is introduced.
SELECT length(timeSeriesResampleToGridWithStaleness(toDateTime64(0, 0), toDateTime64(16777214, 0), 1, 1)(timestamp, value)) AS grid_len
FROM ts_data_overflow_idx;

-- Case 5: UInt32 timestamp with huge UInt64 start (truncated to UInt32). Tests the sibling
-- code path for `TimestampType = UInt32`.
DROP TABLE IF EXISTS ts_data_overflow_idx_u32;
CREATE TABLE ts_data_overflow_idx_u32 (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_data_overflow_idx_u32 VALUES ('2020-01-01 00:00:00', 1.0);
SELECT length(timeSeriesChangesToGrid(0, 16777213, 1, 1)(toUnixTimestamp(timestamp), value)) AS grid_len_u32
FROM ts_data_overflow_idx_u32;

-- Case 6: Typical happy-path usage must still work (regression guard).
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    210 AS end_ts,
    15 AS step_s,
    30 AS window_s
SELECT timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_s, window_s)(timestamp, value) AS happy_path
FROM (SELECT arrayJoin(arrayZip(timestamps, values)) AS ts_and_val, ts_and_val.1 AS timestamp, ts_and_val.2 AS value);

DROP TABLE ts_data_overflow_idx;
DROP TABLE ts_data_overflow_idx_u32;
