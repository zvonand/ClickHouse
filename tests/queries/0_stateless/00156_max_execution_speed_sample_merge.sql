-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel
-- This test validates that max_execution_speed & timeout_before_checking_execution_speed works with:
--      a) regular query
--      b) merge query
-- NOTE: This test uses simple synthetic data to validate the fact throttling was applied.
-- If throttling works as expected - each execution will take >= 1 second, as we allow not more than {max_execution_speed} records/sec
-- If it doesn't - each select will finish immediately, and the test will fail
-- NOTE: fuzzer and parallelism are disabled as it can fiddle behaviour of merge and cause the flakiness - we're checking the functional correctness here

SET max_execution_speed = 2;                      -- this is the parameter we're testing - execution to be throttled down to 2 records/sec
SET timeout_before_checking_execution_speed = 0;  -- and to apply it immediately
SET max_block_size = 1;                           -- this one needs to be tweaked to ensure only 1 record per block is written; otherwise we'll read everything at once, and throttling won't work

CREATE TEMPORARY TABLE times (t DateTime);

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
CREATE
  TABLE t00156_max_execution_speed_sample_merge
    (v UInt64)
  ENGINE = MergeTree
  ORDER BY intHash32(v)
  SAMPLE BY intHash32(v)
  SETTINGS min_bytes_for_wide_part = 0;           -- another tweak to force Wide parts so that max_block_size is respected within granules

INSERT INTO t00156_max_execution_speed_sample_merge SELECT number FROM numbers(30);

INSERT INTO times SELECT now();
SELECT * FROM t00156_max_execution_speed_sample_merge SAMPLE 1/2 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT * FROM merge('t00156_max_execution_speed_sample_merge') SAMPLE 1/2 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
