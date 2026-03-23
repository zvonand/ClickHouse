-- This test validates that max_execution_speed & timeout_before_checking_execution_speed works with:
--      a) regular query
--      b) merge query
-- NOTE: This test uses simple synthetic data to validate the fact throttling was applied.
-- If throttling works as expected - each execution will take >= 1 second, as we allow not more than {max_execution_speed} records/sec
-- If it doesn't - each select will finish immediately, and the test will fail
-- NOTE: Setting max_block_size=1 to ensure sleepEachRow(..) applies per each row guaranteed and the resulting timing is predictable [2-3] seconds

SET max_execution_speed = 5;                      -- this is parameter we're testing - to throttle execution down to 5 records/sec
SET timeout_before_checking_execution_speed = 0;  -- and to start throttling immediately
SET max_block_size = 1;                           -- needs to be tweaked to make sure we write only 1 record per block; otherwise we'll read everything at once, and throttling won't work

CREATE TEMPORARY TABLE times (t DateTime);

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
CREATE 
  TABLE t00156_max_execution_speed_sample_merge
    (v UInt64)
  ENGINE = MergeTree
  ORDER BY intHash32(v)
  SAMPLE BY intHash32(v)
  SETTINGS min_bytes_for_wide_part = 0;           -- another tweak to make sure all records don't end up in the same granule

INSERT INTO t00156_max_execution_speed_sample_merge SELECT number FROM numbers(30);

INSERT INTO times SELECT now();
SELECT * FROM t00156_max_execution_speed_sample_merge SAMPLE 1/2 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 2 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT * FROM merge('t00156_max_execution_speed_sample_merge') SAMPLE 1/2 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 2 FROM times;

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
