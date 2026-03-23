-- This test validates that max_execution_speed & timeout_before_checking_execution_speed works with:
--      a) regular query
--      b) merge query
-- NOTE: This test uses simple synthetic data to validate the fact throttling was applied.
-- If throttling works as expected - each execution will take >= 1 second, as we allow not more than {max_execution_speed} records/seconds
-- If it doesn't - each select will finish immediately, and the test will fail
-- NOTE: Setting max_block_size=1 to ensure sleepEachRow(..) applies per each row guaranteed and the resulting timing is predictable [2-3] seconds

SET max_execution_speed = 10;
SET timeout_before_checking_execution_speed = 0;
SET max_block_size = 1;

CREATE TEMPORARY TABLE times (t DateTime);

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
CREATE 
  TABLE t00156_max_execution_speed_sample_merge
    (v UInt64)
  ENGINE = MergeTree
  ORDER BY intHash32(v)
  SAMPLE BY intHash32(v);

INSERT INTO t00156_max_execution_speed_sample_merge SELECT number FROM numbers(4);

INSERT INTO times SELECT now();
SELECT * FROM t00156_max_execution_speed_sample_merge SAMPLE 1/2 WHERE sleepEachRow(1) == 0 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 2 FROM times;
TRUNCATE TABLE times;

INSERT INTO times SELECT now();
SELECT * FROM merge('t00156_max_execution_speed_sample_merge') SAMPLE 1/2 WHERE sleepEachRow(1) == 0 FORMAT Null;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 2 FROM times;

DROP TABLE IF EXISTS t00156_max_execution_speed_sample_merge;
