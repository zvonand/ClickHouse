-- Comparing Time/Time64 with DateTime/DateTime64 promotes Time to DateTime

SET session_timezone = 'UTC';

SELECT 'Time promoted to DateTime64 - epoch date prepended';
SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') = toDateTime64('1900-01-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') = toDateTime64('2025-06-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') < toDateTime64('2025-06-01 14:45:40', 9);

SELECT 'Time64 promoted to DateTime64';
SELECT CAST('14:45:40.123', 'Time64(3)') = toDateTime64('1970-01-01 14:45:40.123', 3);
SELECT CAST('14:45:40.123', 'Time64(3)') = toDateTime64('2025-06-01 14:45:40.123', 3);

SELECT 'DateTime vs Time - Time promoted to DateTime';
SELECT toDateTime('1970-01-01 10:10:10') = CAST('10:10:10', 'Time');
SELECT toDateTime('2025-01-01 10:10:10') = CAST('10:10:10', 'Time');
SELECT toDateTime('2025-01-01 10:10:10') > CAST('10:10:10', 'Time');

SELECT 'DateTime vs Time64';
SELECT toDateTime('1970-01-01 14:45:40') = CAST('14:45:40.000', 'Time64(3)');

SELECT 'Time vs Time64';
SELECT CAST('14:45:40', 'Time') = CAST('14:45:40.000', 'Time64(3)');

SELECT 'DateTime vs DateTime64';
SELECT toDateTime('2025-01-01 14:45:40') = toDateTime64('2025-01-01 14:45:40', 3);

SELECT 'Filtering via table';
DROP TABLE IF EXISTS test_time_cmp;
CREATE TABLE test_time_cmp (t Time) ENGINE = MergeTree ORDER BY t;
INSERT INTO test_time_cmp VALUES ('14:45:40');
SELECT * FROM test_time_cmp WHERE t = toDateTime64('1970-01-01 14:45:40', 9);
SELECT * FROM test_time_cmp WHERE t = toDateTime64('2025-06-01 14:45:40', 3);
SELECT * FROM test_time_cmp WHERE t = CAST('14:45:40', 'Time');
DROP TABLE test_time_cmp;
