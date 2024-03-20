set asterisk_include_materialized_columns=1;

DROP TABLE IF EXISTS queue_mode_test;

CREATE TABLE queue_mode_test(a UInt64, b UInt64) ENGINE=MergeTree() ORDER BY (a) SETTINGS queue_mode=1;

SELECT 'start';
SELECT * FROM queue_mode_test;

SELECT 'insert some data';
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(2);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(3);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(4);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(5);

SELECT 'optimize table to create single part';
OPTIMIZE TABLE queue_mode_test;

SELECT * FROM queue_mode_test;

SELECT 'cursor lookup';
SELECT * FROM queue_mode_test WHERE (_queue_block_number, _queue_block_offset) > (3, 1);

-- queue columns cannot be directly inserted (they be materialized)
INSERT INTO queue_mode_test (a,b,_queue_block_number,_queue_block_offset) SELECT number, number, number, number FROM numbers(2); -- { serverError ILLEGAL_COLUMN }

-- queue columns CANNOT be altered. (only DROP is checked here, it is enough)
ALTER TABLE queue_mode_test DROP COLUMN _queue_block_offset; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE queue_mode_test DROP COLUMN _queue_block_number; -- { serverError ILLEGAL_COLUMN }

-- _queue_block_number shall NOT be reused
ALTER TABLE queue_mode_test DELETE WHERE _queue_block_number == 4;
OPTIMIZE TABLE queue_mode_test;
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(5);
OPTIMIZE TABLE queue_mode_test;
SELECT count() FROM queue_mode_test where _queue_block_number == 4;

-- _queue_block_number, _queue_block_offset can be manually created in a non-queue table
CREATE TABLE queue_mode_test_2(_queue_block_offset UInt64, _queue_block_number UInt64) ENGINE=MergeTree() ORDER BY ();
DROP TABLE queue_mode_test_2 SYNC;

-- _queue_block_number, _queue_block_offset can be ADDed to a non-queue table
CREATE TABLE queue_mode_test_3(a UInt64) ENGINE=MergeTree() ORDER BY (a);
ALTER TABLE queue_mode_test_3 ADD COLUMN _queue_block_number UInt32 FIRST;
ALTER TABLE queue_mode_test_3 ADD COLUMN _queue_block_offset UInt32 FIRST;
DROP TABLE queue_mode_test_3 SYNC;

-- setting can be disabled
ALTER TABLE queue_mode_test MODIFY SETTING queue_mode=0;

DROP TABLE queue_mode_test SYNC;

SELECT 'end';
