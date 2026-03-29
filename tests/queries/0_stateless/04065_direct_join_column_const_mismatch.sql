-- Test that DirectJoinMergeTreeEntity handles ColumnConst columns correctly
-- when merging multiple blocks from the pipeline (convertToFullColumnIfConst).
-- A column added via ALTER after data insertion produces ColumnConst in
-- MergeTree reads. With small max_block_size, multiple such blocks are
-- merged in executePlan, which previously triggered an assertTypeEquality
-- failure in insertRangeFrom.

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_right (id UInt64, value String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_right SELECT number, 'val_' || toString(number) FROM numbers(10);

-- Add a column after data is already written; the existing part does not
-- store it, so MergeTree fills it as ColumnConst on read.
ALTER TABLE t_right ADD COLUMN new_col String DEFAULT 'default_value';

CREATE TABLE t_left (id UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO t_left SELECT number FROM numbers(10);

SET join_algorithm = 'direct';
SET max_block_size = 2;
SET enable_analyzer = 1;

SELECT l.id, r.value, r.new_col
FROM t_left AS l
INNER JOIN t_right AS r ON l.id = r.id
ORDER BY l.id;

SELECT '--';

SELECT l.id, r.value, r.new_col
FROM t_left AS l
LEFT JOIN t_right AS r ON l.id = r.id
ORDER BY l.id;

DROP TABLE t_left;
DROP TABLE t_right;
