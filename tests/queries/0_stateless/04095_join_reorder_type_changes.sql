-- Regression test for join reorder with type changes (toNullable from LEFT JOIN)
-- When the optimizer reorders a three-table join so that the non-LEFT-JOIN pair
-- is joined first, the type changes from the LEFT JOIN must not be applied
-- prematurely at the wrong step.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt32, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (1, 'Join_1_Value_0'), (2, 'Join_1_Value_1');
INSERT INTO t2 VALUES (1, 'Join_2_Value_0'), (3, 'Join_2_Value_2');
INSERT INTO t3 VALUES (1, 'Join_3_Value_0'), (2, 'Join_3_Value_1');

SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
INNER JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0'
ORDER BY ALL
SETTINGS join_use_nulls = 1;

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
