-- Tags: no-random-settings

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], [3, 4], [5, 6]);

-- Only n.a is used — n.b and n.c should not be read.
SELECT n.a FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- Both n.a and n.b used — n.c should not be read.
SELECT n.a, n.b FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- Direct reference to n — all subcolumns needed.
SELECT n FROM t_nested ARRAY JOIN n ORDER BY n.a;

-- n used only in WHERE — should still be pruned to only n.a.
SELECT 1 FROM t_nested ARRAY JOIN n WHERE n.a > 0;

DROP TABLE t_nested;

-- General case: ARRAY JOIN with two independent arrays, only one used.
DROP TABLE IF EXISTS t_two_arrays;
CREATE TABLE t_two_arrays (a Array(Int64), b Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_two_arrays VALUES ([1, 2], [3, 4]);

SELECT b FROM t_two_arrays ARRAY JOIN a, b ORDER BY b;

DROP TABLE t_two_arrays;
