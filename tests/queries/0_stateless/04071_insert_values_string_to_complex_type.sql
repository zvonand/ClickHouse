SELECT 'Map(String, Float32) - basic insert and order by key';
DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (m Map(String, Float32)) ENGINE = Memory;
INSERT INTO t_map VALUES ('{\'key1\':1, \'key2\':10}');
INSERT INTO t_map VALUES ('{\'key1\':2, \'key2\':20}');
SELECT m FROM t_map ORDER BY m['key1'];
DROP TABLE t_map;

SELECT 'Map with nested quoting (key contains apostrophe)';
DROP TABLE IF EXISTS t_map_quote;
CREATE TABLE t_map_quote (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_quote VALUES ('{\'it\\\'s\':1}');
SELECT m FROM t_map_quote;
DROP TABLE t_map_quote;

SELECT 'Empty Map';
DROP TABLE IF EXISTS t_map_empty;
CREATE TABLE t_map_empty (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_empty VALUES ('{}');
SELECT m FROM t_map_empty;
DROP TABLE t_map_empty;

SELECT 'Array(UInt32) - with empty array';
DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (a Array(UInt32)) ENGINE = Memory;
INSERT INTO t_arr VALUES ('[1,2,3]');
INSERT INTO t_arr VALUES ('[]');
SELECT a FROM t_arr ORDER BY length(a);
DROP TABLE t_arr;

SELECT 'Tuple(String, UInt32)';
DROP TABLE IF EXISTS t_tup;
CREATE TABLE t_tup (t Tuple(String, UInt32)) ENGINE = Memory;
INSERT INTO t_tup VALUES ('(\'hello\', 42)');
SELECT t FROM t_tup;
DROP TABLE t_tup;

SELECT 'Multiple columns: one complex, one simple';
DROP TABLE IF EXISTS t_multi;
CREATE TABLE t_multi (id UInt32, m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_multi VALUES (1, '{\'a\':1}');
INSERT INTO t_multi VALUES (2, '{\'b\':2}');
SELECT id, m FROM t_multi ORDER BY id;
DROP TABLE t_multi;

SELECT 'Array of Maps (nested complex types)';
DROP TABLE IF EXISTS t_arr_map;
CREATE TABLE t_arr_map (a Array(Map(String, UInt32))) ENGINE = Memory;
INSERT INTO t_arr_map VALUES ('[{\'a\':1}, {\'b\':2}]');
SELECT a FROM t_arr_map;
DROP TABLE t_arr_map;

SELECT 'Map with Array values (nested complex types)';
DROP TABLE IF EXISTS t_map_arr;
CREATE TABLE t_map_arr (m Map(String, Array(UInt32))) ENGINE = Memory;
INSERT INTO t_map_arr VALUES ('{\'key\':[1,2,3]}');
SELECT m FROM t_map_arr;
DROP TABLE t_map_arr;

SELECT 'Fast path with input_format_values_interpret_expressions=0';
SET input_format_values_interpret_expressions = 0;
DROP TABLE IF EXISTS t_no_expr;
CREATE TABLE t_no_expr (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_no_expr VALUES ('{\'key\':1}');
SELECT m FROM t_no_expr;
DROP TABLE t_no_expr;
SET input_format_values_interpret_expressions = 1;

SELECT 'Native syntax - verify no regression';
DROP TABLE IF EXISTS t_native;
CREATE TABLE t_native (m Map(String, Float32)) ENGINE = Memory;
INSERT INTO t_native VALUES ({'key1':1, 'key2':10});
SELECT m FROM t_native;
DROP TABLE t_native;

SELECT 'Map with LowCardinality key type';
DROP TABLE IF EXISTS t_map_lc;
CREATE TABLE t_map_lc (m Map(LowCardinality(String), UInt32)) ENGINE = Memory;
INSERT INTO t_map_lc VALUES ('{\'a\':1}'), ('{\'b\':2}');
SELECT m FROM t_map_lc ORDER BY toString(m);
DROP TABLE t_map_lc;

SELECT 'Nested (Array(Tuple(...)) internally)';
DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (n Nested(key String, value UInt32)) ENGINE = Memory;
INSERT INTO t_nested VALUES ('[\'a\',\'b\']', '[1,2]');
SELECT n.key, n.value FROM t_nested;
DROP TABLE t_nested;

SELECT 'Multiple rows in single INSERT (batch)';
DROP TABLE IF EXISTS t_batch;
CREATE TABLE t_batch (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_batch VALUES ('{\'a\':1}'), ('{\'b\':2}'), ('{\'c\':3}');
SELECT m FROM t_batch ORDER BY toString(m);
DROP TABLE t_batch;
