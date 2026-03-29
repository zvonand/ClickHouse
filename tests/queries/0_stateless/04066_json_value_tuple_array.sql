-- Tests for SQL/JSON functions with multi-path (Tuple/Array) second argument.
-- The second argument can be an arbitrarily nested constant structure of Tuples/Arrays
-- with String leaves. The result mirrors the shape, with each leaf replaced by the function's result.

SET function_json_value_return_type_allow_nullable = 1;

SELECT '--JSON_VALUE with Tuple--';
SELECT JSON_VALUE('{"a":1, "b":"hello"}', tuple('$.a', '$.b'));
SELECT JSON_VALUE('{"a":1}', tuple('$.a', '$.missing'));
SELECT JSON_VALUE('{bad json}', tuple('$.a', '$.b'));

SELECT '--JSON_VALUE with Array--';
SELECT JSON_VALUE('{"a":1, "b":"hello"}', array('$.a', '$.b'));
SELECT JSON_VALUE('{"a":1}', array('$.a', '$.missing'));
SELECT JSON_VALUE('{bad json}', array('$.a'));

SELECT '--JSON_VALUE with nested structure--';
SELECT JSON_VALUE('{"a":1, "b":2, "c":3}', tuple(array('$.a', '$.b'), '$.c'));
SELECT JSON_VALUE('{"a":1, "b":2, "c":3}', tuple(tuple('$.a', '$.b'), array('$.c')));
SELECT JSON_VALUE('{"x":10}', array(tuple('$.x', '$.y')));

SELECT '--JSON_VALUE preserves all value types--';
SELECT JSON_VALUE('{"a":null, "b":true, "c":1.5, "d":"text", "e":[1,2]}', tuple('$.a', '$.b', '$.c', '$.d', '$.e'));

SELECT '--JSON_VALUE with multiple rows--';
SELECT JSON_VALUE(json, tuple('$.name', '$.age')) FROM VALUES('json String', ('{"name":"Alice","age":30}'), ('{"name":"Bob","age":25}'), ('{bad}'));

SELECT '--JSON_VALUE with nullable=0--';
SET function_json_value_return_type_allow_nullable = 0;
SELECT JSON_VALUE('{"a":1}', tuple('$.a', '$.missing'));
SELECT JSON_VALUE('{"a":1}', array('$.a', '$.missing'));
SET function_json_value_return_type_allow_nullable = 1;

SELECT '--JSON_EXISTS with Tuple--';
SELECT JSON_EXISTS('{"a":1, "b":2}', tuple('$.a', '$.missing'));

SELECT '--JSON_EXISTS with Array--';
SELECT JSON_EXISTS('{"a":1, "b":2}', array('$.a', '$.missing', '$.b'));

SELECT '--JSON_EXISTS with nested structure--';
SELECT JSON_EXISTS('{"a":1}', tuple(array('$.a', '$.b'), '$.a'));

SELECT '--JSON_QUERY with Tuple--';
SELECT JSON_QUERY('{"a":[1,2], "b":{"c":3}}', tuple('$.a', '$.b'));

SELECT '--JSON_QUERY with Array--';
SELECT JSON_QUERY('{"a":[1,2]}', array('$.a', '$.missing'));

SELECT '--Error cases--';
SELECT JSON_VALUE('{"a":1}', tuple('$..invalid')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"a":1}', tuple('$.a', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSON_VALUE('{"a":1}', array(1, 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
