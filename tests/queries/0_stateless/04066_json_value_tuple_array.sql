-- Tests for JSON_VALUE with tuple and array output

SELECT '--JSON_VALUE WITH TUPLE OUTPUT--';
SELECT JSON_VALUE('{"hello":null}', tuple('$.hello'));
SELECT JSON_VALUE('{"hello":1}', tuple('$', '$.hello', '$.hello1'));
SELECT JSON_VALUE('{"hello":1.2}', tuple('$', '$.hello'));
SELECT JSON_VALUE('{"hello":true}', tuple('$', '$.hello'));
SELECT JSON_VALUE('{"hello":"world"}', tuple('$', '$.hello'));
SELECT JSON_VALUE('{"hello":["world","world2"], "hello1": {"a": "b"}}', tuple('$', '$.hello', '$.hello[1]', '$.hello1', '$.hello1.a'));
SELECT JSON_VALUE('{"hello":[{"a":"b"}, {"a":"b1"}, {"b":"c1"}]}', tuple('$.hello[0]', '$.hello[*]', '$.hello[*].a', '$.hello[*].b'));
SELECT JSON_VALUE('{"hello":{"world":"!"}}', tuple('$.hello'));
SELECT JSON_VALUE('{hello:world}', tuple('$.hello')); -- invalid json => default value (empty string)
SELECT JSON_VALUE('', tuple('$.hello'));
SELECT JSON_VALUE('{"foo foo":"bar"}', tuple('$."foo foo"'));
SELECT JSON_VALUE('{"hello":"\\uD83C\\uDF3A \\uD83C\\uDF38 \\uD83C\\uDF37 Hello, World \\uD83C\\uDF37 \\uD83C\\uDF38 \\uD83C\\uDF3A"}', tuple('$.hello'));
SELECT JSON_VALUE('{"a":"Hello \\"World\\" \\\\"}', tuple('$.a'));
select JSON_VALUE('{"a":"\\n\\u0000"}', tuple('$.a'));
select JSON_VALUE('{"a":"\\u263a"}', tuple('$.a'));
SELECT JSON_VALUE('{"1key":1}', tuple('$.1key'));
SELECT JSON_VALUE('{"hello":1}', tuple('$[hello]', '$["hello"]', '$[\'hello\']'));
SELECT JSON_VALUE('{"hello 1":1}', tuple('$["hello 1"]'));
SELECT JSON_VALUE('{"1key":1}', tuple('$..1key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', tuple('$1key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', tuple('$key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', tuple('$.[key]')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', tuple('$.[key]', 1));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSON_VALUE('{"1key":1}', tuple('$.key', '$.1key'));

SELECT '--JSON_VALUE WITH ARRAY OUTPUT--';
SELECT JSON_VALUE('{"hello":null}', array('$.hello'));
SELECT JSON_VALUE('{"hello":1}', array('$', '$.hello', '$.hello1'));
SELECT JSON_VALUE('{"hello":1.2}', array('$', '$.hello'));
SELECT JSON_VALUE('{"hello":true}', array('$', '$.hello'));
SELECT JSON_VALUE('{"hello":"world"}', array('$', '$.hello'));
SELECT JSON_VALUE('{"hello":["world","world2"], "hello1": {"a": "b"}}', array('$', '$.hello', '$.hello[1]', '$.hello1', '$.hello1.a'));
SELECT JSON_VALUE('{"hello":[{"a":"b"}, {"a":"b1"}, {"b":"c1"}]}', array('$.hello[0]', '$.hello[*]', '$.hello[*].a', '$.hello[*].b'));
SELECT JSON_VALUE('{"hello":{"world":"!"}}', array('$.hello'));
SELECT JSON_VALUE('{hello:world}', array('$.hello')); -- invalid json => default value (empty string)
SELECT JSON_VALUE('', array('$.hello'));
SELECT JSON_VALUE('{"foo foo":"bar"}', array('$."foo foo"'));
SELECT JSON_VALUE('{"hello":"\\uD83C\\uDF3A \\uD83C\\uDF38 \\uD83C\\uDF37 Hello, World \\uD83C\\uDF37 \\uD83C\\uDF38 \\uD83C\\uDF3A"}', array('$.hello'));
SELECT JSON_VALUE('{"a":"Hello \\"World\\" \\\\"}', array('$.a'));
select JSON_VALUE('{"a":"\\n\\u0000"}', array('$.a'));
select JSON_VALUE('{"a":"\\u263a"}', array('$.a'));
SELECT JSON_VALUE('{"1key":1}', array('$.1key'));
SELECT JSON_VALUE('{"hello":1}', array('$[hello]', '$["hello"]', '$[\'hello\']'));
SELECT JSON_VALUE('{"hello 1":1}', array('$["hello 1"]'));
SELECT JSON_VALUE('{"1key":1}', array('$..1key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', array('$1key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', array('$key')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', array('$.[key]')); -- { serverError BAD_ARGUMENTS }
SELECT JSON_VALUE('{"1key":1}', array(0, 1));  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT JSON_VALUE('{"1key":1}', array('$.key', '$.1key'));
