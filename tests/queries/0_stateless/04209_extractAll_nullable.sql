-- Functions returning Array (or other types that can't be Nullable) must accept
-- Nullable inputs and return their non-Nullable result type unchanged.
-- See https://github.com/ClickHouse/ClickHouse/issues/56977

-- extractAll on a Nullable column with a non-null value
SELECT extractAll(toNullable('Hello, world'), '(\\w+)');

-- extractAll on a Nullable column with NULL falls back to the default of Array(String) (empty array)
SELECT extractAll(materialize(CAST(NULL AS Nullable(String))), '(\\w+)');

-- extractAll on Nullable with mixed values
DROP TABLE IF EXISTS t_extract_null;
CREATE TABLE t_extract_null (s Nullable(String)) ENGINE = Memory;
INSERT INTO t_extract_null VALUES ('a b c'), (NULL), ('x y z');
SELECT s, extractAll(s, '(\\w+)') FROM t_extract_null ORDER BY s;
DROP TABLE t_extract_null;

-- Other "extract multiple strings to array" functions
SELECT extractAllGroups(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractAllGroupsHorizontal(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractAllGroupsVertical(toNullable('a=1, b=2'), '(\\w+)=(\\d+)');
SELECT extractGroups(toNullable('a=1'), '(\\w+)=(\\d+)');

-- splitBy* family
SELECT splitByChar(',', toNullable('a,b,c'));
SELECT splitByString('::', toNullable('a::b::c'));
SELECT splitByRegexp('\\W+', toNullable('hello, world!'));
SELECT splitByWhitespace(toNullable('hello world'));
SELECT splitByNonAlpha(toNullable('one,two;three'));
SELECT alphaTokens(toNullable('one,two;three'));

-- All of the above with NULL input return the default value (empty array, not NULL)
SELECT extractAllGroups(materialize(CAST(NULL AS Nullable(String))), '(\\w+)=(\\d+)');
SELECT splitByChar(',', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByString('::', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByRegexp('\\W+', materialize(CAST(NULL AS Nullable(String))));
SELECT splitByWhitespace(materialize(CAST(NULL AS Nullable(String))));
SELECT splitByNonAlpha(materialize(CAST(NULL AS Nullable(String))));

-- The result type must remain non-Nullable (it would be illegal to wrap Array in Nullable)
SELECT toTypeName(extractAll(toNullable('x'), '(\\w+)'));
SELECT toTypeName(splitByChar(',', toNullable('a,b')));
SELECT toTypeName(extractAllGroups(toNullable('x'), '(\\w+)'));

-- LowCardinality(Nullable(String)) input must also work
SELECT extractAll(toLowCardinality(toNullable('Hello world')), '(\\w+)');
SELECT toTypeName(extractAll(toLowCardinality(toNullable('Hello')), '(\\w+)'));
