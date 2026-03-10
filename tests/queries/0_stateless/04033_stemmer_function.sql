-- Tests for the stemmer() function.
-- stemmer(word, lang) stems a single word or an array of words using Snowball algorithms.
-- Accepts String, FixedString, Nullable and LowCardinality variants, and Array thereof.

SET allow_experimental_nlp_functions = 1;

SELECT 'Scalar inputs.';

SELECT '- String input.';
SELECT stemmer('blessing', 'en');
SELECT stemmer('disguise', 'en');
SELECT stemmer('running', 'en');

SELECT '- FixedString input produces String output.';
SELECT stemmer(toFixedString('blessing', 10), 'en');
SELECT toTypeName(stemmer(toFixedString('word', 10), 'en'));

SELECT '- Nullable(String) input preserves nullability.';
SELECT stemmer(toNullable('blessing'), 'en');
SELECT stemmer(toNullable(NULL), 'en');
SELECT toTypeName(stemmer(toNullable('word'), 'en'));

SELECT '- Nullable(FixedString) input.';
SELECT stemmer(toNullable(toFixedString('blessing', 10)), 'en');
SELECT toTypeName(stemmer(toNullable(toFixedString('word', 10)), 'en'));

SELECT '- LowCardinality(String) input.';
SELECT stemmer(toLowCardinality('blessing'), 'en');
SELECT toTypeName(stemmer(toLowCardinality('word'), 'en'));

SELECT '- LowCardinality(FixedString) input.';
SELECT stemmer(toLowCardinality(toFixedString('blessing', 10)), 'en');

SELECT 'Array inputs.';

SELECT '- Array(String) input.';
SELECT stemmer(['blessing', 'disguise', 'running'], 'en');
SELECT toTypeName(stemmer(['word'], 'en'));

SELECT '- Array(FixedString) input produces Array(String) output.';
SELECT stemmer([toFixedString('blessing', 10), toFixedString('disguise', 10)], 'en');
SELECT toTypeName(stemmer([toFixedString('word', 10)], 'en'));

SELECT '- Array(Nullable(String)) input preserves nulls.';
SELECT stemmer([toNullable('blessing'), NULL, toNullable('running')], 'en');
SELECT toTypeName(stemmer([toNullable('word')], 'en'));

SELECT '- Array(Nullable(FixedString)) input.';
SELECT stemmer([toNullable(toFixedString('blessing', 10)), NULL, toNullable(toFixedString('running', 10))], 'en');
SELECT toTypeName(stemmer([toNullable(toFixedString('word', 10))], 'en'));

SELECT 'Multiple rows.';

SELECT '- Multiple rows from a String column.';
SELECT stemmer(w, 'en') FROM (SELECT arrayJoin(['blessing', 'disguise', 'running']) AS w);

SELECT '- Multiple rows from an Array(String) column.';
SELECT stemmer(arr, 'en') FROM (SELECT arrayJoin([['blessing', 'disguise'], ['running', 'faster']]) AS arr);

SELECT '- Multiple rows from a Nullable(String) column.';
SELECT stemmer(w, 'en') FROM (SELECT arrayJoin([toNullable('blessing'), NULL, toNullable('running')]) AS w);

SELECT 'Non-English languages.';

SELECT '- German stemming.';
SELECT stemmer('Häuser', 'de');

SELECT '- French stemming.';
SELECT stemmer(['mangeons', 'finissons'], 'fr');

SELECT 'Table tests.';

SELECT '- Table with a String column.';
CREATE TABLE stem_test_str (word String) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_str VALUES ('blessing'), ('disguise'), ('running'), ('faster');
SELECT stemmer(word, 'en') FROM stem_test_str ORDER BY word;
DROP TABLE stem_test_str;

SELECT '- Table with a FixedString column.';
CREATE TABLE stem_test_fstr (word FixedString(15)) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_fstr VALUES ('blessing'), ('disguise'), ('running');
SELECT stemmer(word, 'en') FROM stem_test_fstr ORDER BY word;
DROP TABLE stem_test_fstr;

SELECT '- Table with a Nullable(String) column.';
CREATE TABLE stem_test_null (word Nullable(String)) ENGINE = MergeTree ORDER BY word SETTINGS allow_nullable_key = 1;
INSERT INTO stem_test_null VALUES ('blessing'), (NULL), ('running');
SELECT stemmer(word, 'en') FROM stem_test_null ORDER BY word;
DROP TABLE stem_test_null;

SELECT '- Table with an Array(String) column.';
CREATE TABLE stem_test_arr (words Array(String)) ENGINE = MergeTree ORDER BY words;
INSERT INTO stem_test_arr VALUES (['blessing', 'disguise']), (['running', 'faster']);
SELECT stemmer(words, 'en') FROM stem_test_arr ORDER BY words;
DROP TABLE stem_test_arr;

SELECT '- Table with an Array(Nullable(String)) column.';
CREATE TABLE stem_test_arr_null (words Array(Nullable(String))) ENGINE = MergeTree ORDER BY words SETTINGS allow_nullable_key = 1;
INSERT INTO stem_test_arr_null VALUES (['blessing', NULL, 'running']), (['faster', NULL]);
SELECT stemmer(words, 'en') FROM stem_test_arr_null ORDER BY words;
DROP TABLE stem_test_arr_null;

SELECT '- Table with a LowCardinality(String) column.';
CREATE TABLE stem_test_lc (word LowCardinality(String)) ENGINE = MergeTree ORDER BY word;
INSERT INTO stem_test_lc VALUES ('blessing'), ('disguise'), ('blessing');
SELECT stemmer(word, 'en') FROM stem_test_lc ORDER BY word;
DROP TABLE stem_test_lc;

SELECT 'Negative tests.';

SELECT '- Whitespace in a String input raises BAD_ARGUMENTS.';
SELECT stemmer('hello world', 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Whitespace in an Array element raises BAD_ARGUMENTS.';
SELECT stemmer(['hello', 'hello world'], 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Whitespace in a FixedString input raises BAD_ARGUMENTS.';
SELECT stemmer(toFixedString('hello world', 15), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Whitespace inside Array(Nullable(String)) raises BAD_ARGUMENTS.';
SELECT stemmer([toNullable('hello world')], 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Unsupported language raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stemmer('word', 'xx'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '- Wrong type for first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stemmer(42, 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '- Wrong type for second argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stemmer('word', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '- Tab character in a String input raises BAD_ARGUMENTS.';
SELECT stemmer(concat('hello', char(9), 'world'), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Newline character in a String input raises BAD_ARGUMENTS.';
SELECT stemmer(concat('hello', char(10), 'world'), 'en'); -- { serverError BAD_ARGUMENTS }

SELECT '- Array(UInt32) as first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stemmer([1, 2, 3], 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '- Array(Nullable(UInt32)) as first argument raises ILLEGAL_TYPE_OF_ARGUMENT.';
SELECT stemmer([toNullable(1)], 'en'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '- Calling without the experimental setting raises SUPPORT_IS_DISABLED.';
SET allow_experimental_nlp_functions = 0;
SELECT stemmer('blessing', 'en'); -- { serverError SUPPORT_IS_DISABLED }
