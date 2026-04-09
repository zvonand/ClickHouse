SELECT 'Negative tests';
-- Must accept two to three arguments
SELECT matchPhrase(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT matchPhrase('a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT matchPhrase('a', 'b', 'c', 'd'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- 1st arg must be String or FixedString
SELECT matchPhrase(1, 'hello'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- 2nd arg must be const String
SELECT matchPhrase('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT matchPhrase('a', materialize('b')); -- { serverError ILLEGAL_COLUMN }
-- 3rd arg (if given) must be const String (tokenizer name)
SELECT matchPhrase('a', 'b', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT matchPhrase('a', 'b', 'unsupported_tokenizer'); -- { serverError BAD_ARGUMENTS }
-- sparseGrams is not supported because gram ordering depends on context
SELECT matchPhrase('a', 'b', 'sparseGrams'); -- { serverError BAD_ARGUMENTS }
SELECT matchPhrase('a', 'b', 'array'); -- { serverError BAD_ARGUMENTS }
-- NULL arguments
SELECT matchPhrase(NULL); -- { serverError BAD_ARGUMENTS }
SELECT matchPhrase(NULL, NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT matchPhrase(NULL, 'quick brown');
SELECT matchPhrase('the quick brown fox', NULL); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Constants: matchPhrase should be constant';

SELECT 'Default tokenizer (splitByNonAlpha)';

SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'quick brown');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'quick brown fox');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'the lazy dog');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'the quick brown fox jumps over the lazy dog');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'quick fox');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'brown quick');
SELECT matchPhrase('the quick brown fox jumps over the lazy dog', 'dog lazy');
SELECT matchPhrase('the quick brown fox', 'quick');
SELECT matchPhrase('the quick brown fox', 'missing');
SELECT matchPhrase('the the the quick', 'the quick');
SELECT matchPhrase('the the the quick', 'the the');
SELECT matchPhrase('the the the quick', 'the the the');
SELECT matchPhrase('the the the quick', 'the the the quick');
SELECT matchPhrase('the the the quick', 'the the the the');
SELECT '-- correct failure handling';
SELECT matchPhrase('a a a b', 'a a b');
SELECT matchPhrase('a b a b a b c', 'a b a b c');
SELECT matchPhrase('x x x x y', 'x x y');
SELECT matchPhrase('hello world', 'hello world');
SELECT matchPhrase('hello world', 'hello');
SELECT matchPhrase('hello world', 'world');
SELECT matchPhrase('hello---world...foo', 'hello world');
SELECT matchPhrase('one,two;three!four', 'two three');
SELECT matchPhrase('hello world', '');
SELECT matchPhrase('', 'hello');
SELECT matchPhrase('', '');
SELECT matchPhrase('hello world', '!!!');
SELECT '-- tokenizer separators in phrase are removed before matching';
SELECT matchPhrase('error: connection refused', 'error---connection');
SELECT matchPhrase('error: connection refused', 'error:connection');
SELECT matchPhrase('one two three', 'one...two...three');
SELECT matchPhrase('one two three', 'one!@#two$%^three');
SELECT '-- FixedString input';
SELECT matchPhrase(toFixedString('the quick brown fox', 19), 'quick brown');
SELECT '-- non-const input';
SELECT matchPhrase(materialize('the quick brown fox'), 'quick brown');
SELECT '-- Nullable(String) input';
SELECT matchPhrase(toNullable('the quick brown fox'), 'quick brown');
SELECT matchPhrase(CAST(NULL AS Nullable(String)), 'quick brown');
SELECT '-- Nullable(FixedString) input';
SELECT matchPhrase(toNullable(toFixedString('the quick brown fox', 19)), 'quick brown');
SELECT matchPhrase(CAST(NULL AS Nullable(FixedString(19))), 'quick brown');

SELECT '-- Nullable(String) column values';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message Nullable(String)) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, NULL), (3, 'quick brown eyes'), (4, NULL), (5, 'no match here');

SELECT matchPhrase(message, 'quick brown') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT '-- Nullable(FixedString) column values';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message Nullable(FixedString(50))) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES (1, 'the quick brown fox'), (2, NULL), (3, 'quick brown eyes'), (4, NULL), (5, 'no match here');

SELECT matchPhrase(message, 'quick brown') FROM tab ORDER BY id;

DROP TABLE tab;

SELECT 'splitByString tokenizer';

SELECT matchPhrase('one::two::three::four', 'two::three', 'splitByString([\'::\'])');
SELECT matchPhrase('one::two::three::four', 'two::four', 'splitByString([\'::\'])');
SELECT matchPhrase('()a()bc()d()', 'a()bc', 'splitByString([\'()\'])');
SELECT matchPhrase('()a()bc()d()', 'a()d', 'splitByString([\'()\'])');
SELECT '-- tokenizer separators in phrase are removed before matching';
SELECT matchPhrase('one::two::three::four', 'two::::three', 'splitByString([\'::\'])');
SELECT matchPhrase('one::two::three::four', 'two::::::three', 'splitByString([\'::\'])');

SELECT 'ngrams tokenizer';

SELECT matchPhrase('abcdef', 'bcd', 'ngrams(3)');
SELECT matchPhrase('abcdef', 'abc', 'ngrams(3)');
SELECT matchPhrase('abcdef', 'cde', 'ngrams(3)');

SELECT 'Column values: matchPhrase should be non-constant';

SELECT 'Default tokenizer (splitByNonAlpha)';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'the quick brown fox jumps over the lazy dog'),
    (2, 'a fast red car drove past the old house'),
    (3, 'clickhouse is a fast analytical database'),
    (4, 'the brown quick fox'),
    (5, 'quick brown foxes are fast');

SELECT id FROM tab WHERE matchPhrase(message, 'quick brown') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'fast analytical database') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'brown quick') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'the lazy dog') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'missing phrase') ORDER BY id;

DROP TABLE tab;

SELECT 'splitByString tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, '()a()bc()d()'),
    (2, '()d()bc()a()'),
    (3, '()a()d()');

SELECT id FROM tab WHERE matchPhrase(message, 'a()bc', 'splitByString([\'()\'])') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'bc()d', 'splitByString([\'()\'])') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'a()d', 'splitByString([\'()\'])') ORDER BY id;

DROP TABLE tab;

SELECT 'ngrams tokenizer';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt64, message String) ENGINE = MergeTree() ORDER BY id;
INSERT INTO tab VALUES
    (1, 'abcdef'),
    (2, 'ghijkl'),
    (3, 'abcxyz');

SELECT id FROM tab WHERE matchPhrase(message, 'abc', 'ngrams(3)') ORDER BY id;
SELECT id FROM tab WHERE matchPhrase(message, 'cde', 'ngrams(3)') ORDER BY id;

DROP TABLE tab;
