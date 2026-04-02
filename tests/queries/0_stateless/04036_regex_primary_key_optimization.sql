-- { echo }

-- Sorted: aaa-1, aaa-2, aab-1, bbb-1, bbb-2, ccc-1 (6 rows/granules)
DROP TABLE IF EXISTS test_regex_prefix;
CREATE TABLE test_regex_prefix (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_prefix VALUES ('aaa-1'), ('aaa-2'), ('aab-1'), ('bbb-1'), ('bbb-2'), ('ccc-1');

-- No optimization: empty regex
SELECT count() FROM test_regex_prefix WHERE match(id, '') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: only '^'
SELECT count() FROM test_regex_prefix WHERE match(id, '^') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: no '^' anchor
SELECT count() FROM test_regex_prefix WHERE match(id, 'aaa') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

-- Simple literal prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;

-- Longer literal prefix, tighter pruning
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa-1')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa-1') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa-1') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;

-- NUL bytes in regex have undefined behavior: no optimization attempted
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\0bbb') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

-- Escaped special chars become literal prefix characters
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\.')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\.') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\.') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\|')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\|') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\|') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\(')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\(') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\(') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\)') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\\\')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\\\') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\\\') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\{')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\{') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\{') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\^')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\^') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\^') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\$')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\$') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\$') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\[')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\[') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\[') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\]')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\]') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\]') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\?')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\?') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\?') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\*')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\*') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\*') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\+')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\+') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\+') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\}')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\}') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\}') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- Escaped dash: prefix "aaa-" matches 2 rows
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\-')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\-') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\-') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Non-literal escape sequences: prefix stops before the escape
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\d')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\d') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\d') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa\\w')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa\\w') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa\\w') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;

-- Plain group: parentheses are transparent
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^(aaa)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^(aaa)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^(aaa)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Group followed by literal continues prefix extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^(aaa)-1')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^(aaa)-1') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^(aaa)-1') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- No optimization: non-capturing group (?:...)
SELECT count() FROM test_regex_prefix WHERE match(id, '^(?:aaa)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Invalid regex: malformed flag group
SELECT count() FROM test_regex_prefix WHERE match(id, '^(?iaaa)'); -- {serverError CANNOT_COMPILE_REGEXP}
-- No optimization: valid flag group (?i:...)
SELECT count() FROM test_regex_prefix WHERE match(id, '^(?i:aaa)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Nested groups are transparent
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^((aaa))')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^((aaa))') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^((aaa))') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Deep nesting with suffix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^(((aaa)))-1')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^(((aaa)))-1') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^(((aaa)))-1') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;

-- Optional group: prefix truncated to before the group
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa(-1)?')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa(-1)?') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa(-1)?') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Zero-or-more group: same truncation
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa(-1)*')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa(-1)*') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa(-1)*') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Brace quantifier {0,N}: same truncation
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa(-1){0,2}')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa(-1){0,2}') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa(-1){0,2}') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Mandatory repetition +: group content is kept in prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^(aaa)+')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^(aaa)+') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^(aaa)+') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Group followed by literal that matches nothing
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^(aaa)bbb')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^(aaa)bbb') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_prefix WHERE match(id, '^(aaa)bbb') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;

-- No optimization: unescaped '|' outside group
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa|bbb') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Metacharacter '[' stops prefix extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa[12]')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa[12]') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa[12]') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Metacharacter '^' mid-pattern stops prefix extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa^bbb')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa^bbb') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa^bbb') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Metacharacter '$' stops prefix extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa$')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa$') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa$') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Metacharacter '.' stops prefix extraction
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa.')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa.') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa.') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Metacharacter '+' stops extraction but keeps last char (1-or-more)
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa+')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa+') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa+') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;

-- '?' pops last char from prefix, widening the range
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa?')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa?') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa?') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- '*' pops last char
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa*')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa*') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa*') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- '{0,...}' pops last char
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_prefix WHERE match(id, '^aaa{0,5}')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_prefix WHERE match(id, '^aaa{0,5}') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_prefix WHERE match(id, '^aaa{0,5}') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;

DROP TABLE test_regex_prefix;

-- Sorted: xxa-1, xxa-2, xxb-1, yyy-1 (4 rows/granules)
DROP TABLE IF EXISTS test_regex_safety;
CREATE TABLE test_regex_safety (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_safety VALUES ('xxa-1'), ('xxa-2'), ('xxb-1'), ('yyy-1');

-- Alternation inside group with prefix before group: prefix kept up to group start
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_safety WHERE match(id, '^xx(a.*|b.*)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_safety WHERE match(id, '^xx(a.*|b.*)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_safety WHERE match(id, '^xx(a.*|b.*)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Group with wildcard but no alternation: full prefix through the group
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_safety WHERE match(id, '^(xxa.*)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_safety WHERE match(id, '^(xxa.*)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_safety WHERE match(id, '^(xxa.*)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Escaped '|' is literal, not alternation: full prefix preserved
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_safety WHERE match(id, '^(xxa.*\\|z)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_safety WHERE match(id, '^(xxa.*\\|z)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_safety WHERE match(id, '^(xxa.*\\|z)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;

DROP TABLE test_regex_safety;

-- Sorted: abc-xx-1, abc-xx-2, abc-yy-1, def-zz-1 (4 rows/granules)
DROP TABLE IF EXISTS test_regex_altern;
CREATE TABLE test_regex_altern (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_altern VALUES ('abc-xx-1'), ('abc-xx-2'), ('abc-yy-1'), ('def-zz-1');

-- Alternation with common prefix across branches
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- No optimization: branches with no common prefix
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|def)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Three branches
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx-1|abc-xx-2|abc-yy-1)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx-1|abc-xx-2|abc-yy-1)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx-1|abc-xx-2|abc-yy-1)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Branches with different lengths
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-y)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-y)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-y)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- No optimization: first branch has wildcard prefix
SELECT count() FROM test_regex_altern WHERE match(id, '^(.*abc|def-zz)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: second branch has wildcard prefix
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|.*def)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

-- Escaped chars inside alternation branches
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc\\-xx|abc\\-yy)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc\\-xx|abc\\-yy)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc\\-xx|abc\\-yy)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Nested groups inside alternation branches
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc(-xx)|abc(-yy))')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc(-xx)|abc(-yy))') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc(-xx)|abc(-yy))') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- No optimization: alternation inside nested group makes branch prefix empty
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc(x|y)|def(z|w))') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: '|' inside character class is literal, but branches still have no common prefix
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc[|]x|def[|]z)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- ')' inside negated character class does not close the group
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc[^)]x|abc[^)]y)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc[^)]x|abc[^)]y)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc[^)]x|abc[^)]y)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- ']' at start of character class is literal
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc[]|]x|abc[]|]y)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc[]|]x|abc[]|]y)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc[]|]x|abc[]|]y)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Escaped chars inside character class
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc[\\|]x|abc[\\|]y)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc[\\|]x|abc[\\|]y)') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc[\\|]x|abc[\\|]y)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;

-- No optimization: optional alternation group
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|def)?') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|def)*') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|def){0,1}') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Mandatory repetition: optimization applies
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)+')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)+') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)+') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy){1,3}')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy){1,3}') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy){1,3}') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Literal after group does not affect prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)zzz')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)zzz') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_altern WHERE match(id, '^(abc-xx|abc-yy)zzz') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;

-- Invalid regex: unclosed group
SELECT count() FROM test_regex_altern WHERE match(id, '^(abc|def'); -- {serverError CANNOT_COMPILE_REGEXP}
-- No optimization: single branch with wildcard
SELECT count() FROM test_regex_altern WHERE match(id, '^(.*)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_altern;

-- Sorted: aaa-1, bbb-1 (2 rows/granules)
DROP TABLE IF EXISTS test_regex_handler;
CREATE TABLE test_regex_handler (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_handler VALUES ('aaa-1'), ('bbb-1');

-- Direct prefix extraction succeeds
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_handler WHERE match(id, '^aaa')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_handler WHERE match(id, '^aaa') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_handler WHERE match(id, '^aaa') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- No optimization: expression too short for alternation analysis
SELECT count() FROM test_regex_handler WHERE match(id, '^.') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: top-level alternation
SELECT count() FROM test_regex_handler WHERE match(id, '^aaa|bbb') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: non-capturing group
SELECT count() FROM test_regex_handler WHERE match(id, '^(?:aaa|bbb)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- Alternation analysis finds common prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_handler WHERE match(id, '^(aaa|aab)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_handler WHERE match(id, '^(aaa|aab)') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_handler WHERE match(id, '^(aaa|aab)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- No optimization: alternation with no common prefix
SELECT count() FROM test_regex_handler WHERE match(id, '^(aaa|bbb)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_handler;

-- Regression test: alternation branches with wildcards and a shared suffix
-- longer than the branch prefixes. Must not use the suffix as a key prefix.
-- Sorted: LONGMATCH-other, ab-LONGMATCH, cd-LONGMATCH (3 rows/granules)
DROP TABLE IF EXISTS test_regex_regression;
CREATE TABLE test_regex_regression (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_regression VALUES ('ab-LONGMATCH'), ('cd-LONGMATCH'), ('LONGMATCH-other');

-- No common prefix between "ab" and "cd": must scan all granules
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)');
SELECT * FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)') ORDER BY id;
-- Same result with pruning disabled
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)') SETTINGS use_primary_key = 0) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)') SETTINGS use_primary_key = 0;
SELECT * FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)') ORDER BY id SETTINGS use_primary_key = 0;
-- Optional group matches everything
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)?')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)?');
SELECT * FROM test_regex_regression WHERE match(id, '^(ab.*LONGMATCH|cd.*LONGMATCH)?') ORDER BY id;
-- Branches with wildcards sharing a common prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_regression WHERE match(id, '^(ab.*X|ab.*Y)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_regression WHERE match(id, '^(ab.*X|ab.*Y)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_regression WHERE match(id, '^(ab.*X|ab.*Y)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;

DROP TABLE test_regex_regression;

-- Data with special characters: aa.bb, aabb, aa|bb, bbcc (4 rows/granules)
DROP TABLE IF EXISTS test_regex_special;
CREATE TABLE test_regex_special (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_special VALUES ('aa.bb'), ('aa|bb'), ('aabb'), ('bbcc');

-- Escaped dot matches literal '.'
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_special WHERE match(id, '^aa\\.')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_special WHERE match(id, '^aa\\.') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_special WHERE match(id, '^aa\\.') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- Escaped pipe matches literal '|'
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_special WHERE match(id, '^aa\\|')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_special WHERE match(id, '^aa\\|') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_special WHERE match(id, '^aa\\|') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Escaped backslash matches literal '\'
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_special WHERE match(id, '^aa\\\\')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_special WHERE match(id, '^aa\\\\') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_special WHERE match(id, '^aa\\\\') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE test_regex_special;

-- Sorted: aaa, aab, bbb (3 rows/granules)
DROP TABLE IF EXISTS test_regex_edge;
CREATE TABLE test_regex_edge (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_edge VALUES ('aaa'), ('aab'), ('bbb');

-- Group followed by literal that matches nothing in the range
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_edge WHERE match(id, '^(aaa)b')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_edge WHERE match(id, '^(aaa)b') SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT * FROM test_regex_edge WHERE match(id, '^(aaa)b') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 1;
-- Quantifier inside group pops last char from prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_edge WHERE match(id, '^(aab*)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_edge WHERE match(id, '^(aab*)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_edge WHERE match(id, '^(aab*)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Three branches with progressively shrinking common prefix
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_edge WHERE match(id, '^(aaax|aaby|aacz)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_edge WHERE match(id, '^(aaax|aaby|aacz)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_edge WHERE match(id, '^(aaax|aaby|aacz)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- Common prefix equals the shorter branch
SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_edge WHERE match(id, '^(aa|aab)')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_edge WHERE match(id, '^(aa|aab)') SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT * FROM test_regex_edge WHERE match(id, '^(aa|aab)') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 2;
-- No optimization: quantifier on empty prefix
SELECT count() FROM test_regex_edge WHERE match(id, '^?') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_edge;

-- Malformed and non-optimizable patterns
DROP TABLE IF EXISTS test_regex_negative;
CREATE TABLE test_regex_negative (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_negative VALUES ('abc'), ('def');

-- Invalid regex
SELECT count() FROM test_regex_negative WHERE match(id, '^abc)'); -- {serverError CANNOT_COMPILE_REGEXP}
SELECT count() FROM test_regex_negative WHERE match(id, '^abc\\'); -- {serverError CANNOT_COMPILE_REGEXP}
SELECT count() FROM test_regex_negative WHERE match(id, '^abc[def'); -- {serverError CANNOT_COMPILE_REGEXP}
SELECT count() FROM test_regex_negative WHERE match(id, '^(abc'); -- {serverError CANNOT_COMPILE_REGEXP}
-- No optimization: empty alternation branch matches everything
SELECT count() FROM test_regex_negative WHERE match(id, '^(abc|)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
SELECT count() FROM test_regex_negative WHERE match(id, '^(|abc)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: wildcard at start
SELECT count() FROM test_regex_negative WHERE match(id, '^.*abc') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: character class at start
SELECT count() FROM test_regex_negative WHERE match(id, '^[abc]def') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: dot at start
SELECT count() FROM test_regex_negative WHERE match(id, '^.abc') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: disjoint branches
SELECT count() FROM test_regex_negative WHERE match(id, '^(abc|def|ghi)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: top-level alternation
SELECT count() FROM test_regex_negative WHERE match(id, '^abc|^def') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: no anchor
SELECT count() FROM test_regex_negative WHERE match(id, '(abc|def)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}
-- No optimization: non-capturing group
SELECT count() FROM test_regex_negative WHERE match(id, '^(?:abc|def)') SETTINGS force_primary_key = 1; -- {serverError INDEX_NOT_USED}

DROP TABLE test_regex_negative;

-- Optional group containing wildcard: "^xx(abc.*)?def"
-- The group may or may not appear, so both "xxabcZdef" and "xxdef" match.
-- Sorted: other, xxabcZdef, xxdef (3 rows/granules)
DROP TABLE IF EXISTS test_regex_optgroup;
CREATE TABLE test_regex_optgroup (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_optgroup VALUES ('xxabcZdef'), ('xxdef'), ('other');

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_optgroup WHERE match(id, '^xx(abc.*)?def')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_optgroup WHERE match(id, '^xx(abc.*)?def') SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT * FROM test_regex_optgroup WHERE match(id, '^xx(abc.*)?def') ORDER BY id SETTINGS force_primary_key = 1, max_rows_to_read = 3;
-- Confirm: same result with pruning disabled
SELECT count() FROM test_regex_optgroup WHERE match(id, '^xx(abc.*)?def') SETTINGS use_primary_key = 0;

DROP TABLE test_regex_optgroup;

-- Top-level alternation with wildcard: "^ab.*|cd"
-- Parsed as (^ab.*) | (cd) — the "cd" branch matches anywhere in the string.
-- Sorted: abXXX, mmmmcd, zzzzcd (3 rows/granules)
DROP TABLE IF EXISTS test_regex_toplevel_alt;
CREATE TABLE test_regex_toplevel_alt (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_toplevel_alt VALUES ('abXXX'), ('mmmmcd'), ('zzzzcd');

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_toplevel_alt WHERE match(id, '^ab.*|cd')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_toplevel_alt WHERE match(id, '^ab.*|cd');
SELECT * FROM test_regex_toplevel_alt WHERE match(id, '^ab.*|cd') ORDER BY id;
-- Confirm: same result with pruning disabled
SELECT count() FROM test_regex_toplevel_alt WHERE match(id, '^ab.*|cd') SETTINGS use_primary_key = 0;

DROP TABLE test_regex_toplevel_alt;

-- Regression: top-level alternation after a grouped alternation.
-- ^(ab|ac)|zz parses as (^(ab|ac))|(zz) — the "zz" branch is unanchored.
-- The alternation helper must not return a prefix from the grouped part.
-- Sorted: abXX, mmzz, zzzz (3 rows/granules)
DROP TABLE IF EXISTS test_regex_group_then_alt;
CREATE TABLE test_regex_group_then_alt (id String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_regex_group_then_alt VALUES ('abXX'), ('mmzz'), ('zzzz');

SELECT trimLeft(explain) FROM (EXPLAIN PLAN indexes=1 SELECT id FROM test_regex_group_then_alt WHERE match(id, '^(ab|ac)|zz')) WHERE explain LIKE '%Condition%' OR explain LIKE '%Granules%';
SELECT count() FROM test_regex_group_then_alt WHERE match(id, '^(ab|ac)|zz');
SELECT * FROM test_regex_group_then_alt WHERE match(id, '^(ab|ac)|zz') ORDER BY id;
-- Confirm: same result with pruning disabled
SELECT count() FROM test_regex_group_then_alt WHERE match(id, '^(ab|ac)|zz') SETTINGS use_primary_key = 0;

DROP TABLE test_regex_group_then_alt;
