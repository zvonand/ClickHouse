-- Issue #104605: queries like SELECT substring(x, `x` -> `x`) aborted the
-- server with an "Inconsistent AST formatting" LOGICAL_ERROR. The parser
-- merged the comma-separated arguments into a single lambda-with-tuple
-- argument (substring((x, x) -> x)), which the formatter emitted faithfully,
-- but the SubstringLayer parser rejected the formatted output because it did
-- not accept the one-argument form -- so the format/re-parse round-trip check
-- aborted the server.
--
-- The fix accepts the one-argument form at the parser level for substring
-- and position (the two special-cased layers that previously rejected it).
-- The query then fails at the analyzer level with a sensible error instead of
-- a LOGICAL_ERROR, and the round-trip check no longer fires.

-- Original reproducer from the issue. We don't care which error fires, only
-- that the server does not abort with LOGICAL_ERROR. The lambda has duplicate
-- parameter names (x and x), which is rejected early as BAD_ARGUMENTS.
SELECT substring(x, `x` -> `x`); -- { serverError BAD_ARGUMENTS }

-- Variants that exercise the same SubstringLayer / PositionLayer path. With
-- distinct lambda parameter names the call reaches function resolution and
-- fails with ILLEGAL_TYPE_OF_ARGUMENT (substring / position do not accept
-- lambda arguments).
SELECT substring(s, x -> x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT position(h, x -> x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Round-trip through formatQuerySingleLine must succeed (this is what the
-- internal AST round-trip check does, just from inside SQL).
SELECT formatQuerySingleLine('SELECT substring(x, `x` -> `x`)');
SELECT formatQuerySingleLine('SELECT position(h, `x` -> `x`)');

-- The one-argument forms that the round-trip check produces internally must
-- now parse and reach the analyzer (which rejects them).
SELECT substring('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT position('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Legitimate substring / position calls must keep working.
SELECT substring('abcdef', 2, 3);
SELECT substring('abcdef' FROM 2 FOR 3);
SELECT position('abcdef', 'cd');
SELECT position('cd' IN 'abcdef');

-- Legitimate higher-order function lambdas must keep working -- this is the
-- documented f(a, b -> body) == f((a, b) -> body) merging behavior used by
-- mapApply, arrayFold, etc.
SELECT arrayMap(x -> x + 1, [1, 2, 3]);
SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30]);
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);
SELECT arrayFold(acc, x -> acc + x, [1, 2, 3, 4], toUInt64(0));
SELECT mapApply((k, v) -> (k, v * 2), map(1, 10, 2, 20));
SELECT mapApply(k, v -> (k, v * 2), map(1, 10, 2, 20));
