-- Issue #104605: parsing `f(x, y -> z)` silently merged the leading args into the
-- lambda's left-hand side, producing `f((x, y) -> z)` — a one-argument call. The
-- formatted query then could not be re-parsed, triggering the
-- `Inconsistent AST formatting` `LOGICAL_ERROR` round-trip check.

-- The original reproducer from the issue. We don't care about the result —
-- only that the server doesn't abort with `LOGICAL_ERROR` and the query
-- round-trips cleanly. `UNKNOWN_IDENTIFIER` is fine.
SELECT substring(x, `x` -> `x`); -- { serverError UNKNOWN_IDENTIFIER }

-- The buggy parser silently turned these multi-argument calls into a
-- single-argument lambda. Now they preserve their original arity and produce
-- a deterministic AST shape, so they no longer trigger the round-trip error.
-- The error is `UNKNOWN_IDENTIFIER`, `NUMBER_OF_ARGUMENTS_DOESNT_MATCH`, or
-- another semantic error depending on the function — the important thing is
-- that the server does not crash and the formatted query re-parses.
SELECT f(a, x -> y); -- { serverError UNKNOWN_IDENTIFIER }
SELECT f(a, b, c -> d); -- { serverError UNKNOWN_IDENTIFIER }
SELECT substring(s, x -> x); -- { serverError UNKNOWN_IDENTIFIER }
SELECT arrayMap(arr, x -> x + 1); -- { serverError UNKNOWN_IDENTIFIER }

-- Round-trip check using `formatQuerySingleLine`. The formatted form must be
-- parseable and must produce the same AST shape on re-parse.
SELECT formatQuerySingleLine('SELECT substring(x, `x` -> `x`)');
SELECT formatQuerySingleLine('SELECT f(a, x -> y)');
SELECT formatQuerySingleLine('SELECT f(a, b, c -> d)');

-- Legitimate lambda usages must keep working. The lambda is the first argument
-- of a higher-order function — the standard position in ClickHouse.
SELECT arrayMap(x -> x + 1, [1, 2, 3]);
SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30]);
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);

-- Lambdas can also appear as a non-first argument when wrapped in explicit
-- parentheses — this form is unambiguous and round-trips cleanly.
SELECT formatQuerySingleLine('SELECT f(a, (x, y) -> z)');

-- Standalone parenthesized lambdas (not inside a function call) keep working.
SELECT formatQuerySingleLine('SELECT (x -> x + 1)');
