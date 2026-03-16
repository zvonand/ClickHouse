-- Tags: no-old-analyzer
-- The old analyzer cannot constant-fold arrayMap(x -> toString(x), range(256)),
-- so the needles argument is not treated as constant and a different error is thrown.

SELECT '-- Error: too many needles';
SELECT highlight('text', arrayMap(x -> toString(x), range(256))); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

SELECT '-- Error: too many matches per row';
SELECT highlight('aaa', ['a']) SETTINGS highlight_max_matches_per_row = 1; -- { serverError LIMIT_EXCEEDED }

SELECT '-- OK: increase matches limit';
SELECT highlight('aaa', ['a']) SETTINGS highlight_max_matches_per_row = 10;
