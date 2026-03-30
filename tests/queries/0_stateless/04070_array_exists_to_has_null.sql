-- Verify that arrayExists(x -> x = NULL, ...) is NOT rewritten to has(..., NULL),
-- because the semantics differ: equals(NULL, NULL) is NULL (treated as false by arrayExists),
-- but has([NULL], NULL) returns 1.

SET enable_analyzer = 1;
SET optimize_rewrite_array_exists_to_has = 1;

SELECT arrayExists(x -> x = NULL, [NULL]);
SELECT arrayExists(x -> NULL = x, [NULL]);
SELECT arrayExists(x -> x = NULL, [1, 2, NULL]);
SELECT arrayExists(x -> x = NULL, [1, 2, 3]);
