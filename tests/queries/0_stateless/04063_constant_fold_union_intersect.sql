-- Regression test: constant folded from UNION (INTERSECT ALL) node should not
-- cause "Invalid action query tree node" exception in calculateActionNodeName.
SELECT min(*) FROM (SELECT number FROM numbers(10)) INTERSECT ALL SELECT min(*) FROM (SELECT number FROM numbers(10));
