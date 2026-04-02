-- Covers JoinUsedFlags::setUsed<need_flags=true> path, which is exercised by
-- RIGHT and FULL joins when multiple left rows match a single right row.
-- Regression test for PR #99274 (reduce contention on JoinUsedFlags).

-- RIGHT JOIN: two left rows match r.k=0, two match r.k=2, right r.k=3 is unmatched
SELECT l.k, r.k, r.v
FROM (SELECT toNullable(arrayJoin([0, 0, 1, 2, 2])) AS k) AS l
RIGHT JOIN (SELECT number AS k, number * 10 AS v FROM numbers(4)) AS r
ON l.k = r.k
ORDER BY r.k, l.k NULLS LAST;

-- FULL JOIN: same data, unmatched right row still appears
SELECT l.k, r.k, r.v
FROM (SELECT toNullable(arrayJoin([0, 0, 1, 2, 2])) AS k) AS l
FULL JOIN (SELECT number AS k, number * 10 AS v FROM numbers(4)) AS r
ON l.k = r.k
ORDER BY r.k, l.k NULLS LAST;

-- RIGHT ANTI JOIN: only right rows with no left match
SELECT r.k, r.v
FROM (SELECT arrayJoin([0, 1]) AS k) AS l
RIGHT ANTI JOIN (SELECT number AS k, number * 10 AS v FROM numbers(4)) AS r
ON l.k = r.k
ORDER BY r.k;
