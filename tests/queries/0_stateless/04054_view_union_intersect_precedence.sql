-- https://github.com/ClickHouse/ClickHouse/issues/99257
-- INTERSECT has higher precedence than UNION, and this must be preserved after DETACH/ATTACH.

DROP TABLE IF EXISTS v0;

CREATE VIEW v0 AS (SELECT 1 c0, 1 c1 UNION DISTINCT SELECT 2, 2 INTERSECT SELECT 3, 3);

SELECT * FROM v0;
SELECT '---';

DETACH TABLE v0 SYNC;
ATTACH TABLE v0;

SELECT * FROM v0;

DROP TABLE v0;
