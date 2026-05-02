-- Regression test: `max_bytes_before_external_join` must be enforced for
-- thresholds below the value that was previously hardcoded as a floor in
-- `GraceHashJoin::hasMemoryOverflow`. With a low threshold the build-side
-- bucket of the in-memory join inside `GraceHashJoin` would otherwise grow
-- past `max_memory_usage` until allocation tracking aborts the query.

SET max_memory_usage = '160Mi';
SET max_bytes_before_external_join = '16Mi';
SET grace_hash_join_initial_buckets = 1;

SELECT 'single-thread hash low threshold';
SET join_algorithm = 'hash';
SET max_threads = 1;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);

SELECT 'concurrent parallel_hash low threshold';
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;
SELECT count()
FROM (SELECT number AS k FROM numbers(2000000)) AS t1
INNER JOIN (SELECT number AS k FROM numbers(2000000)) AS t2
USING (k);
