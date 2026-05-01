-- Regression test: with `max_bytes_before_external_join` set, the in-memory peak of
-- a spilling hash join must stay below `max_memory_usage`. Previously several issues
-- in `ConcurrentHashJoin`, `SpillingHashJoin` and `GraceHashJoin` could conspire to
-- make the in-memory hash table grow well past the configured external-join
-- threshold, causing a `MEMORY_LIMIT_EXCEEDED` exception during the build phase.

-- The right side here totals ~150 MiB (1M rows × ~150 bytes per row), which is well
-- above `max_bytes_before_external_join = 80 MiB`. With the fixes in place, the
-- query stays under `max_memory_usage = 200 MiB` and completes successfully.

SET max_memory_usage = '200Mi';
SET max_bytes_before_external_join = '80Mi';
SET grace_hash_join_initial_buckets = 1;

SELECT 'single-thread hash';
SET join_algorithm = 'hash';
SET max_threads = 1;
SELECT count()
FROM (SELECT number AS k FROM numbers(1000000)) AS t1
INNER JOIN (SELECT number AS k, randomPrintableASCII(140) AS s FROM numbers(1000000)) AS t2
USING (k);

SELECT 'concurrent parallel_hash';
SET join_algorithm = 'parallel_hash';
SET max_threads = 4;
SELECT count()
FROM (SELECT number AS k FROM numbers(1000000)) AS t1
INNER JOIN (SELECT number AS k, randomPrintableASCII(140) AS s FROM numbers(1000000)) AS t2
USING (k);
