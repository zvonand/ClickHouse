-- Verify CGroupMemoryUsedWithoutPageCache is present and <= CGroupMemoryUsed.
-- Snapshot both metrics in a single read to avoid races between asynchronous_metrics updates.
-- Uses if() to produce a deterministic result regardless of cgroup availability.
WITH
    (SELECT groupArray((metric, value)) FROM system.asynchronous_metrics
     WHERE metric IN ('CGroupMemoryUsed', 'CGroupMemoryUsedWithoutPageCache')) AS metrics,
    arrayFirst(x -> x.1 = 'CGroupMemoryUsed', metrics) AS used,
    arrayFirst(x -> x.1 = 'CGroupMemoryUsedWithoutPageCache', metrics) AS without_cache
SELECT
    if(used.2 > 0, without_cache.2 > 0, 1),
    if(used.2 > 0, without_cache.2 <= used.2, 1);
