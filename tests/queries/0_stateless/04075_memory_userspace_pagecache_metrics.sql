-- Verify CGroupMemoryUsedWithoutPageCache is present when CGroup metrics are available.
-- Uses if() to produce a deterministic result regardless of cgroup availability.
SELECT if(
    (SELECT count() FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsed') > 0,
    (SELECT count() FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsedWithoutPageCache') > 0,
    1);

-- When CGroup metrics are available, verify the invariant:
--   CGroupMemoryUsedWithoutPageCache <= CGroupMemoryUsed
-- Uses if() to produce a deterministic result regardless of cgroup availability.
SELECT if(
    (SELECT count() FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsed') > 0
    AND (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsed') > 0,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsedWithoutPageCache')
        <= (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsed'),
    1);
