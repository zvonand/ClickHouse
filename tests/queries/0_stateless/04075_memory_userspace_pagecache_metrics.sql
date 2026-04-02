-- Verify MemoryUserSpacePageCache is present in system.asynchronous_metrics.
SELECT count() > 0 FROM system.asynchronous_metrics WHERE metric = 'MemoryUserSpacePageCache';

-- When CGroup metrics are available, verify the invariant:
--   CGroupMemoryUsedWithoutPageCache <= CGroupMemoryUsed
-- (the without-page-cache variant can only be equal or smaller)
WITH
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsed') AS cgroup_used,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'CGroupMemoryUsedWithoutPageCache') AS cgroup_used_wo_pc
SELECT
    cgroup_used_wo_pc <= cgroup_used
WHERE cgroup_used > 0 AND cgroup_used_wo_pc > 0;
