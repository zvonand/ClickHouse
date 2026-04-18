-- This test fails roughly 50% of the time - diagnostics step 1 should detect
-- partial failures and label it "flaky". The reference is 1 (the most common
-- result), so when rand() < 2147483647 returns 0 the test fails.
SELECT rand() < 2147483647 AS x;
