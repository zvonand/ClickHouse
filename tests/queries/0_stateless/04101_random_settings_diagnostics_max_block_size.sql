-- Tags: no-parallel
-- This test is designed to fail when max_block_size is randomized away from its default,
-- so that --diagnose-random-settings can identify the culprit setting.
SHOW SETTING max_block_size;
