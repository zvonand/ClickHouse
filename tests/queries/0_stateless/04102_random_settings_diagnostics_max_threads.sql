-- This test is designed to fail when max_joined_block_size_rows is randomized away from its default,
-- so that --diagnose-random-settings can identify the culprit setting.
SHOW SETTING max_joined_block_size_rows;
