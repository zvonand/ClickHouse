-- Tags: no-parallel
-- This test is designed to fail when max_block_size or max_joined_block_size_rows
-- is randomized away from its default, so that --diagnose-random-settings can
-- identify both culprit settings.
SHOW SETTING max_block_size;
SHOW SETTING max_joined_block_size_rows;
