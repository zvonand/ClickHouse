-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103120
--
-- `SET <name> = DEFAULT` used to lose the `is_auto` flag for `SettingFieldMaxThreads`-typed
-- settings (`max_threads`, `max_final_threads`, `max_parsing_threads`). The cause was that
-- `BaseSettings::resetValueToDefault` round-tripped through `static_cast<Field>(*default)`,
-- which for `SettingFieldMaxThreads` returns the resolved auto-value (an opaque `UInt64`),
-- so the subsequent `operator=(const Field &)` reconstructed the field as if it had been
-- set to that explicit value.
--
-- We use `startsWith(value, '''auto(')` to assert that the auto state is preserved without
-- depending on the actual core count.
--
-- 1. Initial state: undo any session-level value the test runner may have injected via
--    randomized settings, then assert auto. This step itself exercises the fix - on a
--    buggy build the reset would return the resolved value instead of auto.
SET max_threads = DEFAULT;
SET max_final_threads = DEFAULT;
SET max_parsing_threads = DEFAULT;
SELECT 'max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 2. Setting an explicit value clears auto.
SET max_threads = 4;
SET max_final_threads = 5;
SET max_parsing_threads = 6;
SELECT 'after-set max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'after-set max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'after-set max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 3. SET = DEFAULT must restore auto for all three settings.
SET max_threads = DEFAULT;
SET max_final_threads = DEFAULT;
SET max_parsing_threads = DEFAULT;
SELECT 'after-default max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'after-default max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'after-default max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 4. Resetting an already-auto setting must keep it auto.
SET max_threads = DEFAULT;
SELECT 'idempotent max_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';

-- 5. Multiple-setting form `SET a = DEFAULT, b = DEFAULT` must restore auto for all.
SET max_threads = 7, max_final_threads = 8, max_parsing_threads = 9;
SET max_threads = DEFAULT, max_final_threads = DEFAULT, max_parsing_threads = DEFAULT;
SELECT 'multi-default max_threads',         startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_threads';
SELECT 'multi-default max_final_threads',   startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_final_threads';
SELECT 'multi-default max_parsing_threads', startsWith(value, '''auto(') FROM system.settings WHERE name = 'max_parsing_threads';

-- 6. Control: a `SettingAutoWrapper`-style setting (`query_plan_join_swap_table`) preserves
--    the literal `auto` keyword across DEFAULT - was already correct, must remain correct.
SET query_plan_join_swap_table = 'true';
SELECT 'autowrapper-set', value FROM system.settings WHERE name = 'query_plan_join_swap_table';
SET query_plan_join_swap_table = DEFAULT;
SELECT 'autowrapper-default', value FROM system.settings WHERE name = 'query_plan_join_swap_table';

-- 7. Control: a regular numeric setting (`max_block_size`) is `changed=0` after DEFAULT.
SET max_block_size = 12345;
SELECT 'control changed', changed FROM system.settings WHERE name = 'max_block_size';
SET max_block_size = DEFAULT;
SELECT 'control default-changed', changed FROM system.settings WHERE name = 'max_block_size';
