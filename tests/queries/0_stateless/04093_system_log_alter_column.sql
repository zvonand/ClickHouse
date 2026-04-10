-- Tags: no-parallel
-- Verify that system log tables can be flushed after ALTER TABLE adds a column.
-- This reproduces the bug where SystemLog::flushImpl would fail with
-- "Invalid number of columns in chunk pushed to OutputPort" because the INSERT
-- did not specify an explicit column list.

SET log_processors_profiles = 1;

SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS processors_profile_log;

ALTER TABLE system.processors_profile_log ADD COLUMN IF NOT EXISTS extra_test_column UInt64 DEFAULT 0;

SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS processors_profile_log;

SELECT count() > 0 FROM system.processors_profile_log;

ALTER TABLE system.processors_profile_log DROP COLUMN IF EXISTS extra_test_column;
