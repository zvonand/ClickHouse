-- Regression test: Pretty format with 0-row chunks should not throw std::length_error.
-- https://github.com/ClickHouse/ClickHouse/issues/99528
SELECT 1 FROM system.asynchronous_inserts FORMAT Pretty;
SELECT 1 FROM system.asynchronous_inserts FORMAT PrettyCompact;
SELECT database FROM system.asynchronous_inserts FORMAT PrettySpace;
