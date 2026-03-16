-- Regression test: Pretty format with 0-row chunks should not throw std::length_error.
-- https://github.com/ClickHouse/ClickHouse/issues/99528
SELECT 1 WHERE 0 FORMAT Pretty;
SELECT 1 WHERE 0 FORMAT PrettyCompact;
SELECT 1 WHERE 0 FORMAT PrettySpace;
