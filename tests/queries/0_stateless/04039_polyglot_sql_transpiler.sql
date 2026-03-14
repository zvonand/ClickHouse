-- Tags: no-fasttest
-- no-fasttest: polyglot requires Rust build

SET allow_experimental_polyglot_dialect = 1;

-- Test basic SQLite dialect transpilation
SET dialect = 'polyglot', polyglot_dialect = 'sqlite';
SELECT IFNULL(1, 2);

-- Test MySQL dialect transpilation
SET polyglot_dialect = 'mysql';
SELECT IFNULL(1, 2);

-- Test PostgreSQL dialect transpilation
SET polyglot_dialect = 'postgresql';
SELECT COALESCE(1, 2);

-- Test switching back to clickhouse dialect
SET dialect = 'clickhouse';
SELECT 1;

-- Test that polyglot dialect requires the experimental setting
SET allow_experimental_polyglot_dialect = 0;
SET dialect = 'polyglot', polyglot_dialect = 'sqlite';
SELECT 1; -- { serverError SUPPORT_IS_DISABLED }
