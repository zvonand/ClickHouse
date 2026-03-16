-- { echo }

SET enable_analyzer = 0;
SELECT 'Old Analyzer:';

SELECT 'Negative Limit By Only';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -100 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -9223372036854775808 BY g;
SELECT number, number % 3 AS g FROM numbers(1000000) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Negative Limit By and Negative Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 OFFSET -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET -100 BY g;
SELECT number, number % 3 AS g FROM numbers(1000) ORDER BY g, number LIMIT -5 OFFSET -4 BY g;
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY g, number LIMIT -5 OFFSET -1000 BY g;

SELECT 'Negative Limit By and Positive Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 OFFSET 2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET 3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -100 OFFSET 4 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 5 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 100 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -8 OFFSET 3 BY g;

SELECT 'Common (Unordered) Path';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 OFFSET 1 BY g;

SELECT 'Edge Cases';
SELECT number, number % 3 AS g FROM numbers(0) ORDER BY g, number LIMIT -2 BY g;
SELECT number, number AS g FROM numbers(5) ORDER BY g LIMIT -1 BY g;
SELECT number, 0 AS g FROM numbers(10) ORDER BY number LIMIT -3 BY g;
SELECT number, if(number % 3 = 0, NULL, number % 3) AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Combined with Regular Limit';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g LIMIT 3;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g LIMIT -3;

SELECT 'Multiple Key Columns';
SELECT number, number % 2 AS g1, number % 3 AS g2 FROM numbers(12) ORDER BY g1, g2, number LIMIT -1 BY g1, g2;

SELECT 'String Keys';
SELECT number, toString(number % 2) AS g FROM numbers(10) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET -2 BY x;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET 50 BY x;
SELECT number, number % 3 AS g FROM numbers(1000000) ORDER BY g, number LIMIT -1 BY g;
SELECT DISTINCT number % 5 AS g FROM numbers(1000000) ORDER BY g LIMIT -1 OFFSET -4 BY g;

SELECT 'Real Table';
DROP TABLE IF EXISTS neg_limit_by_tab;
CREATE TABLE neg_limit_by_tab (id UInt8, val UInt32) ENGINE = MergeTree ORDER BY (id, val)
AS SELECT number % 3 AS id, number AS val FROM numbers(30);

SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -2 BY id;
SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -3 OFFSET -2 BY id;
SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -1 OFFSET 8 BY id;

SELECT 'Big Table';
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY g, number LIMIT -3 BY g;
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY number LIMIT -3 BY g;

SELECT 'Positive Limit Negative Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 1 OFFSET -3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT 2 OFFSET -1 BY g;

SELECT 'Zero Limit and Offset Edge Cases';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 OFFSET -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET 0 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 OFFSET 2 BY g;

SELECT 'Expression LIMIT BY';
SELECT number FROM numbers(15) ORDER BY number % 3, number LIMIT -2 BY number % 3;
SELECT number FROM numbers(15) ORDER BY number % 3, number LIMIT -2 OFFSET -1 BY number % 3;

SELECT 'Sparse columns';
DROP TABLE IF EXISTS test_sparse;
CREATE TABLE test_sparse (id UInt8 DEFAULT 0, val UInt32 DEFAULT 0) ENGINE = MergeTree ORDER BY (id, val)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.001;

INSERT INTO test_sparse (id) SELECT 0 FROM numbers(100);
INSERT INTO test_sparse SELECT number % 3, number FROM numbers(30);

OPTIMIZE TABLE test_sparse FINAL;

SELECT id, val FROM test_sparse ORDER BY id, val LIMIT -2 BY id;

SET enable_analyzer = 1;
SELECT 'Analyzer:';

SELECT 'Negative Limit By Only';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -100 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -9223372036854775808 BY g;
SELECT number, number % 3 AS g FROM numbers(1000000) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Negative Limit By and Negative Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 OFFSET -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET -100 BY g;
SELECT number, number % 3 AS g FROM numbers(1000) ORDER BY g, number LIMIT -5 OFFSET -4 BY g;
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY g, number LIMIT -5 OFFSET -1000 BY g;

SELECT 'Negative Limit By and Positive Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -1 OFFSET 2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET 3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -100 OFFSET 4 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 5 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 OFFSET 100 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -8 OFFSET 3 BY g;

SELECT 'Common (Unordered) Path';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT -2 OFFSET 1 BY g;

SELECT 'Edge Cases';
SELECT number, number % 3 AS g FROM numbers(0) ORDER BY g, number LIMIT -2 BY g;
SELECT number, number AS g FROM numbers(5) ORDER BY g LIMIT -1 BY g;
SELECT number, 0 AS g FROM numbers(10) ORDER BY number LIMIT -3 BY g;
SELECT number, if(number % 3 = 0, NULL, number % 3) AS g FROM numbers(15) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Combined with Regular Limit';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g LIMIT 3;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -2 BY g LIMIT -3;

SELECT 'Multiple Key Columns';
SELECT number, number % 2 AS g1, number % 3 AS g2 FROM numbers(12) ORDER BY g1, g2, number LIMIT -1 BY g1, g2;

SELECT 'String Keys';
SELECT number, toString(number % 2) AS g FROM numbers(10) ORDER BY g, number LIMIT -1 BY g;

SELECT 'Misc';
SELECT DISTINCT number % 8 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET -2 BY x;
SELECT DISTINCT number % 80 AS x FROM numbers(120) ORDER BY x LIMIT -3 OFFSET 50 BY x;
SELECT number, number % 3 AS g FROM numbers(1000000) ORDER BY g, number LIMIT -1 BY g;
SELECT DISTINCT number % 5 AS g FROM numbers(1000000) ORDER BY g LIMIT -1 OFFSET -4 BY g;

SELECT 'Real Table';
DROP TABLE IF EXISTS neg_limit_by_tab;
CREATE TABLE neg_limit_by_tab (id UInt8, val UInt32) ENGINE = MergeTree ORDER BY (id, val)
AS SELECT number % 3 AS id, number AS val FROM numbers(30);

SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -2 BY id;
SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -3 OFFSET -2 BY id;
SELECT id, val FROM neg_limit_by_tab ORDER BY id, val LIMIT -1 OFFSET 8 BY id;

SELECT 'Big Table';
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY g, number LIMIT -3 BY g;
SELECT number, number % 7 AS g FROM numbers(100000) ORDER BY number LIMIT -3 BY g;

SELECT 'Positive Limit Negative Offset';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 2 OFFSET -1 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT 1 OFFSET -3 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY number LIMIT 2 OFFSET -1 BY g;

SELECT 'Zero Limit and Offset Edge Cases';
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 OFFSET -2 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -3 OFFSET 0 BY g;
SELECT number, number % 3 AS g FROM numbers(15) ORDER BY g, number LIMIT -0 OFFSET 2 BY g;

SELECT 'Expression LIMIT BY';
SELECT number FROM numbers(15) ORDER BY number % 3, number LIMIT -2 BY number % 3;
SELECT number FROM numbers(15) ORDER BY number % 3, number LIMIT -2 OFFSET -1 BY number % 3;

SELECT 'Sparse columns';
DROP TABLE IF EXISTS test_sparse;
CREATE TABLE test_sparse (id UInt8 DEFAULT 0, val UInt32 DEFAULT 0) ENGINE = MergeTree ORDER BY (id, val)
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.001;

INSERT INTO test_sparse (id) SELECT 0 FROM numbers(100);
INSERT INTO test_sparse SELECT number % 3, number FROM numbers(30);

OPTIMIZE TABLE test_sparse FINAL;

SELECT id, val FROM test_sparse ORDER BY id, val LIMIT -2 BY id;
