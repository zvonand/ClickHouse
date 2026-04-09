-- Tags: no-fasttest, no-ordinary-database

-- Tests dot product (inner product) distance function with vector similarity indexes.

SET enable_analyzer = 1;

SELECT 'Basic dot product query';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 3)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]), (3, [1.0, 1.0, 0.0]), (4, [1.0, 1.0, 1.0]), (5, [0.5, 0.5, 0.5]), (6, [0.1, 0.2, 0.3]), (7, [0.9, 0.1, 0.0]), (8, [0.0, 0.9, 0.1]), (9, [0.3, 0.3, 0.3]);

WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, dotProduct(vec, reference_vec)
FROM tab
ORDER BY dotProduct(vec, reference_vec) DESC
LIMIT 3;

SELECT 'EXPLAIN: dot product uses vector similarity index';

EXPLAIN indexes = 1
WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, dotProduct(vec, reference_vec)
FROM tab
ORDER BY dotProduct(vec, reference_vec) DESC
LIMIT 3;

DROP TABLE tab;

SELECT 'Dot product with multiple granules';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 3) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]), (3, [1.0, 1.0, 0.0]), (4, [1.0, 1.0, 1.0]), (5, [0.5, 0.5, 0.5]), (6, [0.1, 0.2, 0.3]), (7, [0.9, 0.1, 0.0]), (8, [0.0, 0.9, 0.1]), (9, [0.3, 0.3, 0.3]), (10, [0.8, 0.2, 0.0]), (11, [0.0, 0.0, 0.9]);

WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, dotProduct(vec, reference_vec)
FROM tab
ORDER BY dotProduct(vec, reference_vec) DESC
LIMIT 3;

DROP TABLE tab;

SELECT 'Wrong sort direction: ASC with dotProduct should not use index';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 3) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]), (3, [1.0, 1.0, 0.0]), (4, [1.0, 1.0, 1.0]), (5, [0.5, 0.5, 0.5]), (6, [0.1, 0.2, 0.3]), (7, [0.9, 0.1, 0.0]), (8, [0.0, 0.9, 0.1]), (9, [0.3, 0.3, 0.3]), (10, [0.8, 0.2, 0.0]), (11, [0.0, 0.0, 0.9]);

EXPLAIN indexes = 1
WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, dotProduct(vec, reference_vec)
FROM tab
ORDER BY dotProduct(vec, reference_vec) ASC
LIMIT 3;

DROP TABLE tab;

SELECT 'Wrong sort direction: DESC with L2Distance should not use index';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]), (3, [1.0, 1.0, 0.0]), (4, [1.0, 1.0, 1.0]), (5, [0.5, 0.5, 0.5]), (6, [0.1, 0.2, 0.3]), (7, [0.9, 0.1, 0.0]), (8, [0.0, 0.9, 0.1]), (9, [0.3, 0.3, 0.3]), (10, [0.8, 0.2, 0.0]), (11, [0.0, 0.0, 0.9]);

EXPLAIN indexes = 1
WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, L2Distance(vec, reference_vec)
FROM tab
ORDER BY L2Distance(vec, reference_vec) DESC
LIMIT 3;

DROP TABLE tab;

SELECT 'Wrong sort direction: DESC with cosineDistance should not use index';

CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'cosineDistance', 3) GRANULARITY 2) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3;
INSERT INTO tab VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]), (3, [1.0, 1.0, 0.0]), (4, [1.0, 1.0, 1.0]), (5, [0.5, 0.5, 0.5]), (6, [0.1, 0.2, 0.3]), (7, [0.9, 0.1, 0.0]), (8, [0.0, 0.9, 0.1]), (9, [0.3, 0.3, 0.3]), (10, [0.8, 0.2, 0.0]), (11, [0.0, 0.0, 0.9]);

EXPLAIN indexes = 1
WITH [1.0, 0.0, 0.0] AS reference_vec
SELECT id, vec, cosineDistance(vec, reference_vec)
FROM tab
ORDER BY cosineDistance(vec, reference_vec) DESC
LIMIT 3;

DROP TABLE tab;
