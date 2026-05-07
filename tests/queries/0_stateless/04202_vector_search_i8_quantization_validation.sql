-- Tags: no-fasttest, no-ordinary-database

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab_i8;
DROP TABLE IF EXISTS tab_f32;

-- ----------------------------------------------------------------------------
-- `i8` quantization: rejects non-finite elements and zero-magnitude vectors
-- ----------------------------------------------------------------------------

CREATE TABLE tab_i8 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3, 'i8', 32, 128))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_i8 VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]);

SELECT '-- i8 SELECT with NaN reference vector';
SELECT id FROM tab_i8 ORDER BY L2Distance(vec, [nan, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

SELECT '-- i8 SELECT with +Inf reference vector';
SELECT id FROM tab_i8 ORDER BY L2Distance(vec, [inf, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

SELECT '-- i8 SELECT with -Inf reference vector';
SELECT id FROM tab_i8 ORDER BY L2Distance(vec, [-inf, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

SELECT '-- i8 SELECT with zero-magnitude reference vector';
SELECT id FROM tab_i8 ORDER BY L2Distance(vec, [0.0, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

SELECT '-- i8 SELECT with valid reference vector still works';
SELECT id FROM tab_i8 ORDER BY L2Distance(vec, [1.0, 0.0, 0.0]) LIMIT 1;

SELECT '-- i8 INSERT with NaN element';
INSERT INTO tab_i8 VALUES (10, [nan, 0.0, 0.0]); -- { serverError INCORRECT_DATA }

SELECT '-- i8 INSERT with +Inf element';
INSERT INTO tab_i8 VALUES (11, [inf, 0.0, 0.0]); -- { serverError INCORRECT_DATA }

SELECT '-- i8 INSERT with zero-magnitude vector';
INSERT INTO tab_i8 VALUES (12, [0.0, 0.0, 0.0]); -- { serverError INCORRECT_DATA }

DROP TABLE tab_i8;

-- ----------------------------------------------------------------------------
-- Default (`bf16`) quantization: still rejects NaN/Inf, but accepts zero-magnitude
-- ----------------------------------------------------------------------------

CREATE TABLE tab_f32 (id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 3))
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab_f32 VALUES (0, [1.0, 0.0, 0.0]), (1, [0.0, 1.0, 0.0]), (2, [0.0, 0.0, 1.0]);

SELECT '-- bf16 SELECT with NaN reference vector still rejected';
SELECT id FROM tab_f32 ORDER BY L2Distance(vec, [nan, 0.0, 0.0]) LIMIT 1; -- { serverError INCORRECT_QUERY }

SELECT '-- bf16 SELECT with zero-magnitude reference vector is allowed';
-- All three indexed vectors are equidistant from the origin, so we just check that the query
-- succeeds and returns one row.
SELECT count() FROM (SELECT id FROM tab_f32 ORDER BY L2Distance(vec, [0.0, 0.0, 0.0]) LIMIT 1);

SELECT '-- bf16 INSERT with NaN element still rejected';
INSERT INTO tab_f32 VALUES (10, [nan, 0.0, 0.0]); -- { serverError INCORRECT_DATA }

SELECT '-- bf16 INSERT with zero-magnitude vector is allowed';
INSERT INTO tab_f32 VALUES (11, [0.0, 0.0, 0.0]);

DROP TABLE tab_f32;
