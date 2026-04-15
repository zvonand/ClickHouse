-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test for sort order violation during SummingMergeTree merge when the
-- sort key is a hash expression over a Float32 column containing signaling NaN (SNaN).
--
-- Root cause: SummingSortedAlgorithm::setRow() converted Float32 to Float64 (via Field),
-- which silently converts x86 signaling NaN to quiet NaN, changing the NaN bit pattern
-- and thus changing the gccMurmurHash value. This caused CheckSortedTransform to detect
-- an out-of-order merge output (server crash in debug builds, silent data corruption
-- in release builds).

DROP TABLE IF EXISTS t_float_nan_sort;

-- gccMurmurHash of Float32 hashes the raw 4 bytes. Different NaN bit patterns produce
-- different hashes. generateRandom can produce non-canonical (signaling) NaN values.
CREATE TABLE t_float_nan_sort (c0 Int32, c1 Float32)
ENGINE = SummingMergeTree()
ORDER BY (gccMurmurHash(c1));

INSERT INTO TABLE t_float_nan_sort (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 4471575971265722, 3161, 5) LIMIT 130;

INSERT INTO TABLE t_float_nan_sort (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 14156128262908154975, 2463, 2) LIMIT 10;

-- This OPTIMIZE triggers a merge. Before the fix, debug builds crashed here with:
-- "Logical error: Sort order of blocks violated for column number ..."
OPTIMIZE TABLE t_float_nan_sort FINAL;

SELECT count() > 0 FROM t_float_nan_sort;

-- Also test with cityHash64 (same class of bug)
DROP TABLE IF EXISTS t_float_nan_sort_city;
CREATE TABLE t_float_nan_sort_city (c0 Int32, c1 Float32)
ENGINE = SummingMergeTree()
ORDER BY (cityHash64(c1));

INSERT INTO TABLE t_float_nan_sort_city (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 4471575971265722, 3161, 5) LIMIT 142;

INSERT INTO TABLE t_float_nan_sort_city (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 14156128262908154975, 2463, 2) LIMIT 453;

OPTIMIZE TABLE t_float_nan_sort_city FINAL;

SELECT count() > 0 FROM t_float_nan_sort_city;

DROP TABLE t_float_nan_sort;
DROP TABLE t_float_nan_sort_city;
