-- Reproducer for a bug where SAMPLE on a Buffer table wrapping a MergeTree
-- without a sampling key caused a std::out_of_range exception (logical error).
-- The Buffer engine unconditionally claimed to support sampling.

DROP TABLE IF EXISTS buffer_for_sample_test;
DROP TABLE IF EXISTS merge_tree_for_sample_test;

CREATE TABLE merge_tree_for_sample_test (s UInt128, x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE buffer_for_sample_test (s UInt128, x UInt32) ENGINE = Buffer(currentDatabase(), 'merge_tree_for_sample_test', 16, 10, 60, 10, 1000, 1048576, 2097152);

INSERT INTO merge_tree_for_sample_test VALUES (1, 1), (2, 2), (3, 3);

SELECT * FROM buffer_for_sample_test SAMPLE 2 / 10; -- { serverError SAMPLING_NOT_SUPPORTED }
SELECT * FROM buffer_for_sample_test FINAL SAMPLE 2 / 10; -- { serverError SAMPLING_NOT_SUPPORTED }

DROP TABLE buffer_for_sample_test;
DROP TABLE merge_tree_for_sample_test;
