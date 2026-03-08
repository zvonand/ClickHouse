-- Tags: no-random-merge-tree-settings
-- Test that _block_number/_block_offset projection is correctly rebuilt after mutations.

DROP TABLE IF EXISTS mt_mutation_test SYNC;

CREATE TABLE mt_mutation_test(
    a UInt64,
    b String,
    PROJECTION _commit_order (
        SELECT *, _block_number, _block_offset
        ORDER BY _block_number, _block_offset
    )
)
ENGINE = MergeTree
ORDER BY a
SETTINGS enable_block_number_column=1, enable_block_offset_column=1;

-- 4 parts, 3 rows each, values of `a` in random order across inserts
INSERT INTO mt_mutation_test VALUES (7, 'a'), (2, 'b'), (11, 'c');
INSERT INTO mt_mutation_test VALUES (5, 'd'), (14, 'e'), (1, 'f');
INSERT INTO mt_mutation_test VALUES (9, 'g'), (4, 'h'), (13, 'i');
INSERT INTO mt_mutation_test VALUES (15, 'j'), (8, 'k'), (3, 'l');
OPTIMIZE TABLE mt_mutation_test FINAL;

SELECT 'after merge';
SELECT lhs.a, lhs.b, lhs._block_number, lhs._block_offset, rhs._parent_block_number, rhs._parent_block_offset, (lhs._block_number, lhs._block_offset) = (rhs._parent_block_number, rhs._parent_block_offset)
FROM mt_mutation_test AS lhs
JOIN mergeTreeProjection(currentDatabase(), 'mt_mutation_test', '_commit_order') AS rhs USING (a)
ORDER BY lhs.a;

-- UPDATE mutation: changes column values but preserves _block_number/_block_offset
ALTER TABLE mt_mutation_test UPDATE b = ' ' WHERE a % 3 = 0 SETTINGS mutations_sync = 2;

SELECT 'after UPDATE';
SELECT lhs.a, lhs.b, lhs._block_number, lhs._block_offset, rhs._parent_block_number, rhs._parent_block_offset, (lhs._block_number, lhs._block_offset) = (rhs._parent_block_number, rhs._parent_block_offset)
FROM mt_mutation_test AS lhs
JOIN mergeTreeProjection(currentDatabase(), 'mt_mutation_test', '_commit_order') AS rhs USING (a)
ORDER BY lhs.a;

-- DELETE mutation: removes rows, surviving rows keep their _block_number/_block_offset
ALTER TABLE mt_mutation_test DELETE WHERE a > 12 SETTINGS mutations_sync = 2;

SELECT 'after DELETE';
SELECT lhs.a, lhs.b, lhs._block_number, lhs._block_offset, rhs._parent_block_number, rhs._parent_block_offset, (lhs._block_number, lhs._block_offset) = (rhs._parent_block_number, rhs._parent_block_offset)
FROM mt_mutation_test AS lhs
JOIN mergeTreeProjection(currentDatabase(), 'mt_mutation_test', '_commit_order') AS rhs USING (a)
ORDER BY lhs.a;

DROP TABLE mt_mutation_test SYNC;
