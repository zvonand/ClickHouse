-- Test for share_nested_offsets MergeTree setting.
-- Covers: wide parts, compact parts, merges, missing columns, immutability.

SET flatten_nested = 0;

-- =============================================================================
-- 1. Default behavior (share_nested_offsets=true): Nested semantics enforced
-- =============================================================================

SELECT '--- shared offsets (default) ---';

DROP TABLE IF EXISTS t_shared;
CREATE TABLE t_shared
(
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS share_nested_offsets = true;

-- Matching sizes — should succeed.
INSERT INTO t_shared VALUES ([1, 2], ['x', 'y']);
SELECT * FROM t_shared;

-- Mismatched sizes — should fail.
INSERT INTO t_shared VALUES ([1, 2, 3], ['x', 'y']); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_shared;

-- =============================================================================
-- 2. Wide parts with share_nested_offsets=false
-- =============================================================================

SELECT '--- independent wide parts ---';

DROP TABLE IF EXISTS t_wide;
CREATE TABLE t_wide
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0;

-- Different array sizes — should succeed.
INSERT INTO t_wide VALUES (1, [1, 2, 3], ['x', 'y']);
INSERT INTO t_wide VALUES (2, [10], ['a', 'b', 'c', 'd']);
INSERT INTO t_wide VALUES (3, [], ['only_b']);

SELECT id, n.a, n.b FROM t_wide ORDER BY id;

-- Merge wide parts and verify data survives.
OPTIMIZE TABLE t_wide FINAL;
SELECT id, n.a, n.b FROM t_wide ORDER BY id;

DROP TABLE t_wide;

-- =============================================================================
-- 3. Compact parts with share_nested_offsets=false
-- =============================================================================

SELECT '--- independent compact parts ---';

DROP TABLE IF EXISTS t_compact;
CREATE TABLE t_compact
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 1000000000;

-- Different array sizes in compact parts.
INSERT INTO t_compact VALUES (1, [1, 2, 3], ['x', 'y']);
INSERT INTO t_compact VALUES (2, [10], ['a', 'b', 'c', 'd']);
INSERT INTO t_compact VALUES (3, [], ['only_b']);

SELECT id, n.a, n.b FROM t_compact ORDER BY id;

-- Merge compact parts and verify data survives.
OPTIMIZE TABLE t_compact FINAL;
SELECT id, n.a, n.b FROM t_compact ORDER BY id;

DROP TABLE t_compact;

-- =============================================================================
-- 4. ALTER ADD COLUMN — missing column with dotted name
-- =============================================================================

SELECT '--- alter add column ---';

DROP TABLE IF EXISTS t_alter;
CREATE TABLE t_alter
(
    id UInt64,
    `n.a` Array(UInt32)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0;

INSERT INTO t_alter VALUES (1, [1, 2, 3]);

-- Add a sibling column — old parts don't have it, should read as default.
ALTER TABLE t_alter ADD COLUMN `n.b` Array(String) DEFAULT [];

SELECT id, n.a, n.b FROM t_alter ORDER BY id;

-- Insert with both columns, different sizes.
INSERT INTO t_alter VALUES (2, [10], ['x', 'y', 'z']);
SELECT id, n.a, n.b FROM t_alter ORDER BY id;

DROP TABLE t_alter;

-- =============================================================================
-- 5. Multiple Nested-like groups are independent
-- =============================================================================

SELECT '--- multiple groups ---';

DROP TABLE IF EXISTS t_multi;
CREATE TABLE t_multi
(
    id UInt64,
    `a.x` Array(UInt32),
    `a.y` Array(String),
    `b.x` Array(UInt32),
    `b.y` Array(UInt8)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false;

INSERT INTO t_multi VALUES (1, [1], ['hello', 'world'], [100, 200, 300], [0]);
SELECT id, a.x, a.y, b.x, b.y FROM t_multi ORDER BY id;

DROP TABLE t_multi;

-- =============================================================================
-- 6. Immutability: setting cannot be changed after creation
-- =============================================================================

SELECT '--- immutability ---';

DROP TABLE IF EXISTS t_immutable;
CREATE TABLE t_immutable (`n.a` Array(UInt32)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS share_nested_offsets = false;

ALTER TABLE t_immutable MODIFY SETTING share_nested_offsets = true; -- { serverError READONLY_SETTING }

DROP TABLE t_immutable;

-- =============================================================================
-- 7. ALTER TABLE UPDATE with different array sizes on sibling columns
-- =============================================================================

SELECT '--- update with different sizes ---';

DROP TABLE IF EXISTS t_update;
CREATE TABLE t_update
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0;

INSERT INTO t_update VALUES (1, [1, 2], ['x', 'y', 'z']);
INSERT INTO t_update VALUES (2, [10, 20, 30], ['a']);

-- Update n.a to a different size than n.b — should succeed with share_nested_offsets=false.
SET mutations_sync = 1;
ALTER TABLE t_update UPDATE `n.a` = [100] WHERE id = 1;
SET mutations_sync = 0;

SELECT id, n.a, n.b FROM t_update ORDER BY id;

DROP TABLE t_update;

-- =============================================================================
-- 8. ALTER TABLE RENAME COLUMN for dotted-name columns
-- =============================================================================

SELECT '--- rename dotted columns ---';

DROP TABLE IF EXISTS t_rename;
CREATE TABLE t_rename
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0;

INSERT INTO t_rename VALUES (1, [1, 2, 3], ['x', 'y']);

-- Rename from one nested group to another — should succeed with share_nested_offsets=false.
ALTER TABLE t_rename RENAME COLUMN `n.a` TO `m.a`;
SELECT id, m.a, n.b FROM t_rename ORDER BY id;

-- Rename from nested to non-nested — should also succeed.
ALTER TABLE t_rename RENAME COLUMN `n.b` TO `flat_b`;
SELECT id, m.a, flat_b FROM t_rename ORDER BY id;

DROP TABLE t_rename;

-- =============================================================================
-- 9. Missing defaults during INSERT when sibling column exists
-- =============================================================================

SELECT '--- missing defaults ---';

DROP TABLE IF EXISTS t_defaults;
CREATE TABLE t_defaults
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0;

-- Insert only n.a, leaving n.b missing — n.b should get empty arrays, not replicated offsets.
INSERT INTO t_defaults (id, `n.a`) VALUES (1, [1, 2, 3]);
SELECT id, n.a, n.b FROM t_defaults ORDER BY id;

-- Insert only n.b, leaving n.a missing.
INSERT INTO t_defaults (id, `n.b`) VALUES (2, ['x', 'y']);
SELECT id, n.a, n.b FROM t_defaults ORDER BY id;

DROP TABLE t_defaults;

-- =============================================================================
-- 10. Vertical merge of 2 array columns
-- =============================================================================

SELECT '--- vertical merge ---';

DROP TABLE IF EXISTS t_vertical;
CREATE TABLE t_vertical
(
    id UInt64,
    `n.a` Array(UInt32),
    `n.b` Array(String)
)
ENGINE = MergeTree ORDER BY id
SETTINGS share_nested_offsets = false, min_bytes_for_wide_part = 0,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vertical VALUES (1, [1, 2, 3], ['x']);
INSERT INTO t_vertical VALUES (2, [10], ['a', 'b', 'c', 'd']);
INSERT INTO t_vertical VALUES (3, [], ['only_b']);

-- Force vertical merge.
OPTIMIZE TABLE t_vertical FINAL;

SELECT id, n.a, n.b FROM t_vertical ORDER BY id;

DROP TABLE t_vertical;

SELECT '--- done ---';
