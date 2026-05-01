-- Regression for https://github.com/ClickHouse/ClickHouse/issues/103660
-- `KeyCondition::extractPlainRanges` must not treat a relaxed (deduplicated/transformed) set
-- as exact. For `tuple(i, i) NOT IN (tuple(1, 2))` `MergeTreeSetIndex` deduplicates the tuple
-- columns to a single key column, producing the relaxed set `{1}`. Building the complement
-- `(-inf, 1) U (1, +inf)` and treating it as exact ranges incorrectly skips `i = 1`,
-- even though `tuple(1, 1) != tuple(1, 2)`.

-- `numbers()` consumes the extracted plain ranges directly, so the bug surfaces here.
SELECT 'numbers NOT IN tuple';
SELECT number FROM numbers(5) WHERE tuple(number, number) NOT IN (tuple(1, 2)) ORDER BY number;

-- The `NOT has` variant goes through the same code path via `tryPrepareSetIndexForHas`.
SELECT 'numbers NOT has tuple';
SELECT number FROM numbers(5) WHERE NOT has([tuple(1, 2)], (number, number)) ORDER BY number;

-- Per-row evaluation sanity check: every `tuple(n, n)` differs from `tuple(1, 2)`,
-- so `NOT IN` is `1` for every row.
SELECT 'per-row eval';
SELECT number, tuple(number, number) NOT IN (tuple(1, 2)) AS val FROM numbers(5) ORDER BY number;

-- The mirror `IN` predicate must remain consistent: no row has `tuple(n, n) = tuple(1, 2)`.
SELECT 'numbers IN tuple';
SELECT number FROM numbers(5) WHERE tuple(number, number) IN (tuple(1, 2)) ORDER BY number;

-- `generate_series` shares the same range-honoring source. Its column is named `generate_series`.
SELECT 'generate_series NOT IN tuple';
SELECT generate_series FROM generate_series(0, 4) WHERE tuple(generate_series, generate_series) NOT IN (tuple(1, 2)) ORDER BY generate_series;

-- Sanity: a real `MergeTree` table with a relaxed predicate must also return all rows.
DROP TABLE IF EXISTS t_extract_plain_ranges_relaxed;
CREATE TABLE t_extract_plain_ranges_relaxed (i UInt64) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_extract_plain_ranges_relaxed SELECT number FROM numbers(5);

SELECT 'mergetree NOT IN tuple';
SELECT i FROM t_extract_plain_ranges_relaxed WHERE tuple(i, i) NOT IN (tuple(1, 2)) ORDER BY i;

DROP TABLE t_extract_plain_ranges_relaxed;
