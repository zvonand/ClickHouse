-- Tags: no-fasttest
--
-- Per-branch parity tests for `nativeCastWithDecimalScale`, used by JIT-compiled
-- `if`/`multiIf` when at least one branch has a `Decimal` type.
--
-- Each block prints the same expression twice: once with `compile_expressions = 1`
-- (the JIT path that calls `nativeCastWithDecimalScale`) and once with
-- `compile_expressions = 0` (the interpreter path). Both must produce identical
-- output. The "jit" / "no_jit" label on every row makes a divergence trivially
-- visible in the diff against the reference file.
--
-- Background: regression first reported in
-- https://github.com/ClickHouse/ClickHouse/issues/103808 and fixed in PR #103809
-- ("Fix wrong values from JIT-compiled if/multiIf with Decimal result").
--
-- Branches NOT exercised here because the analyzer promotes the result type to
-- `Variant` and `canBeNativeType` excludes `Variant`, so
-- `FunctionIfBase::isCompilableImpl` returns `false` and the helper is never
-- called from JIT for these:
--   * `Float32`/`Float64` <-> `Decimal`  (`if(Float, Decimal)` -> `Variant(Decimal, Float)`)
--   * `Date`/`Date32`     <-> `Decimal`  (`if(Date, Decimal)`  -> `Variant(Date, Decimal)`)
-- `nativeCastWithDecimalScale` still has correct code paths for these (the
-- `pow10_fp_const` helper avoids 64-bit narrowing via `APFloat::convertFromAPInt`
-- so high-bit-width `Decimal128`/`Decimal256` factors stay accurate) so the
-- helper is forward-compatible if the analyzer ever stops promoting to `Variant`.
-- The high-bit-width side of `pow10_int_const` (`Decimal128` `to_scale >= 20`,
-- `Decimal256` `to_scale >= 39`) is exercised through the reachable integer
-- multiplier path below.

SET min_count_to_compile_expression = 0;

-- ============================================================================
-- Branch: Integer -> Decimal, to_scale = 0  (just widen)
-- ============================================================================
SELECT 'int8_to_dec9_0:jit',     toString(if(toBool(1), 1::Decimal(9, 0), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_0:no_jit',  toString(if(toBool(1), 1::Decimal(9, 0), toInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'uint8_to_dec18_0:jit',     toString(if(toBool(0), 1::Decimal(18, 0), toUInt8(255))) SETTINGS compile_expressions = 1;
SELECT 'uint8_to_dec18_0:no_jit',  toString(if(toBool(0), 1::Decimal(18, 0), toUInt8(255))) SETTINGS compile_expressions = 0;

SELECT 'int64_to_dec38_0:jit',     toString(if(toBool(0), 1::Decimal(38, 0), toInt64(-9223372036854775807))) SETTINGS compile_expressions = 1;
SELECT 'int64_to_dec38_0:no_jit',  toString(if(toBool(0), 1::Decimal(38, 0), toInt64(-9223372036854775807))) SETTINGS compile_expressions = 0;

SELECT 'uint64_to_dec38_0:jit',     toString(if(toBool(0), 1::Decimal(38, 0), toUInt64(18446744073709551615))) SETTINGS compile_expressions = 1;
SELECT 'uint64_to_dec38_0:no_jit',  toString(if(toBool(0), 1::Decimal(38, 0), toUInt64(18446744073709551615))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal, to_scale > 0  (widen + multiply by 10^to_scale)
-- ============================================================================
-- Small precisions (Decimal32/Decimal64).
SELECT 'int8_to_dec9_4_pos:jit',     toString(if(toBool(0), 1::Decimal(9, 4), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_4_pos:no_jit',  toString(if(toBool(0), 1::Decimal(9, 4), toInt8(7))) SETTINGS compile_expressions = 0;

SELECT 'int8_to_dec9_4_neg:jit',     toString(if(toBool(0), 1::Decimal(9, 4), toInt8(-7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec9_4_neg:no_jit',  toString(if(toBool(0), 1::Decimal(9, 4), toInt8(-7))) SETTINGS compile_expressions = 0;

SELECT 'int16_to_dec18_4:jit',     toString(if(toBool(0), 1::Decimal(18, 4), toInt16(-32000))) SETTINGS compile_expressions = 1;
SELECT 'int16_to_dec18_4:no_jit',  toString(if(toBool(0), 1::Decimal(18, 4), toInt16(-32000))) SETTINGS compile_expressions = 0;

SELECT 'uint16_to_dec18_4:jit',     toString(if(toBool(0), 1::Decimal(18, 4), toUInt16(65535))) SETTINGS compile_expressions = 1;
SELECT 'uint16_to_dec18_4:no_jit',  toString(if(toBool(0), 1::Decimal(18, 4), toUInt16(65535))) SETTINGS compile_expressions = 0;

SELECT 'int32_to_dec18_7:jit',     toString(if(toBool(0), 1::Decimal(18, 7), toInt32(-1234567890))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec18_7:no_jit',  toString(if(toBool(0), 1::Decimal(18, 7), toInt32(-1234567890))) SETTINGS compile_expressions = 0;

SELECT 'uint32_to_dec18_7:jit',     toString(if(toBool(0), 1::Decimal(18, 7), toUInt32(4294967295))) SETTINGS compile_expressions = 1;
SELECT 'uint32_to_dec18_7:no_jit',  toString(if(toBool(0), 1::Decimal(18, 7), toUInt32(4294967295))) SETTINGS compile_expressions = 0;

-- Decimal128 (storage 128 bit). `10^to_scale` still fits in 64 bits (< 19).
SELECT 'int64_to_dec38_11:jit',     toString(if(toBool(0), 1::Decimal(38, 11), toInt64(123456789))) SETTINGS compile_expressions = 1;
SELECT 'int64_to_dec38_11:no_jit',  toString(if(toBool(0), 1::Decimal(38, 11), toInt64(123456789))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal128, to_scale > 0 with `10^to_scale` exceeding 64 bits
-- (`pow10_int_const` produces a 128-bit `APInt` factor)
-- ============================================================================
SELECT 'int32_to_dec38_20:jit',     toString(if(toBool(0), 1::Decimal(38, 20), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec38_20:no_jit',  toString(if(toBool(0), 1::Decimal(38, 20), toInt32(7))) SETTINGS compile_expressions = 0;

SELECT 'int32_to_dec38_30:jit',     toString(if(toBool(0), 1::Decimal(38, 30), toInt32(-3))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec38_30:no_jit',  toString(if(toBool(0), 1::Decimal(38, 30), toInt32(-3))) SETTINGS compile_expressions = 0;

SELECT 'int8_to_dec38_37:jit',     toString(if(toBool(0), 1::Decimal(38, 37), toInt8(7))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec38_37:no_jit',  toString(if(toBool(0), 1::Decimal(38, 37), toInt8(7))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Integer -> Decimal256, to_scale > 0 with `10^to_scale` exceeding 128 bits
-- (`pow10_int_const` produces a 256-bit `APInt` factor)
-- ============================================================================
SELECT 'int32_to_dec76_50:jit',     toString(if(toBool(0), 1::Decimal(76, 50), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'int32_to_dec76_50:no_jit',  toString(if(toBool(0), 1::Decimal(76, 50), toInt32(7))) SETTINGS compile_expressions = 0;

SELECT 'int8_to_dec76_75:jit',     toString(if(toBool(0), 1::Decimal(76, 75), toInt8(-3))) SETTINGS compile_expressions = 1;
SELECT 'int8_to_dec76_75:no_jit',  toString(if(toBool(0), 1::Decimal(76, 75), toInt8(-3))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Decimal -> Decimal, same scale (only widen to result precision)
-- ============================================================================
SELECT 'dec9_to_dec18_same_scale:jit',     toString(if(toBool(1), 1.5::Decimal(18, 4), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec9_to_dec18_same_scale:no_jit',  toString(if(toBool(1), 1.5::Decimal(18, 4), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 0;

SELECT 'dec18_to_dec38_same_scale:jit',     toString(if(toBool(0), 1.5::Decimal(38, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec18_to_dec38_same_scale:no_jit',  toString(if(toBool(0), 1.5::Decimal(38, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

SELECT 'dec38_to_dec76_same_scale:jit',     toString(if(toBool(1), 1.5::Decimal(76, 4), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec38_to_dec76_same_scale:no_jit',  toString(if(toBool(1), 1.5::Decimal(76, 4), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Decimal -> Decimal, scale increase (widen + multiply by 10^diff)
-- ============================================================================
SELECT 'dec9_4_to_dec18_7:jit',     toString(if(toBool(1), 1.5::Decimal(18, 7), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec9_4_to_dec18_7:no_jit',  toString(if(toBool(1), 1.5::Decimal(18, 7), 2.5::Decimal(9, 4))) SETTINGS compile_expressions = 0;

SELECT 'dec9_4_to_dec18_7_neg:jit',     toString(if(toBool(0), 1.5::Decimal(18, 7), toDecimal32(-2.5, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec9_4_to_dec18_7_neg:no_jit',  toString(if(toBool(0), 1.5::Decimal(18, 7), toDecimal32(-2.5, 4))) SETTINGS compile_expressions = 0;

-- Crossing the 64-bit threshold for the multiplier: `dec18_4 -> dec38_30` needs
-- `10^26` factor which exceeds 64 bits.
SELECT 'dec18_4_to_dec38_30:jit',     toString(if(toBool(0), 1.5::Decimal(38, 30), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec18_4_to_dec38_30:no_jit',  toString(if(toBool(0), 1.5::Decimal(38, 30), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

-- 256-bit storage with scale-up factor > 128 bits.
SELECT 'dec38_4_to_dec76_50:jit',     toString(if(toBool(0), 1.5::Decimal(76, 50), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 1;
SELECT 'dec38_4_to_dec76_50:no_jit',  toString(if(toBool(0), 1.5::Decimal(76, 50), 2.5::Decimal(38, 4))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Decimal -> Decimal, scale decrease (widen + signed divide by 10^diff)
-- The least-supertype machinery typically picks the LARGER scale, so we exercise
-- the divide path through an explicit `cast` that narrows the scale.
-- ============================================================================
SELECT 'cast_dec18_8_to_dec18_2:jit',     toString(if(toBool(1), cast(1.23456789::Decimal(18, 8) as Decimal(18, 2)), 2.5::Decimal(18, 2))) SETTINGS compile_expressions = 1;
SELECT 'cast_dec18_8_to_dec18_2:no_jit',  toString(if(toBool(1), cast(1.23456789::Decimal(18, 8) as Decimal(18, 2)), 2.5::Decimal(18, 2))) SETTINGS compile_expressions = 0;

SELECT 'cast_dec38_30_to_dec38_4_neg:jit',     toString(if(toBool(1), cast(toDecimal128(-1.23456789012345, 30) as Decimal(38, 4)), 0::Decimal(38, 4))) SETTINGS compile_expressions = 1;
SELECT 'cast_dec38_30_to_dec38_4_neg:no_jit',  toString(if(toBool(1), cast(toDecimal128(-1.23456789012345, 30) as Decimal(38, 4)), 0::Decimal(38, 4))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: Nullable wrappers around the above (the Nullable->Nullable, Nullable->T,
-- T->Nullable arms of `nativeCastWithDecimalScale` recurse into the inner branches).
-- ============================================================================
SELECT 'nullable_dec_int_jit',     toString(if(toBool(1), toNullable(1.5::Decimal(38, 30)), toInt32(7))) SETTINGS compile_expressions = 1;
SELECT 'nullable_dec_int_no_jit',  toString(if(toBool(1), toNullable(1.5::Decimal(38, 30)), toInt32(7))) SETTINGS compile_expressions = 0;

SELECT 'nullable_dec_dec_jit',     toString(if(toBool(0), toNullable(1.5::Decimal(38, 30)), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'nullable_dec_dec_no_jit',  toString(if(toBool(0), toNullable(1.5::Decimal(38, 30)), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

SELECT 'both_nullable_jit',     toString(if(toBool(0), toNullable(1.5::Decimal(38, 30)), toNullable(2.5::Decimal(18, 4)))) SETTINGS compile_expressions = 1;
SELECT 'both_nullable_no_jit',  toString(if(toBool(0), toNullable(1.5::Decimal(38, 30)), toNullable(2.5::Decimal(18, 4)))) SETTINGS compile_expressions = 0;

SELECT 'nullable_picks_null_jit',     toString(if(toBool(0), 1.5::Decimal(38, 30), CAST(NULL, 'Nullable(Decimal(18, 4))'))) SETTINGS compile_expressions = 1;
SELECT 'nullable_picks_null_no_jit',  toString(if(toBool(0), 1.5::Decimal(38, 30), CAST(NULL, 'Nullable(Decimal(18, 4))'))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: `multiIf` with mixed integer literals + Decimal else
-- (the original failure mode from #103808)
-- ============================================================================
SELECT 'multiif_dec38_30_pick0:jit',     toString(multiIf(toBool(1), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec38_30_pick0:no_jit',  toString(multiIf(toBool(1), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 0;

SELECT 'multiif_dec38_30_pickelse:jit',     toString(multiIf(toBool(0), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec38_30_pickelse:no_jit',  toString(multiIf(toBool(0), 1, toBool(0), 2, 3.5::Decimal(38, 30))) SETTINGS compile_expressions = 0;

SELECT 'multiif_dec76_60:jit',     toString(multiIf(toBool(1), 1, toBool(0), 2, 3.5::Decimal(76, 60))) SETTINGS compile_expressions = 1;
SELECT 'multiif_dec76_60:no_jit',  toString(multiIf(toBool(1), 1, toBool(0), 2, 3.5::Decimal(76, 60))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Branch: identity (from_type == to_type), short-circuit return of original value
-- ============================================================================
SELECT 'identity_dec18_4:jit',     toString(if(toBool(1), 1.5::Decimal(18, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 1;
SELECT 'identity_dec18_4:no_jit',  toString(if(toBool(1), 1.5::Decimal(18, 4), 2.5::Decimal(18, 4))) SETTINGS compile_expressions = 0;

SELECT 'identity_dec76_60:jit',     toString(if(toBool(0), 1.5::Decimal(76, 60), 2.5::Decimal(76, 60))) SETTINGS compile_expressions = 1;
SELECT 'identity_dec76_60:no_jit',  toString(if(toBool(0), 1.5::Decimal(76, 60), 2.5::Decimal(76, 60))) SETTINGS compile_expressions = 0;

-- ============================================================================
-- Materialized columns (the original failure from #103808 required materialization;
-- pure constants are folded before JIT so the helper is bypassed).
-- ============================================================================
SELECT 'materialized_dec18_7:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec18_7:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(18, 7)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

SELECT 'materialized_dec38_30:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec38_30:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(38, 30)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

SELECT 'materialized_dec76_73:jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_dec76_73:no_jit',
    toString(sum(if(k != 1, r, 1)))
FROM (SELECT materialize(1::Decimal(76, 73)) AS r, materialize(1) AS k)
SETTINGS compile_expressions = 0;

-- Materialized integer + materialized Decimal branch
-- (exercises Int -> Decimal helper with non-constant integer input).
SELECT 'materialized_int_to_dec38_30:jit',
    toString(sum(if(k != 0, r, k)))
FROM (SELECT materialize(1.5::Decimal(38, 30)) AS r, materialize(toInt64(7)) AS k)
SETTINGS compile_expressions = 1;
SELECT 'materialized_int_to_dec38_30:no_jit',
    toString(sum(if(k != 0, r, k)))
FROM (SELECT materialize(1.5::Decimal(38, 30)) AS r, materialize(toInt64(7)) AS k)
SETTINGS compile_expressions = 0;

-- ============================================================================
-- multiIf over a multi-row table with mixed branch types.
-- Each row picks a different branch, exercising the JIT path through
-- materialized integer keys with a high-scale Decimal else branch.
-- ============================================================================
DROP TABLE IF EXISTS jit_decimal_parity_input;
CREATE TABLE jit_decimal_parity_input (idx UInt32, k Int64) ENGINE = MergeTree ORDER BY idx;
INSERT INTO jit_decimal_parity_input VALUES (0, 0), (1, 1), (2, 2), (3, -7), (4, 100);

SELECT 'multirow_multiif:jit',
    arraySort(groupArray(toString(multiIf(k = 0, 1, k = 1, 2, k = 2, 3, 5.5::Decimal(38, 30)))))
FROM jit_decimal_parity_input
SETTINGS compile_expressions = 1;
SELECT 'multirow_multiif:no_jit',
    arraySort(groupArray(toString(multiIf(k = 0, 1, k = 1, 2, k = 2, 3, 5.5::Decimal(38, 30)))))
FROM jit_decimal_parity_input
SETTINGS compile_expressions = 0;

DROP TABLE jit_decimal_parity_input;
