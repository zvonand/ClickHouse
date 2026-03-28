-- Test that toStartOfInterval with extreme DateTime64 values throws overflow instead of UB.
-- The internal DateTime64 value is near INT64_MAX, and the scale is larger than the target
-- interval scale, so the `scale_multiplier > target_scale` code path is exercised.

-- Millisecond interval with scale=6 (microseconds): scale_diff=1000, t + 500 overflows
SELECT toStartOfInterval(CAST(9223372036854775806 AS DateTime64(6)), toIntervalMillisecond(1)); -- { serverError DECIMAL_OVERFLOW }

-- Millisecond interval with scale=9 (nanoseconds): scale_diff=1000000, t + 500000 overflows
SELECT toStartOfInterval(CAST(9223372036854775806 AS DateTime64(9)), toIntervalMillisecond(1)); -- { serverError DECIMAL_OVERFLOW }

-- Microsecond interval with scale=9 (nanoseconds): scale_diff=1000, t + 500 overflows
SELECT toStartOfInterval(CAST(9223372036854775806 AS DateTime64(9)), toIntervalMicrosecond(1)); -- { serverError DECIMAL_OVERFLOW }
