-- Regression test: positiveModulo with large unsigned divisors should not cause signed integer overflow (UB).
-- When res < 0 and b is a large UInt64 (> INT64_MAX), the old code did res += static_cast<Int64>(b),
-- which overflowed signed arithmetic. The fix performs the addition in unsigned arithmetic.

-- Core overflow-triggering cases: res is very negative, static_cast<Int64>(b) is also negative, sum overflows.
SELECT positiveModulo(toInt64('-9223372036854775808'), toUInt64('9223372036854775809'));
SELECT positiveModulo(toInt64('-9223372036854775807'), toUInt64('18446744073709551614'));

-- Non-overflow cases that should still work correctly
SELECT positiveModulo(toInt64(-5), toInt64(3));
SELECT positiveModulo(toInt64(-1), toInt64(1));
SELECT positiveModulo(toInt64(-1), toInt64(9223372036854775807));
