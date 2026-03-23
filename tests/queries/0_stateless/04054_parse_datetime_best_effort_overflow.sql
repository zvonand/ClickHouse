-- Test that parseDateTimeBestEffort does not overflow on very long fractional parts.
-- The fractional digit count must be clamped to avoid signed integer overflow in readDecimalNumber.
-- https://github.com/ClickHouse/ClickHouse/pull/100368

-- 10-digit unix timestamp with 19-digit fractional part (exceeds Int64::digits10 = 18)
SELECT parseDateTime64BestEffort('1234567890.1234567890123456789');

-- 9-digit unix timestamp with 19-digit fractional part
SELECT parseDateTime64BestEffort('123456789.1234567890123456789');

-- Exactly 18 digits (at the limit, should also work)
SELECT parseDateTime64BestEffort('1234567890.123456789012345678');
SELECT parseDateTime64BestEffort('123456789.123456789012345678');
