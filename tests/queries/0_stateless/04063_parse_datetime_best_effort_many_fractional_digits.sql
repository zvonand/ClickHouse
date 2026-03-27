-- Regression test: parsing datetime strings with many fractional digits must not cause
-- signed integer overflow (UB) in readDecimalNumber. Fractional digits are capped at
-- digits10 of the result type (Int64::digits10 = 18).

-- Exactly 18 fractional digits (at the cap) - all should succeed
SELECT parseDateTime64BestEffort('1596752940.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('100000000.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('2020-08-07 01:29:00.123456789012345678', 6, 'UTC');

-- More than 18 fractional digits - previously caused UB, must now return NULL gracefully
SELECT parseDateTime64BestEffortOrNull('1596752940.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('100000000.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('2020-08-07 01:29:00.12345678901234567890', 6, 'UTC');
