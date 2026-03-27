-- Regression test: parsing datetime strings with many fractional digits must not cause
-- signed integer overflow (UB) in readDecimalNumber. Fractional digits are capped at
-- digits10 of the result type (Int64::digits10 = 18).

-- Exactly 18 fractional digits (at the cap) - all should succeed
SELECT parseDateTime64BestEffort('1596752940.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('100000000.123456789012345678', 6, 'UTC');
SELECT parseDateTime64BestEffort('2020-08-07 01:29:00.123456789012345678', 6, 'UTC');

-- 19 fractional digits: readDigits reads all 19 into its buffer (UInt64::digits10 = 19),
-- then the value is capped to 18; the 19th digit is silently dropped - still parses fine
SELECT parseDateTime64BestEffort('1596752940.1234567890123456789', 6, 'UTC');
SELECT parseDateTime64BestEffort('100000000.1234567890123456789', 6, 'UTC');
SELECT parseDateTime64BestEffort('2020-08-07 01:29:00.1234567890123456789', 6, 'UTC');

-- 20+ fractional digits: the 20th digit is left in the stream after readDigits exhausts
-- its buffer, causing a parse error - previously also caused UB before the cap was added
SELECT parseDateTime64BestEffortOrNull('1596752940.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('100000000.12345678901234567890', 6, 'UTC');
SELECT parseDateTime64BestEffortOrNull('2020-08-07 01:29:00.12345678901234567890', 6, 'UTC');
