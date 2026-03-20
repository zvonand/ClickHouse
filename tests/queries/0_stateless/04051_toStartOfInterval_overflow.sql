-- Regression test: toStartOfInterval with large interval values should throw instead of UB (signed integer overflow)
SELECT toStartOfInterval(toDateTime64('2023-10-09 00:01:34', 9), toIntervalMillisecond(9223372036854775806)); -- { serverError DECIMAL_OVERFLOW }
SELECT toStartOfInterval(toDateTime64('2023-10-09 00:01:34', 9), toIntervalMicrosecond(9223372036854775806)); -- { serverError DECIMAL_OVERFLOW }
