-- Compound interval literals: INTERVAL 'string' KIND TO KIND
-- https://github.com/ClickHouse/ClickHouse/issues/99611

-- HOUR TO MINUTE
SELECT INTERVAL '1:30' HOUR TO MINUTE;
SELECT toTypeName(INTERVAL '1:30' HOUR TO MINUTE);

-- HOUR TO SECOND
SELECT INTERVAL '1:30:45' HOUR TO SECOND;

-- MINUTE TO SECOND
SELECT INTERVAL '5:30' MINUTE TO SECOND;

-- YEAR TO MONTH
SELECT INTERVAL '2-6' YEAR TO MONTH;
SELECT toTypeName(INTERVAL '2-6' YEAR TO MONTH);

-- DAY TO HOUR
SELECT INTERVAL '5 12' DAY TO HOUR;

-- DAY TO MINUTE
SELECT INTERVAL '5 12:30' DAY TO MINUTE;

-- DAY TO SECOND
SELECT INTERVAL '5 12:30:45' DAY TO SECOND;

-- Date arithmetic with compound intervals
SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL '1:30' HOUR TO MINUTE;
SELECT toDate('2024-03-01') + INTERVAL '2-6' YEAR TO MONTH;
SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL '5 12:30:45' DAY TO SECOND;

-- Zero values
SELECT INTERVAL '0:00' HOUR TO MINUTE;
SELECT INTERVAL '0-0' YEAR TO MONTH;

-- Negative leading field (sign applies to the whole literal)
SELECT toDateTime('2024-01-01 12:00:00') + INTERVAL '-1:30' HOUR TO MINUTE;

-- Negative: one hour minus one second (all components are negated)
SELECT toDateTime('2024-01-01 01:00:01') + INTERVAL '-1:00:01' HOUR TO SECOND;

-- Negative year-month
SELECT toDate('2026-09-01') + INTERVAL '-2-6' YEAR TO MONTH;

-- Negative day-time
SELECT toDateTime('2024-01-06 12:30:45') + INTERVAL '-5 12:30:45' DAY TO SECOND;

-- Leading plus sign
SELECT INTERVAL '+2:15' HOUR TO MINUTE;

-- Invalid: FROM kind must be coarser than TO kind
SELECT INTERVAL '1:30' MINUTE TO HOUR; -- { clientError SYNTAX_ERROR }

-- Invalid: cannot mix Year-Month and Day-Time groups
SELECT INTERVAL '1:30' YEAR TO HOUR; -- { clientError SYNTAX_ERROR }

-- Invalid: malformed string
SELECT INTERVAL '1-2-3' YEAR TO MONTH; -- { clientError SYNTAX_ERROR }
SELECT INTERVAL 'abc' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }

-- Invalid: sign on non-leading component (SQL standard: sign applies to whole literal)
SELECT INTERVAL '1:-30' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }
SELECT INTERVAL '5 -12:30' DAY TO MINUTE; -- { clientError SYNTAX_ERROR }
SELECT INTERVAL '1:+30' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }

-- Large non-negative value
SELECT INTERVAL '1000000000:59' HOUR TO MINUTE;

-- Boundary: maximum Int64 value (positive stays positive)
SELECT INTERVAL '9223372036854775807:1' HOUR TO MINUTE;

-- Boundary: exceeds Int64 range (should reject to avoid sign-flip)
SELECT INTERVAL '9223372036854775808:1' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }
SELECT INTERVAL '18446744073709551615:1' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }

-- Boundary: overflow beyond UInt64 range (should reject)
SELECT INTERVAL '18446744073709551616:1' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }

-- Boundary: large value in non-leading component (also rejects beyond Int64 range)
SELECT INTERVAL '1:9223372036854775807' HOUR TO MINUTE;
SELECT INTERVAL '1:9223372036854775808' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }

-- Boundary: negative of Int64 max
SELECT INTERVAL '-9223372036854775807:1' HOUR TO MINUTE;

-- Boundary: negative component exceeds Int64::max magnitude (should reject)
SELECT INTERVAL '-9223372036854775808:1' HOUR TO MINUTE; -- { clientError SYNTAX_ERROR }
