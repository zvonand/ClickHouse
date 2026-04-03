-- Verify that RANGE(MIN ... MAX ...) correctly validates and configures the max attribute.
-- Previously, buildRangeConfiguration used range->min_attr_name for both min and max lookups
-- (copy-paste bug), so a non-existent max attribute was silently accepted.

DROP TABLE IF EXISTS source_04077;
CREATE TABLE source_04077 (id UInt64, start_date Date, end_date Date, value String) ENGINE = Memory;

-- Test 1: Non-existent MAX attribute must be rejected.
DROP DICTIONARY IF EXISTS dict_04077_bad;
CREATE DICTIONARY dict_04077_bad
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX nonexistent_col); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

-- Test 2: Non-existent MIN attribute must still be rejected (regression guard).
DROP DICTIONARY IF EXISTS dict_04077_bad2;
CREATE DICTIONARY dict_04077_bad2
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN nonexistent_col MAX end_date); -- { serverError INCORRECT_DICTIONARY_DEFINITION }

-- Test 3: Valid dictionary with matching min/max attributes must succeed.
DROP DICTIONARY IF EXISTS dict_04077_good;
CREATE DICTIONARY dict_04077_good
(
    id UInt64,
    start_date Date,
    end_date Date,
    value String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'source_04077'))
LIFETIME(MIN 0 MAX 100)
LAYOUT(RANGE_HASHED())
RANGE(MIN start_date MAX end_date);

SELECT name, type FROM system.dictionaries WHERE name = 'dict_04077_good' AND database = currentDatabase();

DROP DICTIONARY IF EXISTS dict_04077_good;
DROP TABLE IF EXISTS source_04077;
