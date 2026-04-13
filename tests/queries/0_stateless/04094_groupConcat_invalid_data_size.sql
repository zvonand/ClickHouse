-- Verify that groupConcat deserialize rejects absurdly large data_size values.
-- The fuzzer found that CAST(unhex(...), 'AggregateFunction(groupConcat, ...)') with
-- garbage binary data could decode an enormous VarUInt for data_size, hitting a
-- LOGICAL_ERROR in the allocator (fatal in sanitizer builds) instead of a clean
-- user-facing exception.

-- VarUInt 80808080808080808001 decodes to 2^63, which is >= 0x8000000000000000
-- (the allocator's LOGICAL_ERROR threshold).

-- Without the has_limit path (no parameters to groupConcat):
SELECT finalizeAggregation(CAST(unhex('80808080808080808001'), 'AggregateFunction(groupConcat, String)')); -- { serverError BAD_ARGUMENTS }

-- With the has_limit path (groupConcat with delimiter and limit):
SELECT finalizeAggregation(CAST(unhex('80808080808080808001'), 'AggregateFunction(groupConcat(\',\', 10), String)')); -- { serverError BAD_ARGUMENTS }

-- Original fuzzer-found case that triggered LOGICAL_ERROR in the allocator:
SELECT CAST(unhex(toFixedString('', 30)), 'AggregateFunction(groupConcat(\',\', 10), String)'); -- { serverError BAD_ARGUMENTS }
