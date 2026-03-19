-- Tags: no-parallel
-- Validates debugAddressToSymbol diagnostic function using system.stack_trace.
-- Does NOT require SANITIZE_COVERAGE.
--
-- debugAddressToSymbol(addr) returns:
--   "no_object"                    — address not in any mapped binary's address range,
--                                    and also fails as a raw file offset
--   "no_symbol_in_object:<path>:offset=0x<hex>" — in binary but no ELF symbol at offset
--   "found:<symbol>"               — resolved successfully

SET allow_introspection_functions = 1;

-- 1. Invalid address → must return 'no_object'
SELECT debugAddressToSymbol(toUInt64(1234)) AS result;

-- 2. Valid stack_trace addresses → addressToSymbol returns non-empty for them,
--    debugAddressToSymbol must return 'found:' prefixed strings (never 'no_object')
SELECT countIf(NOT startsWith(diag, 'found:')) = 0 AS all_valid_addrs_are_found
FROM (
    SELECT
        addr,
        addressToSymbol(addr)      AS sym,
        debugAddressToSymbol(addr) AS diag
    FROM (SELECT arrayJoin(trace) AS addr FROM system.stack_trace LIMIT 1)
    WHERE sym != ''
);

-- 3. debugAddressToSymbol 'found:' result must contain the same symbol as addressToSymbol
SELECT countIf(substring(diag, 7) != sym) = 0 AS found_prefix_matches_addressToSymbol
FROM (
    SELECT
        addressToSymbol(addr)      AS sym,
        debugAddressToSymbol(addr) AS diag
    FROM (SELECT arrayJoin(trace) AS addr FROM system.stack_trace LIMIT 1)
    WHERE sym != ''
      AND startsWith(debugAddressToSymbol(addr), 'found:')
);

-- 4. Category breakdown of stack_trace addresses (informational)
SELECT
    multiIf(
        diag = 'no_object',            'no_object',
        startsWith(diag, 'no_symbol'), 'no_symbol_in_object',
        startsWith(diag, 'found:'),    'found',
                                       'other'
    ) AS category,
    count() AS cnt
FROM (
    SELECT debugAddressToSymbol(arrayJoin(trace)) AS diag
    FROM system.stack_trace
    LIMIT 1
)
GROUP BY category
ORDER BY cnt DESC;
