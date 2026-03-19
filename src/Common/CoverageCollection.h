#pragma once

#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE

#include <Interpreters/Context_fwd.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace DB
{

/// Look up each NameRef in the pre-loaded LLVM coverage map and insert one
/// row into system.coverage_log with the resolved (file, line_start, line_end)
/// arrays.
///
/// @param test_name   Name of the test whose coverage is being flushed.
/// @param name_refs   NameRef values returned by getCurrentCoveredNameRefs().
/// @param context     A ContextPtr with access to the system database.
///
/// The function is a no-op when the system.coverage_log table does not exist.
void collectAndInsertCoverage(
    std::string_view test_name,
    const std::vector<uint64_t> & name_refs,
    ContextPtr context);

/// Returns the number of entries in the lazily-loaded coverage map.
size_t getCoverageMapSize();

/// Returns how many of the given NameRefs have a match in the coverage map.
size_t countCoverageMatches(const std::vector<uint64_t> & name_refs);

/// Returns the first key in the coverage map (for diagnostics), 0 if empty.
uint64_t getFirstCoverageMapKey();

/// Returns (non_empty_file_count, zero_line_count, first_file_len<<32|first_line).
std::tuple<size_t, size_t, uint64_t> diagCoverageRegions(const std::vector<uint64_t> & name_refs);

}

#endif
