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

}

#endif
