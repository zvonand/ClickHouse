#include "coverage.h"
#include <sys/mman.h>

#pragma clang diagnostic ignored "-Wreserved-identifier"


/// WITH_COVERAGE enables the default implementation of code coverage,
/// that dumps a map to the filesystem.

#if WITH_COVERAGE

#include <mutex>
#include <unistd.h>
#include <string>


extern "C"
{
void __llvm_profile_dump();  // NOLINT
void __llvm_profile_reset_counters();  // NOLINT
}


#endif


void dumpCoverageReportIfPossible()
{
#if WITH_COVERAGE
    static std::mutex mutex;
    std::lock_guard lock(mutex);

    __llvm_profile_dump(); // NOLINT
#endif
}


void loadCoverageMapping()
{
    /// Full LLVM coverage mapping reader will be implemented in a follow-up.
    /// For now this is a noop placeholder.
}


#if WITH_COVERAGE

namespace
{
    std::mutex g_coverage_mutex;
    std::string g_current_test_name;
}

void setCoverageTest(std::string_view test_name)
{
    std::lock_guard lock(g_coverage_mutex);

    /// Reset counters when switching tests.
    __llvm_profile_reset_counters(); // NOLINT

    g_current_test_name = std::string(test_name);
}

void resetCoverage()
{
    setCoverageTest("");
}

#else

void setCoverageTest(std::string_view)
{
}

void resetCoverage()
{
}

#endif
