#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

/// Flush coverage report to file, depending on coverage system
/// proposed by compiler (llvm for clang and gcov for gcc).
///
/// Noop if build without coverage (WITH_COVERAGE=0).
/// Thread safe (use exclusive lock).
/// Idempotent, may be called multiple times.
void dumpCoverageReportIfPossible();

/// Initialize the coverage mapping from the server's own ELF sections.
/// Reads /proc/self/exe to parse __llvm_covmap and __llvm_covfun sections.
/// Builds a counter→region index for fast per-test scanning.
/// Call once at server startup.
/// Noop if build without coverage (WITH_COVERAGE=0).
void loadCoverageMapping();

/// Atomically flush current coverage for the previous test → reset counters → arm new test name.
/// Call before each test. Empty name flushes without starting a new test.
/// Noop if build without coverage (WITH_COVERAGE=0).
void setCoverageTest(std::string_view test_name);

/// Reset the accumulated coverage.
/// For compatibility: equivalent to setCoverageTest("").
/// Noop if build without coverage (WITH_COVERAGE=0).
void resetCoverage();

#if WITH_COVERAGE

/// Return the NameRef values (MD5 hashes of mangled function names) of all functions
/// whose entry counter is > 0 since the last counter reset.
/// These match the NameRef field in __llvm_profile_data and in __llvm_covfun records.
std::vector<uint64_t> getCurrentCoveredNameRefs();

/// Callback invoked by setCoverageTest when flushing coverage for the previous test.
/// Arguments: (test_name, covered_name_refs).
using CoverageFlushCallback = std::function<void(std::string_view, const std::vector<uint64_t> &)>;

/// Register a callback that is called by setCoverageTest before resetting counters.
/// Only one callback can be registered at a time; a second call overwrites the first.
/// Thread-safe.
void registerCoverageFlushCallback(CoverageFlushCallback cb);

#endif
