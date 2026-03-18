#pragma once

#include <cstdint>
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
