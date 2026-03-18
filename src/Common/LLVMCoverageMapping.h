#pragma once

#if defined(__ELF__) && WITH_COVERAGE

#include <cstdint>
#include <string>
#include <vector>

namespace DB
{

struct CoverageRegion
{
    uint64_t name_hash;   /// Matches __llvm_profile_data::NameRef
    std::string file;
    uint32_t line_start;
    uint32_t line_end;
};

/// Parse `__llvm_covfun` and `__llvm_covmap` ELF sections from the binary at binary_path.
/// Returns one CoverageRegion per function (using the function's entry region).
/// Only includes code regions (region_kind == 0), not gap/branch regions.
std::vector<CoverageRegion> readLLVMCoverageMapping(const char * binary_path);

}

#endif
