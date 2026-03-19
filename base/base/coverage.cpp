#include "coverage.h"

#pragma clang diagnostic ignored "-Wreserved-identifier"


/// WITH_COVERAGE enables the default implementation of code coverage,
/// that dumps a map to the filesystem.

#if WITH_COVERAGE

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>


/// Minimal re-declaration of the LLVM profiling data structure.
/// Field layout must match compiler-rt/lib/profile/InstrProfiling.h
/// (generated from InstrProfData.inc with IntPtrT = void*).
///
/// Fields in order:
///   uint64_t  NameRef
///   uint64_t  FuncHash
///   void*     CounterPtr   (relative offset: counter = (char*)&record + (intptr_t)CounterPtr)
///   void*     BitmapPtr    (relative offset, similar)
///   void*     FunctionPointer  (absolute virtual address of the function)
///   void*     Values
///   uint32_t  NumCounters
///   uint16_t  NumValueSites[3]  (IPVK_Last+1 = 3)
///   uint32_t  NumBitmapBytes
struct LLVMProfileData  // NOLINT
{
    const uint64_t NameRef;       // NOLINT
    const uint64_t FuncHash;      // NOLINT
    const void * CounterPtr;      // NOLINT  relative: actual counter = (char*)this + (intptr_t)CounterPtr
    const void * BitmapPtr;       // NOLINT  relative, unused here
    const void * FunctionPointer; // NOLINT  absolute virtual address
    void * Values;                // NOLINT
    const uint32_t NumCounters;      // NOLINT
    const uint16_t NumValueSites[3]; // NOLINT  IPVK_Last+1 = 3 (IndirectCall, MemOpSize, VTableTarget)
    const uint32_t NumBitmapBytes;   // NOLINT  at offset 60 after 2-byte implicit padding
};

static_assert(sizeof(LLVMProfileData) == 64,
    "LLVMProfileData size mismatch - field layout must match compiler-rt __llvm_profile_data");
static_assert(offsetof(LLVMProfileData, CounterPtr)     == 16, "LLVMProfileData::CounterPtr offset mismatch");
static_assert(offsetof(LLVMProfileData, FunctionPointer)== 32, "LLVMProfileData::FunctionPointer offset mismatch");
static_assert(offsetof(LLVMProfileData, NumCounters)    == 48, "LLVMProfileData::NumCounters offset mismatch");

extern "C"
{
void __llvm_profile_dump();  // NOLINT
void __llvm_profile_reset_counters();  // NOLINT
void __llvm_profile_set_filename(const char *);  // NOLINT

const LLVMProfileData * __llvm_profile_begin_data();  // NOLINT
const LLVMProfileData * __llvm_profile_end_data();    // NOLINT
uint64_t * __llvm_profile_begin_counters();            // NOLINT
uint64_t * __llvm_profile_end_counters();              // NOLINT
}


namespace
{
    std::mutex g_coverage_mutex;
    std::string g_current_test_name;
    CoverageFlushCallback g_flush_callback;
}


std::vector<std::pair<uint64_t, uint64_t>> getCurrentCoveredNameRefs()
{
    const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
    const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT

    const uint64_t * const cnts_begin = __llvm_profile_begin_counters(); // NOLINT
    const uint64_t * const cnts_end   = __llvm_profile_end_counters();   // NOLINT

    const std::size_t total = static_cast<std::size_t>(end - begin);

    std::vector<std::pair<uint64_t, uint64_t>> result;
    result.reserve(std::min(total, std::size_t{65536}));

    for (const LLVMProfileData * data = begin; data != end; ++data)
    {
        if (!data->NumCounters)
            continue;

        /// CounterPtr is a relative signed offset from the address of the data record.
        const uint64_t * const entry_counter = reinterpret_cast<const uint64_t *>(
            reinterpret_cast<const char *>(data) + reinterpret_cast<intptr_t>(data->CounterPtr));

        if (entry_counter < cnts_begin || entry_counter >= cnts_end)
            continue;

        if (*entry_counter > 0)
            result.emplace_back(data->NameRef, data->FuncHash);
    }

    return result;
}


void registerCoverageFlushCallback(CoverageFlushCallback cb)
{
    std::lock_guard lock(g_coverage_mutex);
    g_flush_callback = std::move(cb);
}


void setCoverageTest(std::string_view test_name)
{
    /// We collect what we need under the lock, then release it before invoking
    /// the callback (which may do heavy DB work and must not hold the mutex).
    std::string prev_test_name;
    std::vector<std::pair<uint64_t, uint64_t>> name_refs;
    CoverageFlushCallback cb;

    {
        std::lock_guard lock(g_coverage_mutex);

        if (!g_current_test_name.empty() && g_flush_callback)
        {
            /// Collect covered NameRefs before resetting counters.
            name_refs = getCurrentCoveredNameRefs();
            prev_test_name = g_current_test_name;
            cb = g_flush_callback;
        }

        __llvm_profile_reset_counters(); // NOLINT

        g_current_test_name = std::string(test_name);
    }

    /// Invoke the callback outside the lock so the DB layer can look up
    /// source locations and insert into system.coverage_log without risking deadlock.
    if (cb)
        cb(prev_test_name, name_refs);
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
    /// Coverage mapping is loaded lazily on the first call to collectAndInsertCoverage
    /// (in CoverageCollection.cpp). This function is kept as a placeholder for potential
    /// eager initialization in the future.
}
