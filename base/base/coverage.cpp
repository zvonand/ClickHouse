#include "coverage.h"

#pragma clang diagnostic ignored "-Wreserved-identifier"


/// WITH_COVERAGE enables the default implementation of code coverage,
/// that dumps a map to the filesystem.

#if WITH_COVERAGE

#include <algorithm>
#include <atomic>
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

/// ── Shadow call-stack depth tracking ─────────────────────────────────────────
///
/// When the binary is built with -finstrument-functions-after-inlining, the
/// compiler inserts calls to __cyg_profile_func_enter / __cyg_profile_func_exit
/// at every (non-inlined) function entry and exit.  We use these to maintain:
///
///   g_current_depth   — per-thread depth counter, incremented on entry.
///   g_func_min_depth  — per-function minimum depth seen since the last
///                       counter reset.  One atomic<uint32_t> slot per
///                       LLVMProfileData record.  UINT32_MAX = never entered.
///   g_funcptr_lookup  — sorted array of (FunctionPointer, profile_data_index)
///                       pairs for O(log n) lookup in the enter hook.
///
/// All three are initialised lazily on first __cyg_profile_func_enter call.
/// g_func_min_depth is reset to UINT32_MAX in resetDepthTracking(), which is
/// called from setCoverageTest() alongside __llvm_profile_reset_counters().

thread_local uint32_t g_current_depth = 0;

/// Raw array of atomics — std::vector<std::atomic> is not usable because atomic is not moveable.
static std::atomic<uint32_t> * g_func_min_depth = nullptr;
static uint32_t g_func_min_depth_size = 0;
static std::vector<std::pair<uintptr_t, uint32_t>> g_funcptr_lookup; /// sorted by first
static std::once_flag g_depth_init_once;

/// __attribute__((no_instrument_function)) prevents the compiler from
/// instrumenting these functions themselves, which would cause infinite recursion.

static void initDepthTracking() __attribute__((no_instrument_function));
static void initDepthTracking()
{
    std::call_once(g_depth_init_once, []
    {
        const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
        const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT
        const uint32_t n = static_cast<uint32_t>(end - begin);

        /// Initialise min-depth slots (atomic, so can't use assign directly).
        g_func_min_depth = new std::atomic<uint32_t>[n]; // NOLINT
        g_func_min_depth_size = n;
        for (uint32_t j = 0; j < n; ++j)
            g_func_min_depth[j].store(UINT32_MAX, std::memory_order_relaxed);

        /// Build sorted lookup table: (FunctionPointer → profile_data_index).
        g_funcptr_lookup.reserve(n);
        for (uint32_t i = 0; i < n; ++i)
        {
            const auto fp = reinterpret_cast<uintptr_t>(begin[i].FunctionPointer);
            if (fp != 0)
                g_funcptr_lookup.emplace_back(fp, i);
        }
        std::sort(g_funcptr_lookup.begin(), g_funcptr_lookup.end(),
                  [](const auto & a, const auto & b) { return a.first < b.first; });
    });
}

static void resetDepthTracking() __attribute__((no_instrument_function));
static void resetDepthTracking()
{
    for (uint32_t i = 0; i < g_func_min_depth_size; ++i)
        g_func_min_depth[i].store(UINT32_MAX, std::memory_order_relaxed);
}

/// Called by the compiler at every (non-inlined) function entry.
extern "C" void __cyg_profile_func_enter(void * fn, void *) __attribute__((no_instrument_function));
void __cyg_profile_func_enter(void * fn, void *)
{
    const uint32_t depth = ++g_current_depth;

    if (!g_func_min_depth) [[unlikely]]
        initDepthTracking();

    if (!g_func_min_depth)
        return;

    /// Binary-search for the function pointer in the lookup table.
    const auto fp = reinterpret_cast<uintptr_t>(fn);
    const auto it = std::lower_bound(
        g_funcptr_lookup.begin(), g_funcptr_lookup.end(), fp,
        [](const auto & entry, uintptr_t v) { return entry.first < v; });

    if (it == g_funcptr_lookup.end() || it->first != fp)
        return;

    /// Atomic compare-and-swap loop to store the minimum depth seen.
    auto & slot = g_func_min_depth[it->second];
    uint32_t old = slot.load(std::memory_order_relaxed);
    while (old > depth
           && !slot.compare_exchange_weak(old, depth, std::memory_order_relaxed))
    {
    }
}

extern "C" void __cyg_profile_func_exit(void *, void *) __attribute__((no_instrument_function));
void __cyg_profile_func_exit(void *, void *)
{
    --g_current_depth;
}


std::vector<CovCounter> getCurrentCoveredNameRefs()
{
    const LLVMProfileData * begin = __llvm_profile_begin_data(); // NOLINT
    const LLVMProfileData * end   = __llvm_profile_end_data();   // NOLINT

    const uint64_t * const cnts_begin = __llvm_profile_begin_counters(); // NOLINT
    const uint64_t * const cnts_end   = __llvm_profile_end_counters();   // NOLINT

    const std::size_t total = static_cast<std::size_t>(end - begin);

    std::vector<CovCounter> result;
    /// Reserve assuming ~4 non-zero counters per function on average.
    result.reserve(std::min(total * 4, std::size_t{1 << 20}));

    const uint32_t n_profile = static_cast<uint32_t>(end - begin);

    for (uint32_t idx = 0; idx < n_profile; ++idx)
    {
        const LLVMProfileData * data = begin + idx;

        if (!data->NumCounters)
            continue;

        /// CounterPtr is a relative signed offset from the address of the data record.
        const uint64_t * const entry_counter = reinterpret_cast<const uint64_t *>(
            reinterpret_cast<const char *>(data) + reinterpret_cast<intptr_t>(data->CounterPtr));

        /// Validate the entire counter array fits within the counters section.
        if (entry_counter < cnts_begin || entry_counter + data->NumCounters > cnts_end)
            continue;

        /// Skip functions that were never entered — branch counters inside them
        /// would be false positives.
        if (*entry_counter == 0)
            continue;

        /// Read the minimum call depth at which this function was entered.
        /// Cap at 255 (uint8_t max); 255 also serves as "not tracked" sentinel
        /// when the binary was built without -finstrument-functions.
        const uint8_t min_depth = (g_func_min_depth && idx < g_func_min_depth_size)
            ? static_cast<uint8_t>(std::min<uint32_t>(
                  g_func_min_depth[idx].load(std::memory_order_relaxed), 255u))
            : 255u;

        /// Emit one entry per non-zero counter.  Counter 0 is the function entry;
        /// counters 1…N are individual basic-block/branch counters that map to
        /// specific statement-level regions in the LLVM coverage mapping.
        for (uint32_t i = 0; i < data->NumCounters; ++i)
        {
            if (entry_counter[i] > 0)
                result.emplace_back(data->NameRef, data->FuncHash, i, min_depth);
        }
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
    std::vector<CovCounter> name_refs;
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
        resetDepthTracking();

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
