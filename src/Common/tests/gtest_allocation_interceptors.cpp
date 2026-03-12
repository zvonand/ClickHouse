#if !defined(SANITIZER)

#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>
#include <cstring>
#include <limits>
#include <netdb.h>
#include <sys/socket.h>

using namespace DB;

/// NOLINTBEGIN

static void __attribute__((noinline)) useMisterPointer(void * p)
{
    __asm__ volatile ("" : : "r"(p) : "memory");
}

/// Allocate 64MB which is bigger than max_untracked_memory
static constexpr int allocation_size = 1024 * 1024 * 64;

void checkMemory(auto allocation_callback, auto deallocation_callback)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::flushUntrackedMemory();

    const auto before_thread = CurrentThread::get().memory_tracker.get();
    const auto before_global = total_memory_tracker.get();

    auto ptr = allocation_callback();
    CurrentThread::flushUntrackedMemory();

    const auto after_thread = CurrentThread::get().memory_tracker.get();
    const auto after_global = total_memory_tracker.get();

    /// The allocation should be tracked (no under-counting) without double-accounting.
    ASSERT_GE(after_thread - before_thread, allocation_size);
    ASSERT_LE(static_cast<double>(after_thread - before_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(after_global - before_global, allocation_size);
    ASSERT_LE(static_cast<double>(after_global - before_global), static_cast<double>(allocation_size) * 1.1);

    deallocation_callback(ptr);
    CurrentThread::flushUntrackedMemory();

    const auto freed_thread = after_thread - CurrentThread::get().memory_tracker.get();
    const auto freed_global = after_global - total_memory_tracker.get();

    /// The deallocation should be tracked without double-accounting.
    ASSERT_GE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 0.95);
    ASSERT_LE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 0.95);
    ASSERT_LE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 1.1);
}

TEST(AllocationInterceptors, MallocIncreasesTheMemoryTracker)
{
    checkMemory([&]()
    {
        /// Several tricks to ensure the compiler doesn't optimize the allocation out.
        [[ maybe_unused ]] void * ptr = malloc(allocation_size);
        useMisterPointer(ptr);
        *reinterpret_cast<char *>(ptr) = 'a';
        return ptr;
    }, [&](void * ptr) { free(ptr); });
}

TEST(AllocationInterceptors, NewDeleteIncreasesTheMemoryTracker)
{
    checkMemory([&]()
    {
        /// Several tricks to ensure the compiler doesn't optimize the allocation out.
        [[ maybe_unused ]] char * ptr = new char[allocation_size];
        useMisterPointer(ptr);
        *ptr = 'a';
        return ptr;
    }, [&](const char * ptr) { delete[] ptr; });
}

#if !defined(SANITIZE_COVERAGE) && !defined(USE_MUSL)

TEST(AllocationInterceptors, StrdupIncreasesTheMemoryTracker)
{
    /// Build a string of allocation_size - 1 characters (+ NUL = allocation_size bytes).
    std::string big(allocation_size - 1, 'x');

    checkMemory([&]()
    {
        char * ptr = strdup(big.c_str());
        useMisterPointer(ptr);
        return ptr;
    }, [&](char * ptr) { free(ptr); });
}

TEST(AllocationInterceptors, StrndupIncreasesTheMemoryTracker)
{
    /// Build a string longer than allocation_size so strndup truncates to exactly allocation_size chars + NUL.
    std::string big(allocation_size * 2, 'y');

    checkMemory([&]()
    {
        char * ptr = strndup(big.c_str(), allocation_size);
        useMisterPointer(ptr);
        return ptr;
    }, [&](char * ptr) { free(ptr); });
}

#endif

TEST(AllocationInterceptors, FailedReallocPreservesOldAllocationAccounting)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::flushUntrackedMemory();

    const Int64 before_alloc_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_alloc_global = total_memory_tracker.get();

    void * ptr = malloc(allocation_size);
    ASSERT_NE(ptr, nullptr);
    useMisterPointer(ptr);
    *reinterpret_cast<char *>(ptr) = 'a';
    CurrentThread::flushUntrackedMemory();

    const auto after_alloc_thread = CurrentThread::get().memory_tracker.get();
    const auto after_alloc_global = total_memory_tracker.get();

    ASSERT_GE(after_alloc_thread - before_alloc_thread, allocation_size);
    ASSERT_LE(static_cast<double>(after_alloc_thread - before_alloc_thread), static_cast<double>(allocation_size) * 1.1);
    ASSERT_GE(after_alloc_global - before_alloc_global, allocation_size);
    ASSERT_LE(static_cast<double>(after_alloc_global - before_alloc_global), static_cast<double>(allocation_size) * 1.1);

    /// A failed realloc must not lose the old block's accounting.
    void * failed_realloc = realloc(ptr, std::numeric_limits<size_t>::max());
    ASSERT_EQ(failed_realloc, nullptr);
    CurrentThread::flushUntrackedMemory();

    const auto thread_after_realloc = CurrentThread::get().memory_tracker.get();
    const auto global_after_realloc = total_memory_tracker.get();

    EXPECT_GE(static_cast<double>(thread_after_realloc), static_cast<double>(after_alloc_thread) * 0.95);
    EXPECT_LE(static_cast<double>(thread_after_realloc), static_cast<double>(after_alloc_thread) * 1.1);
    EXPECT_GE(static_cast<double>(global_after_realloc), static_cast<double>(after_alloc_global) * 0.95);
    EXPECT_LE(static_cast<double>(global_after_realloc), static_cast<double>(after_alloc_global) * 1.1);

    free(ptr);
    CurrentThread::flushUntrackedMemory();

    const auto freed_thread = after_alloc_thread - CurrentThread::get().memory_tracker.get();
    const auto freed_global = after_alloc_global - total_memory_tracker.get();

    EXPECT_GE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 0.95);
    EXPECT_LE(static_cast<double>(freed_thread), static_cast<double>(allocation_size) * 1.1);
    EXPECT_GE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 0.95);
    EXPECT_LE(static_cast<double>(freed_global), static_cast<double>(allocation_size) * 1.1);
}

TEST(AllocationInterceptors, MallocZeroFreeDoesNotCauseNegativeDrift)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    const Int64 before_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_global = total_memory_tracker.get();

    constexpr size_t iterations = 100000;
    for (size_t i = 0; i < iterations; ++i)
    {
        void * ptr = malloc(0);
        free(ptr);
    }

    EXPECT_GE(CurrentThread::get().memory_tracker.get() - before_thread, -64 * 1024);
    EXPECT_GE(total_memory_tracker.get() - before_global, -64 * 1024);
}

#if !defined(SANITIZE_COVERAGE)

namespace
{

/// Mirrors `estimateGetAddrInfoSize` from AllocationInterceptors.cpp.
size_t testEstimateGetAddrInfoSize(const struct addrinfo * result)
{
    size_t total_size = 0;
    const auto * current = result;
    while (current)
    {
        total_size += sizeof(struct addrinfo);
        total_size += current->ai_addrlen;
        if (current->ai_canonname)
            total_size += std::strlen(current->ai_canonname) + 1;

        current = current->ai_next;
    }

    return total_size;
}

}

/// The `addrinfo` linked list returned by `getaddrinfo` is an opaque,
/// resolver-allocated structure with no API for mutation, so in practice
/// `estimateGetAddrInfoSize` returns the same value every time it is called
/// on the same result. This allows `__wrap_freeaddrinfo` to recalculate the
/// size instead of storing it in a tracking map.
TEST(AllocationInterceptors, EstimateGetAddrInfoSizeIsDeterministic)
{
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;

    struct addrinfo * result = nullptr;
    const int res = getaddrinfo("127.0.0.1", "9000", &hints, &result);
    ASSERT_EQ(res, 0) << gai_strerror(res);
    ASSERT_NE(result, nullptr);

    const size_t first = testEstimateGetAddrInfoSize(result);
    const size_t second = testEstimateGetAddrInfoSize(result);
    EXPECT_EQ(first, second);
    EXPECT_GT(first, 0u);

    freeaddrinfo(result);
}

/// Same as above but with `AI_CANONNAME` to exercise the `ai_canonname` branch.
TEST(AllocationInterceptors, EstimateGetAddrInfoSizeIsDeterministicWithCanonName)
{
    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_CANONNAME;

    struct addrinfo * result = nullptr;
    const int res = getaddrinfo("localhost", "9000", &hints, &result);
    if (res != 0 || !result || !result->ai_canonname)
    {
        if (result)
            freeaddrinfo(result);
        GTEST_SKIP() << "Environment does not resolve localhost with a canonical name";
    }

    const size_t first = testEstimateGetAddrInfoSize(result);
    const size_t second = testEstimateGetAddrInfoSize(result);
    EXPECT_EQ(first, second);
    EXPECT_GT(first, 0u);

    /// The canonical name adds to the size estimate.
    EXPECT_GT(first, sizeof(struct addrinfo));

    freeaddrinfo(result);
}

TEST(AllocationInterceptors, GetAddrInfoFreeAddrInfoDoesNotCauseNegativeDrift)
{
    MainThreadStatus::getInstance();
    total_memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.resetCounters();

    const Int64 before_thread = CurrentThread::get().memory_tracker.get();
    const Int64 before_global = total_memory_tracker.get();

    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_NUMERICHOST | AI_NUMERICSERV;

    struct addrinfo * result = nullptr;
    const int res = getaddrinfo("127.0.0.1", "9000", &hints, &result);
    ASSERT_EQ(res, 0) << gai_strerror(res);
    ASSERT_NE(result, nullptr);

    /// Flush untracked memory so the tracker counters reflect the getaddrinfo allocation.
    /// Small allocations stay in the per-thread untracked buffer (4 MB threshold) and
    /// won't be visible via memory_tracker.get() until flushed.
    CurrentThread::flushUntrackedMemory();

    const Int64 after_thread = CurrentThread::get().memory_tracker.get();
    const Int64 after_global = total_memory_tracker.get();
    ASSERT_GT(after_thread, before_thread);
    ASSERT_GT(after_global, before_global);

    freeaddrinfo(result);
    CurrentThread::flushUntrackedMemory();

    const auto thread_after_free = CurrentThread::get().memory_tracker.get();
    const auto global_after_free = total_memory_tracker.get();
    EXPECT_GE(thread_after_free - before_thread, -64 * 1024);
    EXPECT_GE(global_after_free - before_global, -64 * 1024);
}

#endif

/// NOLINTEND

#endif
