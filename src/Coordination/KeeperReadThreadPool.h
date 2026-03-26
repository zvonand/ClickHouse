#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include <base/defines.h>

namespace DB
{

//asdqwe review
//asdqwe ThreadPool instead of raw std::thread
//asdqwe try the primed busy-wait thing
//asdqwe limit on read batch size, in both places

/// A simple thread pool for parallel execution of read request batches.
/// Persistent threads sleep on a condvar between batches. Work is distributed
/// via atomic fetch_add on a shared index (work-stealing by chunk).
struct KeeperReadThreadPool
{
    struct alignas(64) Atomics
    {
        std::atomic<size_t> next_index{0};
        std::atomic<size_t> remaining{0};
    };
    Atomics atomics;

    struct BatchParams
    {
        size_t total{0};
        size_t chunk_size{1};
        std::function<void(size_t /* begin */, size_t /* end */)> execute_range;
    };
    BatchParams params;

    std::vector<std::thread> threads;
    std::mutex wake_mutex;
    std::condition_variable wake_cv;
    std::condition_variable done_cv;

    std::exception_ptr first_exception;
    std::mutex exception_mutex;

    bool shutdown{false};
    bool batch_ready{false};

    explicit KeeperReadThreadPool(size_t num_threads)
    {
        threads.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
            threads.emplace_back([this] { workerLoop(); });
    }

    ~KeeperReadThreadPool()
    {
        {
            std::lock_guard lock(wake_mutex);
            shutdown = true;
        }
        wake_cv.notify_all();
        for (auto & t : threads)
            t.join();
    }

    /// Execute a batch of tasks. Blocks until all are done.
    /// `func(begin, end)` is called for each chunk [begin, end) of task indices.
    void execute(size_t total, size_t chunk_size, std::function<void(size_t, size_t)> func)
    {
        atomics.next_index.store(0, std::memory_order_relaxed);
        atomics.remaining.store(total, std::memory_order_release);

        params.total = total;
        params.chunk_size = chunk_size;
        params.execute_range = std::move(func);

        {
            std::lock_guard lock(wake_mutex);
            batch_ready = true;
        }
        wake_cv.notify_all();

        /// Coordinator also participates as a worker.
        processChunks();

        /// Wait for all workers to finish.
        {
            std::unique_lock lock(wake_mutex);
            done_cv.wait(lock, [this] { return atomics.remaining.load(std::memory_order_acquire) == 0; });
            batch_ready = false;
        }

        if (first_exception)
        {
            auto ex = first_exception;
            first_exception = nullptr;
            std::rethrow_exception(ex);
        }
    }

private:
    void processChunks()
    {
        while (true)
        {
            size_t begin = atomics.next_index.fetch_add(params.chunk_size, std::memory_order_acq_rel);
            if (begin >= params.total)
                break;

            size_t end = std::min(begin + params.chunk_size, params.total);
            size_t count = end - begin;

            try
            {
                params.execute_range(begin, end);
            }
            catch (...)
            {
                std::lock_guard lock(exception_mutex);
                if (!first_exception)
                    first_exception = std::current_exception();
            }

            if (atomics.remaining.fetch_sub(count, std::memory_order_acq_rel) == count)
            {
                /// We were the last — notify the coordinator.
                std::lock_guard lock(wake_mutex);
                done_cv.notify_one();
            }
        }
    }

    void workerLoop()
    {
        while (true)
        {
            {
                std::unique_lock lock(wake_mutex);
                wake_cv.wait(lock, [this] { return batch_ready || shutdown; });
                if (shutdown)
                    return;
            }

            processChunks();
        }
    }
};

}
