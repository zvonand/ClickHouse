#pragma once

#include <Processors/ISource.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/ProcessList.h>
#include <IO/Progress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

/// Source, that allow to wait until processing of
/// asynchronous insert for specified query_id will be finished.
class WaitForAsyncInsertSource : public ISource
{
public:
    WaitForAsyncInsertSource(
        std::future<AsyncInsertWriteResult> insert_future_,
        size_t timeout_ms_,
        QueryStatusPtr process_list_elem_,
        ProgressCallback progress_callback_)
        : ISource(std::make_shared<const Block>())
        , insert_future(std::move(insert_future_))
        , timeout_ms(timeout_ms_)
        , process_list_elem(std::move(process_list_elem_))
        , progress_callback(std::move(progress_callback_))
    {
        assert(insert_future.valid());
    }

    String getName() const override { return "WaitForAsyncInsert"; }

protected:
    Chunk generate() override
    {
        auto status = insert_future.wait_for(std::chrono::milliseconds(timeout_ms));
        if (status == std::future_status::deferred)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got future in deferred state");

        if (status == std::future_status::timeout)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Wait for async insert timeout ({} ms) exceeded)", timeout_ms);

        auto write_result = insert_future.get();

        /// Report the written rows/bytes as progress so that query_log
        /// and HTTP X-ClickHouse-Summary reflect the actual insert stats.
        if (process_list_elem)
        {
            Progress write_progress;
            write_progress.written_rows = write_result.written_rows;
            write_progress.written_bytes = write_result.written_bytes;
            process_list_elem->updateProgressOut(write_progress);

            /// For INSERT queries, read_rows/read_bytes should also reflect
            /// the data that was parsed.
            Progress read_progress;
            read_progress.read_rows = write_result.written_rows;
            read_progress.read_bytes = write_result.written_bytes;
            process_list_elem->updateProgressIn(read_progress);
        }

        if (progress_callback)
        {
            Progress p;
            p.written_rows = write_result.written_rows;
            p.written_bytes = write_result.written_bytes;
            p.read_rows = write_result.written_rows;
            p.read_bytes = write_result.written_bytes;
            progress_callback(p);
        }

        return Chunk();
    }

private:
    std::future<AsyncInsertWriteResult> insert_future;
    size_t timeout_ms;
    QueryStatusPtr process_list_elem;
    ProgressCallback progress_callback;
};

}
