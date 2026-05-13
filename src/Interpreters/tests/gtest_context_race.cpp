#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Core/Field.h>
#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <vector>

using namespace DB;

template <typename Ptr>
void run(Ptr context)
{
    for (size_t i = 0; i < 100; ++i)
    {
        std::thread t1([context]
        {
            if constexpr (std::is_same_v<ContextWeakPtr, Ptr>)
                context.lock()->getAsyncReadCounters();
            else
                context->getAsyncReadCounters();
        });

        std::thread t2([context]
        {
            Context::createCopy(context);
        });

        t1.join();
        t2.join();
    }
}

TEST(Context, MutableRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextMutablePtr>(context);
}

TEST(Context, ConstRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextPtr>(context);
}

TEST(Context, WeakRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    run<ContextWeakPtr>(context);
}

/// Test for data race in Context::getAccess() where need_recalculate_access
/// was written under a shared lock while being read by another thread.
/// Multiple threads call getAccess() on the same context while another thread
/// toggles need_recalculate_access via setSetting with an access-dependent setting.
TEST(Context, GetAccessRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Populate the cached access object.
    context->getAccess();

    constexpr size_t num_reader_threads = 4;
    constexpr size_t num_iterations = 1000;
    std::atomic<bool> stop{false};

    /// Reader threads: call getAccess() concurrently on the same context.
    std::vector<std::thread> readers;
    for (size_t i = 0; i < num_reader_threads; ++i)
    {
        readers.emplace_back([&context, &stop]
        {
            while (!stop.load(std::memory_order_relaxed))
                context->getAccess();
        });
    }

    /// Writer thread: toggle need_recalculate_access by setting allow_ddl
    /// (one of the three settings in ContextAccessParams::dependsOnSettingName).
    std::thread writer([&context, &stop]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            context->setSetting("allow_ddl", Field(UInt64(1)));
        stop.store(true, std::memory_order_relaxed);
    });

    writer.join();
    for (auto & t : readers)
        t.join();
}

/// Test for data race in `ContextData` copy constructor on `table_function_results`.
///
/// The writer thread calls `Context::executeTableFunction`, which mutates
/// `table_function_results` under `table_function_results_mutex`.
/// The copier thread calls `Context::createCopy`, which invokes the
/// `ContextData(const ContextData &)` copy constructor.
///
/// Without the fix the copy constructor read `o.table_function_results`
/// in its initializer list without acquiring `o.table_function_results_mutex`,
/// and TSan reported a data race against the writer's `emplace`. With the fix
/// the copy of `table_function_results` happens under that mutex.
///
/// See issue ClickHouse/ClickHouse#104807 (STID 1003-358c).
TEST(Context, TableFunctionResultsCopyRace)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Build a minimal `numbers(1)` AST -- the table function does not need a
    /// real ClickHouse server backing to exercise the cache path; even if the
    /// execution itself fails, the code path through `table_function_results`
    /// is unchanged and is what we need to race against.
    auto numbers_ast = makeASTFunction("numbers", make_intrusive<ASTLiteral>(Field(UInt64(1))));

    /// Warm up so the table function machinery is initialized -- ignore any
    /// errors, we only care about reaching the map.
    try
    {
        (void)context->executeTableFunction(numbers_ast);
    }
    catch (...) /// NOLINT(bugprone-empty-catch)
    {
    }

    constexpr size_t num_iterations = 200;
    std::atomic<bool> stop{false};

    /// Writer thread: keep calling executeTableFunction so `table_function_results`
    /// keeps being mutated (insert into the map) under its mutex.
    std::thread writer([&]
    {
        while (!stop.load(std::memory_order_relaxed))
        {
            try
            {
                (void)context->executeTableFunction(numbers_ast);
            }
            catch (...) /// NOLINT(bugprone-empty-catch)
            {
            }
        }
    });

    /// Copier thread: keep copying the context, which invokes the
    /// `ContextData` copy constructor that reads `o.table_function_results`.
    std::thread copier([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            (void)Context::createCopy(context);
        stop.store(true, std::memory_order_relaxed);
    });

    copier.join();
    writer.join();
}
