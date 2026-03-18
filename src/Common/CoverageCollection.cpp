#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE

#include <Common/CoverageCollection.h>
#include <Common/LLVMCoverageMapping.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/BlockIO.h>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace
{

/// Lazily-loaded map from NameRef → CoverageRegion.
/// Populated on the first call to collectAndInsertCoverage by reading
/// `/proc/self/exe`'s `__llvm_covmap` and `__llvm_covfun` ELF sections.
std::unordered_map<uint64_t, CoverageRegion> g_coverage_map;
std::once_flag g_coverage_map_once;

void ensureCoverageMapLoaded()
{
    std::call_once(g_coverage_map_once, []
    {
        const auto regions = readLLVMCoverageMapping("/proc/self/exe");
        g_coverage_map.reserve(regions.size());
        for (const CoverageRegion & r : regions)
            g_coverage_map.emplace(r.name_hash, r);

        LOG_INFO(
            getLogger("CoverageCollection"),
            "Loaded {} function regions from LLVM coverage mapping",
            g_coverage_map.size());
    });
}

} // anonymous namespace


void collectAndInsertCoverage(
    std::string_view test_name,
    const std::vector<uint64_t> & name_refs,
    ContextPtr context)
{
    if (name_refs.empty())
        return;

    ensureCoverageMapLoaded();

    /// Collect unique (file, line_start, line_end) triples.
    struct LineKey
    {
        std::string file;
        uint32_t line_start;
        uint32_t line_end;

        bool operator==(const LineKey & o) const
        {
            return line_start == o.line_start && line_end == o.line_end && file == o.file;
        }
    };
    struct LineKeyHash
    {
        std::size_t operator()(const LineKey & k) const
        {
            std::size_t h = std::hash<std::string>{}(k.file);
            h ^= std::hash<uint32_t>{}(k.line_start) + 0x9e3779b9u + (h << 6) + (h >> 2);
            h ^= std::hash<uint32_t>{}(k.line_end)   + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };

    std::unordered_map<LineKey, bool, LineKeyHash> seen;
    seen.reserve(name_refs.size());

    std::vector<std::string> files;
    std::vector<uint32_t> line_starts;
    std::vector<uint32_t> line_ends;

    for (const uint64_t nr : name_refs)
    {
        const auto it = g_coverage_map.find(nr);
        if (it == g_coverage_map.end())
            continue;

        const CoverageRegion & region = it->second;
        if (region.file.empty() || region.line_start == 0)
            continue;

        LineKey key{region.file, region.line_start, region.line_end};
        if (!seen.emplace(key, true).second)
            continue;

        files.push_back(region.file);
        line_starts.push_back(region.line_start);
        line_ends.push_back(region.line_end);
    }

    if (files.empty())
        return;

    /// Build the INSERT query as a VALUES literal.
    /// Schema: coverage_log (time DateTime, test_name String, files Array(String),
    ///                        line_starts Array(UInt32), line_ends Array(UInt32))
    WriteBufferFromOwnString query_buf;
    writeString("INSERT INTO system.coverage_log (time, test_name, files, line_starts, line_ends) VALUES (now(), ", query_buf);
    writeQuotedString(test_name, query_buf);
    writeString(", [", query_buf);
    for (size_t i = 0; i < files.size(); ++i)
    {
        if (i > 0)
            writeChar(',', query_buf);
        writeQuotedString(files[i], query_buf);
    }
    writeString("], [", query_buf);
    for (size_t i = 0; i < line_starts.size(); ++i)
    {
        if (i > 0)
            writeChar(',', query_buf);
        writeIntText(line_starts[i], query_buf);
    }
    writeString("], [", query_buf);
    for (size_t i = 0; i < line_ends.size(); ++i)
    {
        if (i > 0)
            writeChar(',', query_buf);
        writeIntText(line_ends[i], query_buf);
    }
    writeString("])", query_buf);

    const std::string query = query_buf.str();

    try
    {
        auto query_context = Context::createCopy(context->getGlobalContext());
        query_context->makeQueryContext();
        query_context->setCurrentQueryId({});
        auto block_io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
        block_io.onFinish();
    }
    catch (const Exception & e)
    {
        LOG_WARNING(
            getLogger("CoverageCollection"),
            "Failed to insert coverage for test '{}': {}",
            test_name, e.message());
    }
    catch (...)
    {
        LOG_WARNING(
            getLogger("CoverageCollection"),
            "Failed to insert coverage for test '{}'",
            test_name);
    }
}

} // namespace DB

#endif
