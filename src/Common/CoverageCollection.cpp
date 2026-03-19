#if defined(__ELF__) && !defined(OS_FREEBSD) && WITH_COVERAGE

#include <Common/CoverageCollection.h>
#include <Common/LLVMCoverageMapping.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/BlockIO.h>
#include <base/coverage.h>

#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{

namespace
{

/// Key for the coverage map: (NameRef, FuncHash) pair.
/// Using both fields ensures uniqueness when the same function name hash
/// appears with different body hashes (e.g. overloads or template specialisations
/// that happen to collide on the name hash).
struct CoverageKey
{
    uint64_t name_hash;
    uint64_t func_hash;

    bool operator==(const CoverageKey & o) const
    {
        return name_hash == o.name_hash && func_hash == o.func_hash;
    }
};

struct CoverageKeyHash
{
    std::size_t operator()(const CoverageKey & k) const
    {
        /// Mix the two hashes using a multiplicative constant to spread bits.
        return k.name_hash ^ (k.func_hash * 0x9e3779b97f4a7c15ULL);
    }
};

/// Lazily-loaded map from (NameRef, FuncHash) → CoverageRegion.
/// Populated on the first call to collectAndInsertCoverage by reading
/// `/proc/self/exe`'s `__llvm_covmap` and `__llvm_covfun` ELF sections.
std::unordered_map<CoverageKey, CoverageRegion, CoverageKeyHash> g_coverage_map;
std::once_flag g_coverage_map_once;

void ensureCoverageMapLoaded()
{
    std::call_once(g_coverage_map_once, []
    {
        const auto regions = readLLVMCoverageMapping("/proc/self/exe");
        g_coverage_map.reserve(regions.size());
        for (const CoverageRegion & r : regions)
            g_coverage_map.emplace(CoverageKey{r.name_hash, r.func_hash}, r);

        LOG_INFO(
            getLogger("CoverageCollection"),
            "Loaded {} function regions from LLVM coverage mapping",
            g_coverage_map.size());
    });
}

} // anonymous namespace


void collectAndInsertCoverage(
    std::string_view test_name,
    const std::vector<std::pair<uint64_t, uint64_t>> & name_refs,
    ContextPtr context)
{
    LOG_INFO(getLogger("CoverageCollection"),
        "Flushing test '{}': {} covered NameRefs, coverage map size {}",
        test_name, name_refs.size(), g_coverage_map.size());

    if (name_refs.empty())
    {
        auto msg = fmt::format("CoverageCollection: No covered NameRefs for test '{}', skipping", test_name);
        LOG_INFO(getLogger("CoverageCollection"), "{}", msg);
        std::cerr << msg << "\n";
        return;
    }

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

    for (const auto & [name_hash, func_hash] : name_refs)
    {
        const auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash});
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

    LOG_INFO(getLogger("CoverageCollection"),
        "Test '{}': {} NameRefs resolved to {} unique (file, line) pairs (map_size={})",
        test_name, name_refs.size(), files.size(), g_coverage_map.size());

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
        /// Allow arbitrarily large INSERT queries — the VALUES literal may be megabytes
        /// when a test covers many thousands of functions.
        query_context->setSetting("max_query_size", Field{0ULL});
        auto block_io = executeQuery(query, query_context, QueryFlags{.internal = true}).second;
        block_io.onFinish();
        std::cerr << fmt::format(
            "CoverageCollection: Inserted coverage for test '{}': {} regions\n",
            test_name, files.size());
    }
    catch (const Exception & e)
    {
        auto msg = fmt::format(
            "CoverageCollection: Failed to insert coverage for test '{}': code={} msg={}",
            test_name, e.code(), e.message());
        LOG_WARNING(getLogger("CoverageCollection"), "{}", msg);
        std::cerr << msg << "\n";
    }
    catch (...)
    {
        auto msg = fmt::format(
            "CoverageCollection: Failed to insert coverage for test '{}': unknown exception",
            test_name);
        LOG_WARNING(getLogger("CoverageCollection"), "{}", msg);
        std::cerr << msg << "\n";
    }
}

size_t getCoverageMapSize()
{
    ensureCoverageMapLoaded();
    return g_coverage_map.size();
}

size_t countCoverageMatches(const std::vector<std::pair<uint64_t, uint64_t>> & name_refs)
{
    ensureCoverageMapLoaded();
    size_t count = 0;
    for (const auto & [name_hash, func_hash] : name_refs)
        if (g_coverage_map.count(CoverageKey{name_hash, func_hash}))
            ++count;
    return count;
}

uint64_t getFirstCoverageMapKey()
{
    ensureCoverageMapLoaded();
    if (g_coverage_map.empty())
        return 0;
    return g_coverage_map.begin()->first.name_hash;
}

/// Returns (non_empty_file_count, zero_line_count, first_file_hash)
/// among matched regions for diagnostic purposes.
std::tuple<size_t, size_t, uint64_t> diagCoverageRegions(const std::vector<std::pair<uint64_t, uint64_t>> & name_refs)
{
    ensureCoverageMapLoaded();
    size_t non_empty = 0, zero_line = 0;
    uint64_t first_file_hash = 0;
    for (const auto & [name_hash, func_hash] : name_refs)
    {
        auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash});
        if (it == g_coverage_map.end()) continue;
        const CoverageRegion & r = it->second;
        if (!r.file.empty())
        {
            ++non_empty;
            if (first_file_hash == 0)
            {
                /// Store length of first file as a proxy diagnostic
                first_file_hash = static_cast<uint64_t>(r.file.size()) << 32
                    | static_cast<uint64_t>(r.line_start);
            }
        }
        if (r.line_start == 0) ++zero_line;
    }
    return {non_empty, zero_line, first_file_hash};
}

CurrentCoverageRegions getCurrentCoverageRegions()
{
    ensureCoverageMapLoaded();

    auto name_refs = getCurrentCoveredNameRefs();

    /// Collect unique (file, line_start, line_end) triples — same dedup logic as collectAndInsertCoverage.
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

    CurrentCoverageRegions out;
    for (const auto & [name_hash, func_hash] : name_refs)
    {
        const auto it = g_coverage_map.find(CoverageKey{name_hash, func_hash});
        if (it == g_coverage_map.end())
            continue;

        const CoverageRegion & region = it->second;
        if (region.file.empty() || region.line_start == 0)
            continue;

        LineKey key{region.file, region.line_start, region.line_end};
        if (!seen.emplace(key, true).second)
            continue;

        out.files.push_back(region.file);
        out.line_starts.push_back(region.line_start);
        out.line_ends.push_back(region.line_end);
    }

    return out;
}

}

#endif
