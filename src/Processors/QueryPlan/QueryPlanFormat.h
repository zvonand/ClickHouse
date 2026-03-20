#pragma once

#include <Core/Names.h>
#include <Interpreters/ActionsDAG.h>

#include <string>
#include <string_view>
#include <unordered_map>

namespace DB
{

class WriteBuffer;
class IQueryPlanStep;
class QueryPlan;

struct RuntimeFilterInfo
{
    String pretty_name;
    String build_column_name;
};

struct ExplainFormatSettings
{
    WriteBuffer & out;
    std::string header_prefix;
    std::string detail_prefix;
    size_t offset = 0;
    const size_t base_indent = 2;
    const char indent_char = ' ';
    const bool write_header = false;
    bool compact = false;
    bool pretty = false;
    std::unordered_map<String, String> pretty_names;
    std::unordered_map<String, RuntimeFilterInfo> runtime_filter_names;
};

namespace QueryPlanFormat
{
    String trimColumnIdentifier(std::string_view name);
    void formatOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);
    void formatJoinOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);

    String formatNodePretty(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        int parent_precedence = 0);
    String formatColumnPretty(const String & column_name, const ExplainFormatSettings & settings);

    void buildPrettyNamesMap(
        const QueryPlan & plan,
        std::unordered_map<String, String> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names);
}

}
