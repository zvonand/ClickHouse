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
};

namespace QueryPlanFormat
{
    String trimColumnIdentifier(std::string_view name);
    void formatOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);
    void formatJoinOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix);

    String formatNodePretty(const ActionsDAG::Node * node, int parent_precedence = 0);
    String formatNamePrettyIfPossible(const ActionsDAG & dag, const String & name);
    String formatColumnForExplain(const String & column_name, const ExplainFormatSettings & settings);

    void buildPrettyNamesMap(const QueryPlan & plan, std::unordered_map<String, String> & pretty_names);
}

}
