#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/// PredicateStatisticsLog records predicate selectivity statistics collected
/// from `FilterTransform` (row-level per-atom) and `ReadFromMergeTree` (index-level per-stage)
struct PredicateStatisticsLogElement
{
    UInt16 event_date{};
    time_t event_time{};

    String database;
    String table;
    String query_id;

    String filter_expression;

    String column_name;
    String predicate_class;       /// "Equality", "Range", "In", "LikeSubstring", "IsNull", "Other"
    String function_name;         /// "equals", "less", ...
    UInt64 input_rows{};
    UInt64 passed_rows{};
    Float64 filter_selectivity{}; /// passed_rows / input_rows

    std::vector<String> index_names;
    std::vector<String> index_types;
    std::vector<UInt64> total_granules;
    std::vector<UInt64> granules_after;
    std::vector<Float64> index_selectivities;

    static std::string name() { return "PredicateStatisticsLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class PredicateStatisticsLog : public SystemLog<PredicateStatisticsLogElement>
{
    using SystemLog<PredicateStatisticsLogElement>::SystemLog;
};

}
