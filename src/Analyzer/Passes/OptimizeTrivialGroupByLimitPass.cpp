#include <Analyzer/Passes/OptimizeTrivialGroupByLimitPass.h>

#include <Analyzer/AggregationUtils.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <base/arithmeticOverflow.h>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsBool optimize_trivial_group_by_limit_query;
}

namespace
{

/// Reads LIMIT/OFFSET as `UInt64`. Analyzer keeps negative or fractional
/// values as `Int64`/`Float64`, so `safeGet<UInt64>` would throw on them.
/// Returns `std::nullopt` for negative or fractional values so the caller
/// can skip the optimization in those cases.
std::optional<UInt64> tryGetNonNegativeUInt64(const Field & field)
{
    const Field converted = convertFieldToType(field, DataTypeUInt64());
    if (converted.isNull())
        return std::nullopt;
    return converted.safeGet<UInt64>();
}

}

void OptimizeTrivialGroupByLimitPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const Settings & settings = context->getSettingsRef();
    if (!settings[Setting::optimize_trivial_group_by_limit_query])
        return;

    auto * query = query_tree_node->as<QueryNode>();
    if (query && query->hasGroupBy() && query->hasLimit() && !query->hasHaving() && !query->hasOrderBy() && !query->hasWindow()
        && !query->isGroupByWithTotals() && !query->isGroupByWithRollup() && !query->isGroupByWithCube()
        && !query->isGroupByWithGroupingSets()
        && !hasAggregateFunctionNodes(query->getProjectionNode()))
    {
        auto & mutable_context = query->getMutableContext();
        if (settings[Setting::max_rows_to_group_by] == 0)
        {
            auto limit = tryGetNonNegativeUInt64(query->getLimit()->as<ConstantNode &>().getValue());
            if (!limit)
                return;
            UInt64 offset = 0;
            if (query->hasOffset())
            {
                auto maybe_offset = tryGetNonNegativeUInt64(query->getOffset()->as<ConstantNode &>().getValue());
                if (!maybe_offset)
                    return;
                offset = *maybe_offset;
            }
            UInt64 max_rows = 0;
            if (common::addOverflow(*limit, offset, max_rows))
                return;
            mutable_context->setSetting("max_rows_to_group_by", max_rows);
            mutable_context->setSetting("group_by_overflow_mode", Field("any"));
        }
    }
}

}
