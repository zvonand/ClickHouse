#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

namespace DB
{

std::optional<Range> createRangeFromEstimate(const Estimate & estimate, const DataTypePtr & /*data_type*/, bool is_nullable)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    bool has_minmax = estimate.estimated_min.has_value() && estimate.estimated_max.has_value();
    bool has_null_count = estimate.estimated_null_count.has_value();

    /// Handle NullCount-only case for nullable columns
    if (!has_minmax && has_null_count && is_nullable)
    {
        UInt64 null_count = *estimate.estimated_null_count;
        if (null_count >= estimate.rows_count)
            /// All-NULL part: use [+inf, +inf] so equality/less predicates prune it.
            /// Greater-than predicates won't prune it — accepted asymmetry.
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        if (null_count == 0)
            return Range::createWholeUniverseWithoutNull();
    }

    if (!has_minmax)
        return std::nullopt;

    const Field & min_value = *estimate.estimated_min;
    const Field & max_value = *estimate.estimated_max;

    /// Guard against corrupted or partially-written stats where min > max.
    if (Range::less(max_value, min_value))
        return std::nullopt;

    if (!is_nullable || (has_null_count && *estimate.estimated_null_count == 0))
        return Range(min_value, true, max_value, true);

    return Range(min_value, true, POSITIVE_INFINITY, true);
}

namespace
{

std::optional<Range> createRangeFromNullCount(const Estimate & estimate)
{
    if (!estimate.estimated_null_count.has_value())
        return std::nullopt;

    UInt64 null_count = *estimate.estimated_null_count;
    UInt64 row_count = estimate.rows_count;

    if (null_count == 0)
        return Range(UInt64(0), true, UInt64(0), true);
    if (null_count >= row_count)
        return Range(UInt64(1), true, UInt64(1), true);
    return Range(UInt64(0), true, UInt64(1), true);
}

std::optional<String> tryResolveVirtualKeyParent(
    const String & subcol_name, const ColumnsDescription & columns)
{
    auto dot_pos = subcol_name.rfind('.');
    if (dot_pos == std::string::npos)
        return std::nullopt;

    String parent_name = subcol_name.substr(0, dot_pos);
    String subcol_suffix = subcol_name.substr(dot_pos + 1);

    const auto * col = columns.tryGet(parent_name);
    if (!col)
        return std::nullopt;

    if (subcol_suffix == "null"
        && col->statistics.types_to_desc.contains(StatisticsType::NullCount)
        && isNullableOrLowCardinalityNullable(col->type))
    {
        return parent_name;
    }

    return std::nullopt;
}

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : filter_dag(&filter_node_, context_)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    for (const auto & name : filter_columns)
    {
        const auto * col = columns.tryGet(name);

        if (col)
        {
            if (col->statistics.types_to_desc.contains(StatisticsType::MinMax)
                || col->statistics.types_to_desc.contains(StatisticsType::NullCount))
            {
                stats_column_name_to_type_map[col->name] = col->type;
                useless = false;
            }
        }
        else
        {
            auto parent = tryResolveVirtualKeyParent(name, columns);
            if (parent.has_value())
            {
                stats_column_name_to_type_map[name] = std::make_shared<DataTypeUInt8>();
                virtual_key_to_parent[name] = *parent;
                useless = false;
            }
        }
    }
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns)
{
    const auto column_names = columns.getNames();

    auto it = key_condition_cache.find(column_names);
    if (it != key_condition_cache.end())
        return it->second.get();

    ActionsDAG actions_dag(columns);
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

    auto finalize_key_condition = [&](std::unique_ptr<KeyCondition> kc) -> KeyCondition *
    {
        if (kc->alwaysUnknownOrTrue())
        {
            key_condition_cache[column_names] = nullptr;
            return nullptr;
        }

        auto * ptr = kc.get();
        key_condition_cache[column_names] = std::move(kc);

        for (size_t col_idx : ptr->getUsedColumns())
        {
            if (col_idx < column_names.size())
                used_column_names.insert(column_names[col_idx]);
        }

        return ptr;
    };

    if (filter_dag.dag && filter_dag.predicate)
    {
        ActionsDAGWithInversionPushDown normalized_filter_dag(
            filter_dag.predicate, context, /*normalize_null_columns=*/true);

        return finalize_key_condition(
            std::make_unique<KeyCondition>(normalized_filter_dag, context, column_names, expression));
    }

    return finalize_key_condition(
        std::make_unique<KeyCondition>(filter_dag, context, column_names, expression));
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    Estimates relevant_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.types.contains(StatisticsType::MinMax)
            || estimate.types.contains(StatisticsType::NullCount))
            relevant_estimates[col_name] = estimate;
    }

    if (relevant_estimates.empty())
        return {true, true};

    /// Use only columns that are both in filter and have estimates.
    /// Virtual key columns (e.g., value.null) map to their parent column name
    /// in relevant_estimates (e.g., value).
    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        auto vit = virtual_key_to_parent.find(col_name);
        String lookup_name = vit != virtual_key_to_parent.end() ? vit->second : col_name;
        if (relevant_estimates.contains(lookup_name))
            columns.emplace_back(col_name, col_type);
    }

    if (columns.empty())
        return {true, true};

    KeyCondition * key_condition = getKeyConditionForEstimates(columns);

    if (key_condition)
    {
        Hyperrectangle hyperrectangle;
        DataTypes types;

        for (const auto & [col_name, col_type] : columns)
        {
            auto vit = virtual_key_to_parent.find(col_name);
            if (vit != virtual_key_to_parent.end())
            {
                auto est_it = relevant_estimates.find(vit->second);
                auto range = est_it != relevant_estimates.end()
                    ? createRangeFromNullCount(est_it->second)
                    : std::nullopt;
                hyperrectangle.emplace_back(range.has_value() ? std::move(*range) : Range::createWholeUniverse());
                types.push_back(col_type);
                continue;
            }

            auto est_it = relevant_estimates.find(col_name);
            if (est_it == relevant_estimates.end())
            {
                hyperrectangle.emplace_back(Range::createWholeUniverse());
                types.push_back(col_type);
                continue;
            }

            auto is_nullable_type = isNullableOrLowCardinalityNullable(col_type);
            auto range = createRangeFromEstimate(est_it->second, col_type, is_nullable_type);

            if (range.has_value())
                hyperrectangle.push_back(std::move(*range));
            else
            {
                if (is_nullable_type)
                    hyperrectangle.emplace_back(Range::createWholeUniverse());
                else
                    hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
            }
            types.push_back(col_type);
        }

        return key_condition->checkInHyperrectangle(hyperrectangle, types);
    }

    return {true, true};
}
}
