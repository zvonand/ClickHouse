#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/VirtualColumnUtils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Interpreters/ActionsDAG.h>

#include <base/scope_guard.h>

namespace DB
{

namespace
{

const NameSet common_virtual_names = {"_table", "_database"};

void materializeConstantColumn(QueryPlan & query_plan, const std::string & name, const DataTypePtr & type, const Field & value)
{
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), ActionsDAG::makeAddingConstantColumnActions(name, type, value));
    step->setStepDescription(fmt::format("Materialize {} virtual column", name), 100);
    query_plan.addStep(std::move(step));
}

VirtualsDescriptionPtr filterCommonVirtuals(VirtualsDescriptionPtr initial)
{
    auto filtered_virtuals = std::make_unique<VirtualColumnsDescription>();
    for (const auto & col : initial->getNamesAndTypesList())
        if (!common_virtual_names.contains(col.name))
            filtered_virtuals->add(initial->getDescription(col.name));

    return filtered_virtuals;
}

}

void StorageWithCommonVirtualColumns::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    /// Build a snapshot without common virtuals so readImpl doesn't see them.
    auto filtered_columns = VirtualColumnUtils::filterVirtualColumns(column_names, common_virtual_names, storage_snapshot->metadata, storage_snapshot->virtual_columns);
    auto filtered_snapshot = std::make_shared<StorageSnapshot>(storage_snapshot->storage, storage_snapshot->metadata, filterCommonVirtuals(storage_snapshot->virtual_columns));

    /// Complete snapshot and rollback to initial to not break constant semantics.
    std::swap(filtered_snapshot->data, storage_snapshot->data);
    SCOPE_EXIT({ std::swap(filtered_snapshot->data, storage_snapshot->data); });

    /// Proxy to underlying storage.
    readImpl(query_plan, filtered_columns, filtered_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    /// Materialize constant virtuals.
    if (query_plan.isInitialized())
    {
        if (std::ranges::contains(column_names, "_database") && !query_plan.getCurrentHeader()->has("_database"))
            materializeConstantColumn(query_plan, "_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), getStorageID().getDatabaseName());

        if (std::ranges::contains(column_names, "_table") && !query_plan.getCurrentHeader()->has("_table"))
            materializeConstantColumn(query_plan, "_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), getStorageID().getTableName());
    }
}

void StorageWithCommonVirtualColumns::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    IStorage::read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
}

}
