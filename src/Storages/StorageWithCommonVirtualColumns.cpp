#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/VirtualColumnsDescription.h>
#include <Storages/VirtualColumnUtils.h>

#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Interpreters/ActionsDAG.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void materializeConstantColumn(QueryPlan & query_plan, const std::string & name, const DataTypePtr & type, const Field & value)
{
    auto step = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), ActionsDAG::makeAddingConstantColumnActions(name, type, value));
    step->setStepDescription(fmt::format("Materialize {} virtual column", name), 100);
    query_plan.addStep(std::move(step));
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
    /// Proxy to implementation storage
    const auto physical_columns = VirtualColumnUtils::filterVirtualColumns(column_names, {"_table", "_database"}, storage_snapshot->metadata, getVirtualsPtr());
    readImpl(query_plan, physical_columns, storage_snapshot, query_info,
             context, processed_stage, max_block_size, num_streams);

    /// Materialize constant virtuals
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
