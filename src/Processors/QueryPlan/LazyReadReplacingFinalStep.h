#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyReadReplacingFinalStep : public ISourceStep
{
public:
    LazyReadReplacingFinalStep(
        StorageMetadataPtr metadata_snapshot_,
        const MergeTreeData & data_,
        ContextPtr query_context_,
        LazyFinalSharedStatePtr shared_state_);

    String getName() const override { return "LazyReadReplacingFinal"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    StorageMetadataPtr metadata_snapshot;
    const MergeTreeData & data;
    ContextPtr query_context;
    LazyFinalSharedStatePtr shared_state;
};

}
