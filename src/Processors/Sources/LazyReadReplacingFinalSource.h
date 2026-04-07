#pragma once

#include <Processors/IProcessor.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

struct RangesInDataParts;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class LazyReadReplacingFinalSource : public IProcessor
{
public:
    LazyReadReplacingFinalSource(
        StorageMetadataPtr metadata_snapshot_,
        const MergeTreeData & data_,
        ContextPtr query_context_,
        LazyFinalSharedStatePtr shared_state_);

    String getName() const override { return "LazyReadReplacingFinalSource"; }
    Status prepare() override;
    void work() override;
    Processors expandPipeline() override;

private:
    OutputPort * pipeline_output = nullptr;
    const StorageMetadataPtr metadata_snapshot;
    const MergeTreeData & data;
    const ContextPtr query_context;
    LazyFinalSharedStatePtr shared_state;

    Processors processors;
};

}
