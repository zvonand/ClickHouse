#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

/// Unite several logical streams of data into single logical stream with specified structure.
class UnionStep : public IQueryPlanStep
{
public:
    /// max_threads is used to limit the number of threads for result pipeline.
    /// max_streams limits the number of simultaneously active streams via ConcatProcessors.
    /// max_streams_ratio multiplied by max_threads gives a dynamic stream limit.
    explicit UnionStep(SharedHeaders input_headers_, size_t max_threads_ = 0, size_t max_streams_ = 0, double max_streams_ratio_ = 0.0);

    String getName() const override { return "Union"; }

    QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) override;

    void describePipeline(FormatSettings & settings) const override;

    size_t getMaxThreads() const { return max_threads; }
    size_t getMaxStreams() const { return max_streams; }
    double getMaxStreamsRatio() const { return max_streams_ratio; }

    void serializeSettings(QueryPlanSerializationSettings & settings) const override;
    void serialize(Serialization & ctx) const override;
    bool isSerializable() const override { return true; }

    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool hasCorrelatedExpressions() const override { return false; }

private:
    void updateOutputHeader() override;

    size_t max_threads;
    size_t max_streams;
    double max_streams_ratio;
};

}
