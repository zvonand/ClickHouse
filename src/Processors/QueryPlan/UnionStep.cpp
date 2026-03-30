#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <base/defines.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_streams_for_union_step;
    extern const QueryPlanSerializationSettingsFloat max_streams_for_union_step_to_max_threads_ratio;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static SharedHeader checkHeaders(const SharedHeaders & input_headers)
{
    if (input_headers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot unite an empty set of query plan steps");

    auto res = input_headers.front();
    for (const auto & header : input_headers)
        assertBlocksHaveEqualStructure(*header, *res, "UnionStep");

    return res;
}

UnionStep::UnionStep(SharedHeaders input_headers_, size_t max_threads_, size_t max_streams_, double max_streams_ratio_)
    : max_threads(max_threads_)
    , max_streams(max_streams_)
    , max_streams_ratio(max_streams_ratio_)
{
    updateInputHeaders(std::move(input_headers_));
}

void UnionStep::updateOutputHeader()
{
    output_header = checkHeaders(input_headers);
}

QueryPipelineBuilderPtr UnionStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    if (pipelines.empty())
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->init(Pipe(std::make_shared<NullSource>(output_header)));
        processors = collector.detachProcessors();
        return pipeline;
    }

    size_t new_max_threads = max_threads ? max_threads : settings.max_threads;

    for (auto & cur_pipeline : pipelines)
    {
        /// Headers for union must be equal.
        /// But, just in case, convert it to the same header if not.
        /// This can happen when PREWHERE optimization adds extra pass-through columns
        /// to ReadFromMergeTree output that are not consumed by the expression DAG above,
        /// causing plan headers and pipeline headers to diverge.
        if (!blocksHaveEqualStructure(cur_pipeline->getHeader(), *getOutputHeader()))
        {
            QueryPipelineProcessorsCollector collector(*cur_pipeline, this);
            auto converting_dag = ActionsDAG::makeConvertingActions(
                cur_pipeline->getHeader().getColumnsWithTypeAndName(),
                getOutputHeader()->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name,
                nullptr);

            auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
            cur_pipeline->addSimpleTransform([&](const SharedHeader & cur_header)
            {
                return std::make_shared<ExpressionTransform>(cur_header, converting_actions);
            });

            auto added_processors = collector.detachProcessors();
            processors.insert(processors.end(), added_processors.begin(), added_processors.end());
        }

#if defined(DEBUG_OR_SANITIZER_BUILD)
        assertCompatibleHeader(cur_pipeline->getHeader(), *getOutputHeader(), "UnionStep");
#endif
    }

    *pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines), new_max_threads, &processors);

    /// Compute effective stream limit from the raw settings and the actual max_threads
    /// available on this node (which may differ from the coordinator in distributed execution).
    size_t effective_max_streams = max_streams;
    if (max_streams_ratio > 0 && new_max_threads > 0)
    {
        size_t max_streams_from_ratio = static_cast<size_t>(static_cast<double>(new_max_threads) * max_streams_ratio);
        if (max_streams_from_ratio == 0)
            max_streams_from_ratio = 1;
        if (effective_max_streams)
            effective_max_streams = std::min(effective_max_streams, max_streams_from_ratio);
        else
            effective_max_streams = max_streams_from_ratio;
    }

    if (effective_max_streams && pipeline->getNumStreams() > effective_max_streams)
    {
        QueryPipelineProcessorsCollector collector(*pipeline, this);
        pipeline->narrow(effective_max_streams);
        auto added_processors = collector.detachProcessors();
        processors.insert(processors.end(), added_processors.begin(), added_processors.end());
    }

    return pipeline;
}

void UnionStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void UnionStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_streams_for_union_step] = max_streams;
    settings[QueryPlanSerializationSetting::max_streams_for_union_step_to_max_threads_ratio] = static_cast<float>(max_streams_ratio);
}

void UnionStep::serialize(Serialization & ctx) const
{
    (void)ctx;
}

QueryPlanStepPtr UnionStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<UnionStep>(
        ctx.input_headers,
        /* max_threads_ = */ 0,
        /* max_streams_ = */ ctx.settings[QueryPlanSerializationSetting::max_streams_for_union_step],
        /* max_streams_ratio_ = */ ctx.settings[QueryPlanSerializationSetting::max_streams_for_union_step_to_max_threads_ratio]);
}

void registerUnionStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Union", &UnionStep::deserialize);
}

}
