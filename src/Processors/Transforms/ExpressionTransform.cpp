#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <memory>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <Common/logger_useful.h>

namespace DB
{

Block ExpressionTransform::transformHeader(const Block & header, const ActionsDAG & expression)
{
    return expression.updateHeader(header);
}

ExpressionTransform::ExpressionTransform(
    SharedHeader header_, ExpressionActionsPtr expression_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : ISimpleTransform(header_, std::make_shared<const Block>(transformHeader(*header_, expression_->getActionsDAG())), false)
    , expression(std::move(expression_))
    , updater(std::move(updater_))
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    LOG_DEBUG(getLogger("ExpressionTransform"), "transform() enter this={}, expression={}", static_cast<const void*>(this), static_cast<void*>(expression.get()));

    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    if (updater)
        updater->recordOutputChunk(chunk, block);

    LOG_DEBUG(getLogger("ExpressionTransform"), "transform() exit this={}, expression={}", static_cast<const void*>(this), static_cast<void*>(expression.get()));
}

void ExpressionTransform::onCancel() noexcept
{
    LOG_DEBUG(getLogger("ExpressionTransform"), "onCancel() enter this={}, expression={}", static_cast<const void*>(this), static_cast<void*>(expression.get()));
    ISimpleTransform::onCancel();
    expression->cancel();
    LOG_DEBUG(getLogger("ExpressionTransform"), "onCancel() exit this={}, expression={}", static_cast<const void*>(this), static_cast<void*>(expression.get()));
}

ConvertingTransform::ConvertingTransform(SharedHeader header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, std::make_shared<const Block>(ExpressionTransform::transformHeader(*header_, expression_->getActionsDAG())))
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
