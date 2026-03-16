#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <Core/Block.h>
#include <Functions/IFunction.h>
#include <memory>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>


namespace DB
{

namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

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
    size_t num_rows = chunk.getNumRows();
    Columns columns = chunk.detachColumns();
    auto block = getInputPort().getHeader().cloneWithColumns(columns);

    try
    {
        expression->execute(block, num_rows);
        chunk.setColumns(block.getColumns(), num_rows);
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED)
        {
            chunk.setColumns(columns, num_rows);
        }
        else
        {
            throw;
        }
    }

    if (updater)
        updater->recordOutputChunk(chunk, block);
}

void ExpressionTransform::onCancel() noexcept
{
    ISimpleTransform::onCancel();
    expression->cancel();
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
