#include <Processors/Transforms/VirtualRowTransform.h>

#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static Chunk makeVirtualRowChunk(const Block & header, const Block & pk_block, const ExpressionActionsPtr & virtual_row_conversions)
{
    Columns empty_columns;
    empty_columns.reserve(header.columns());
    for (size_t i = 0; i < header.columns(); ++i)
        empty_columns.push_back(header.getByPosition(i).type->createColumn()->cloneEmpty());

    Chunk chunk;
    chunk.setColumns(std::move(empty_columns), 0);
    chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(0, pk_block, virtual_row_conversions));
    return chunk;
}

VirtualRowTransform::VirtualRowTransform(SharedHeader header_, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , pk_block(pk_block_)
    , virtual_row_conversions(std::move(virtual_row_conversions_))
{
}

VirtualRowTransform::Status VirtualRowTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (generated)
    {
        output.push(std::move(current_chunk));
        generated = false;
        return Status::PortFull;
    }

    if (can_generate)
        return Status::Ready;

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        /// Set input port NotNeeded after chunk was pulled.
        current_chunk = input.pull(true);
        has_input = true;
    }

    /// Now transform.
    return Status::Ready;
}

void VirtualRowTransform::work()
{
    if (can_generate)
    {
        if (generated)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "VirtualRowTransform cannot consume chunk because it already was generated");

        generated = true;
        can_generate = false;

        if (!is_first)
        {
            if (current_chunk.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't generate chunk in VirtualRowTransform");
            return;
        }

        is_first = false;
        current_chunk = makeVirtualRowChunk(getOutputs().front().getHeader(), pk_block, virtual_row_conversions);
    }
    else
    {
        if (!has_input)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "VirtualRowTransform cannot consume chunk because it wasn't read");

        has_input = false;
        can_generate = true;
    }
}

DeferredVirtualRowTransform::DeferredVirtualRowTransform(
    SharedHeader header_,
    const Block & pk_block_,
    ExpressionActionsPtr virtual_row_conversions_,
    ExpressionActionsPtr pk_computation_,
    bool use_last_row_)
    : IProcessor({header_}, {header_})
    , input(inputs.front()), output(outputs.front())
    , pk_block(pk_block_)
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , pk_computation(std::move(pk_computation_))
    , use_last_row(use_last_row_)
{
}

DeferredVirtualRowTransform::Status DeferredVirtualRowTransform::prepare()
{
    /// Check can output.

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (generated)
    {
        output.push(std::move(current_chunk));
        generated = false;

        if (pending_chunk.has_value())
        {
            current_chunk = std::move(*pending_chunk);
            pending_chunk.reset();
            generated = true;
        }

        return Status::PortFull;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        /// Set input port NotNeeded after chunk was pulled.
        current_chunk = input.pull(true);
        has_input = true;
    }

    return Status::Ready;
}

void DeferredVirtualRowTransform::work()
{
    if (!has_input)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DeferredVirtualRowTransform cannot process because no input was read");

    has_input = false;

    if (is_first && current_chunk.getNumRows() > 0)
    {
        is_first = false;

        /// Compute pk_block values from a row of the first input chunk.
        const auto & header = getOutputs().front().getHeader();
        const auto & chunk_cols = current_chunk.getColumns();
        size_t row_idx = use_last_row ? current_chunk.getNumRows() - 1 : 0;

        Block raw_block = header.cloneEmpty();
        auto mut_columns = raw_block.mutateColumns();
        for (size_t i = 0; i < raw_block.columns(); ++i)
            mut_columns[i]->insertFrom(*chunk_cols[i], row_idx);
        raw_block.setColumns(std::move(mut_columns));

        pk_computation->execute(raw_block);

        for (auto & col : pk_block)
            col.column = raw_block.getByName(col.name).column;

        /// Buffer the real chunk and emit the virtual row first.
        pending_chunk = std::move(current_chunk);

        current_chunk = makeVirtualRowChunk(header, pk_block, virtual_row_conversions);
        generated = true;
    }
    else
    {
        /// Passthrough: mark current_chunk as ready to push.
        generated = true;
    }
}

}
