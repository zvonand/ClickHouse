#pragma once

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>

namespace DB
{

/// Virtual row is useful for read-in-order optimization when multiple parts exist.
/// Emits a virtual row (containing pk_block values) before passing through input data.
class VirtualRowTransform : public IProcessor
{
public:
    explicit VirtualRowTransform(SharedHeader header_, const Block & pk_block_, ExpressionActionsPtr virtual_row_conversions_);

    String getName() const override { return "VirtualRowTransform"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    bool has_input = false;
    bool generated = false;
    bool can_generate = true;
    bool is_first = true;

    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
};

/// Reads the first input chunk, computes pk_block from its first row
/// using the primary key expression, emits the virtual row, then passes through all data.
class DeferredVirtualRowTransform : public IProcessor
{
public:
    explicit DeferredVirtualRowTransform(
        SharedHeader header_,
        const Block & pk_block_,
        ExpressionActionsPtr virtual_row_conversions_,
        ExpressionActionsPtr pk_computation_,
        bool use_last_row_);

    String getName() const override { return "DeferredVirtualRowTransform"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    Chunk current_chunk;
    bool has_input = false;
    bool generated = false;
    bool is_first = true;

    Block pk_block;
    ExpressionActionsPtr virtual_row_conversions;
    ExpressionActionsPtr pk_computation;
    bool use_last_row;

    /// Buffer the first input chunk while the virtual row is being emitted.
    std::optional<Chunk> pending_chunk;
};

}
