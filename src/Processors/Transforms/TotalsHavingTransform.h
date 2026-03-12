#pragma once

#include <Common/PODArray_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/finalizeChunk.h>

namespace DB
{

/// Chunk info attached to every chunk output by TotalsHavingTransform.
/// Used by MarshallBlocksTransform to skip marshalling for these chunks.
/// This unfortunate crutch is needed, because for totals, `IOutputFormat::prepareTotals`
/// may call the `cut` method on a column after the chunk already passed `MarshallBlocksTransform`.
/// Since `ColumnBLOB` doesn't support `cut`, we need to skip marshalling for totals.
class TotalsHavingChunkInfo final : public ChunkInfoCloneable<TotalsHavingChunkInfo>
{
};

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
using IColumnFilter = PaddedPODArray<UInt8>;

class ActionsDAG;

enum class TotalsMode : uint8_t;

/** Takes blocks after grouping, with non-finalized aggregate functions.
  * Calculates total values according to totals_mode.
  * If necessary, evaluates the expression from HAVING and filters rows. Returns the finalized and filtered blocks.
  */
class TotalsHavingTransform : public ISimpleTransform
{
public:
    TotalsHavingTransform(
        const Block & header,
        const ColumnsMask & aggregates_mask_,
        bool overflow_row_,
        const ExpressionActionsPtr & expression_,
        const std::string & filter_column_,
        bool remove_filter_,
        TotalsMode totals_mode_,
        double auto_include_threshold_,
        bool final_);

    String getName() const override { return "TotalsHavingTransform"; }

    OutputPort & getTotalsPort() { return outputs.back(); }

    Status prepare() override;
    void work() override;

    bool hasFilter() const { return !filter_column_name.empty(); }

    static Block transformHeader(Block block, const ActionsDAG * expression, const std::string & filter_column_name, bool remove_filter, bool final, const ColumnsMask & aggregates_mask);

protected:
    void transform(Chunk & chunk) override;

    bool finished_transform = false;
    bool total_prepared = false;
    Chunk totals;

private:
    void addToTotals(const Chunk & chunk, const IColumnFilter * filter);
    void prepareTotals();

    /// Params
    const ColumnsMask aggregates_mask;
    bool overflow_row;
    ExpressionActionsPtr expression;
    String filter_column_name;
    bool remove_filter;
    TotalsMode totals_mode;
    double auto_include_threshold;
    bool final;

    size_t passed_keys = 0;
    size_t total_keys = 0;

    size_t filter_column_pos = 0;

    Block finalized_header;

    /// Here are the values that did not pass max_rows_to_group_by.
    /// They are added or not added to the current_totals, depending on the totals_mode.
    Chunk overflow_aggregates;

    /// Here, total values are accumulated. After the work is finished, they will be placed in totals.
    MutableColumns current_totals;
};

}
