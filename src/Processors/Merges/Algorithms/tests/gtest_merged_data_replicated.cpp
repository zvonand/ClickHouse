#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Merges/Algorithms/MergedData.h>
#include <gtest/gtest.h>

using namespace DB;

/// Regression test for STID 2508: assertion failure when MergedData destination
/// is a regular column but source is ColumnReplicated.
///
/// This happens when some merge inputs are null at initialization time (their
/// ColumnReplicated status is unknown), and later arrive via consume() with
/// ColumnReplicated non-sort columns. The destination was set up as regular
/// during initialize(), but the source from a late-arriving input has
/// ColumnReplicated, causing a type mismatch in insertFrom/insertRangeFrom.
///
/// The fix materializes ColumnReplicated sources in MergedData::insertRow/insertRows
/// when the destination is regular, as a defense-in-depth measure. The primary fix
/// is in the merge algorithms' consume() methods which now materialize ALL
/// ColumnReplicated columns (not just sort columns).

TEST(MergedDataReplicated, InsertRowsReplicatedSourceRegularDestination)
{
    /// Set up a header with 2 columns: "key" (would be sort) and "value" (non-sort)
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with input 0 having regular columns; input 1 is null (late-arriving).
    /// This means MergedData destination columns are regular (not ColumnReplicated).
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }
    /// inputs[1] has no chunk — simulates a merge input that arrives later via consume().

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Simulate insertRows with a ColumnReplicated value column.
    /// In the real bug scenario, this comes from a JOIN with enable_lazy_columns_replication=1
    /// where consume() didn't materialize non-sort ColumnReplicated columns.
    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};

    /// Before the fix, this would trigger:
    ///   chassert((isConst() || isSparse() || isReplicated()) ? getDataType() == rhs.getDataType()
    ///            : typeid(*this) == typeid(rhs))
    /// at IColumn.h:862 because destination is regular ColumnInt64 but source is ColumnReplicated.
    ASSERT_NO_THROW(merged_data.insertRows(raw_columns, 0, 1, 1));

    /// Verify the data was inserted correctly.
    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    const auto & result_key = assert_cast<const ColumnInt64 &>(*result.getColumns()[0]);
    ASSERT_EQ(result_key.getInt(0), 2);
}

TEST(MergedDataReplicated, InsertRowReplicatedSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with only regular columns.
    IMergingAlgorithm::Inputs inputs(1);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Insert a single row with ColumnReplicated source.
    auto key_src = ColumnInt64::create();
    key_src->insertValue(2);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(200);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    ColumnRawPtrs raw_columns = {key_src.get(), val_replicated.get()};
    ASSERT_NO_THROW(merged_data.insertRow(raw_columns, 0, 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    const auto & result_val = *result.getColumns()[1];
    ASSERT_EQ(result_val.getInt(0), 200);
}

TEST(MergedDataReplicated, InsertChunkReplicatedSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "value"));

    /// Initialize with only regular columns — no ColumnReplicated seen.
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        auto val_col = ColumnInt64::create();
        key_col->insertValue(1);
        val_col->insertValue(100);
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, {});
    merged_data.initialize(header, inputs);

    /// Pull the initial empty merged data so we can test insertChunk on a fresh state.
    /// (insertChunk requires merged_rows == 0)

    /// Construct a chunk with ColumnReplicated value column.
    auto key_col = ColumnInt64::create();
    key_col->insertValue(3);
    auto val_nested = ColumnInt64::create();
    val_nested->insertValue(300);
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_nested)));

    Chunk chunk(Columns{std::move(key_col), std::move(val_replicated)}, 1);
    ASSERT_NO_THROW(merged_data.insertChunk(std::move(chunk), 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
    const auto & result_val = *result.getColumns()[1];
    ASSERT_EQ(result_val.getInt(0), 300);
}

/// Regression test for the AI review finding on PR #101036: insertChunk's hasDynamicStructure
/// branch fires before the ColumnReplicated materialization guard. When a chunk column is
/// ColumnReplicated(ColumnDynamic), the hasDynamicStructure branch calls insertRangeFrom with
/// a ColumnReplicated source, but ColumnDynamic::insertRangeFrom does assert_cast<const ColumnDynamic &>
/// which crashes in debug/sanitizer builds and is UB in release.
/// The fix materializes ColumnReplicated chunk columns up front before any type-specific branches.
TEST(MergedDataReplicated, InsertChunkReplicatedDynamicSourceRegularDestination)
{
    Block header;
    header.insert(ColumnWithTypeAndName(ColumnInt64::create(), std::make_shared<DataTypeInt64>(), "key"));
    header.insert(ColumnWithTypeAndName(ColumnDynamic::create(254), std::make_shared<DataTypeDynamic>(), "value"));

    /// Initialize with one regular input (no ColumnReplicated) — destination is ColumnDynamic.
    /// Input 1 is null (late-arriving), so MergedData won't see ColumnReplicated during init.
    IMergingAlgorithm::Inputs inputs(2);
    {
        auto key_col = ColumnInt64::create();
        key_col->insertValue(1);
        auto val_col = ColumnDynamic::create(254);
        val_col->insert(Field(100));
        inputs[0].chunk.setColumns(Columns{std::move(key_col), std::move(val_col)}, 1);
    }

    MergedData merged_data(false, 1000, 0, 254);
    merged_data.initialize(header, inputs);

    /// Construct a chunk where the Dynamic column is wrapped in ColumnReplicated.
    /// This simulates a late-arriving merge input from a JOIN with enable_lazy_columns_replication=1.
    auto key_col = ColumnInt64::create();
    key_col->insertValue(3);
    auto val_dynamic = ColumnDynamic::create(254);
    val_dynamic->insert(Field(300));
    ColumnPtr val_replicated = ColumnReplicated::create(ColumnPtr(std::move(val_dynamic)));

    Chunk chunk(Columns{std::move(key_col), std::move(val_replicated)}, 1);

    /// Before the fix, this would crash: columns[i]->hasDynamicStructure() is true on destination,
    /// so ColumnDynamic::insertRangeFrom receives a ColumnReplicated source,
    /// assert_cast<const ColumnDynamic &>(src) fails.
    ASSERT_NO_THROW(merged_data.insertChunk(std::move(chunk), 1));

    Chunk result = merged_data.pull();
    ASSERT_EQ(result.getNumRows(), 1);
}
