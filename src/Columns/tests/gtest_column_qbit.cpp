#include <Columns/ColumnQBit.h>
#include <Columns/ColumnString.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeQBit.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// Build an empty `ColumnQBit` through its `DataTypeQBit` so the internal tuple
/// matches what the rest of the engine produces (see `DataTypeQBit::createColumn`).
MutableColumnPtr createEmptyQBitColumn(const String & element_type, size_t dimension)
{
    auto qbit_type = std::make_shared<DataTypeQBit>(DataTypeFactory::instance().get(element_type), dimension);
    return qbit_type->createColumn();
}

}

/// `ColumnQBit::structureEquals` must agree with itself for two independently-constructed
/// columns that share element type and dimension. This was the path broken before
/// PR #103084: the old implementation compared the internal `ColumnTuple` against the
/// outer `ColumnQBit` via `typeid_cast<const ColumnTuple *>`, which always returned
/// false, so `writeSlice(GenericArraySlice, GenericArraySink)` raised a logical error
/// whenever `if`/`ifNull` ran over tuples or maps containing a QBit element.
TEST(ColumnQBit, StructureEqualsSameDimensionIsTrue)
{
    auto a = createEmptyQBitColumn("Float64", 4);
    auto b = createEmptyQBitColumn("Float64", 4);
    EXPECT_TRUE(a->structureEquals(*b));
    EXPECT_TRUE(b->structureEquals(*a));
}

/// Two `ColumnQBit` columns with different `dimension` values must be reported as
/// structurally different. The 1-vs-8 pair is the most interesting boundary because
/// both dimensions pad to a single byte in the underlying `FixedString` storage:
/// without the explicit `dimension ==` check in `structureEquals`, the inner
/// `ColumnTuple::structureEquals` would happily claim the two columns match, which
/// would let `writeSlice` corrupt data or mis-dispatch at runtime.
TEST(ColumnQBit, StructureEqualsDifferentDimensionIsFalse)
{
    auto d1 = createEmptyQBitColumn("Float64", 1);
    auto d8 = createEmptyQBitColumn("Float64", 8);
    EXPECT_FALSE(d1->structureEquals(*d8));
    EXPECT_FALSE(d8->structureEquals(*d1));

    auto d4 = createEmptyQBitColumn("Float64", 4);
    EXPECT_FALSE(d4->structureEquals(*d8));
    EXPECT_FALSE(d8->structureEquals(*d4));
}

/// Different element types (Float32 vs Float64) yield different tuple widths, so
/// the nested `ColumnTuple::structureEquals` fails even when dimensions match.
TEST(ColumnQBit, StructureEqualsDifferentElementTypeIsFalse)
{
    auto float32 = createEmptyQBitColumn("Float32", 8);
    auto float64 = createEmptyQBitColumn("Float64", 8);
    EXPECT_FALSE(float32->structureEquals(*float64));
    EXPECT_FALSE(float64->structureEquals(*float32));
}

/// `structureEquals` must reject any non-QBit column via the `typeid_cast` guard.
/// This is the other half of the fix: the old implementation delegated to the
/// inner tuple's `structureEquals`, which could accidentally match unrelated
/// column shapes.
TEST(ColumnQBit, StructureEqualsNonQBitRhsIsFalse)
{
    auto qbit = createEmptyQBitColumn("Float64", 4);
    auto other = ColumnString::create();
    EXPECT_FALSE(qbit->structureEquals(*other));
}
