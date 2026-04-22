#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/convertFieldToType.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsMinMax.h>
#include <Storages/Statistics/StatisticsNullCount.h>
#include <Storages/StatisticsDescription.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Statistics/StatisticsTDigest.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Core/Range.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionListParsers.h>

using namespace DB;

TEST(Statistics, TDigestLessThan)
{
    /// this is the simplest data which is continuous integeters.
    /// so the estimated errors should be low.

    std::vector<Int64> data;
    data.reserve(100000);
    for (int i = 0; i < 100000; i++)
        data.push_back(i);

    auto test_less_than = [](const std::vector<Int64> & data1,
                             const std::vector<double> & v,
                             const std::vector<double> & answers,
                             const std::vector<double> & eps)
    {

        DB::QuantileTDigest<Int64> t_digest;

        for (Int64 i : data1)
            t_digest.add(i);

        t_digest.compress();

        for (int i = 0; i < v.size(); i ++)
        {
            auto value = v[i];
            auto result = t_digest.getCountLessThan(value);
            auto answer = answers[i];
            auto error = eps[i];
            ASSERT_LE(result, answer * (1 + error));
            ASSERT_GE(result, answer * (1 - error));
        }
    };
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});

    std::reverse(data.begin(), data.end());
    test_less_than(data, {-1, 1e9, 50000.0, 3000.0, 30.0}, {0, 100000, 50000, 3000, 30}, {0, 0, 0.001, 0.001, 0.001});
}

TEST(Statistics, Estimator)
{
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();
    /// column a, distribution 1,2...,10000
    /// column b, distribution 500,600,500,600...
    /// column c, distribution -10000, -1000, -100, -10, -1, 1, 10, 100, 1008, 1009, 1010, ...
    MutableColumnPtr a = DataTypeInt32().createColumn();
    MutableColumnPtr b = DataTypeInt32().createColumn();
    MutableColumnPtr c = DataTypeInt32().createColumn();
    Int32 c_value[] = {-100000, -1000, -100, -10, -1, 1, 10, 100};
    for (Int32 i = 0; i < 10000; i++)
    {
        a->insert(i+1);
        b->insert(i % 2 == 0 ? 500 : 600);
        c->insert(i < 8 ? c_value[i]: 1000+i);
    }

    auto mock_statistics = [&](const String & column_name)
    {
        ColumnStatisticsDescription mock_description;
        mock_description.data_type = data_type;
        std::vector<StatisticsType> stats_type_to_create({StatisticsType::TDigest, /*StatisticsType::Uniq,*/ StatisticsType::CountMinSketch});
        for (auto stats_type : stats_type_to_create)
        {
            mock_description.types_to_desc.emplace(stats_type, SingleStatisticsDescription(stats_type, nullptr, false));
        }
        ColumnDescription column_desc;
        column_desc.name = column_name;
        column_desc.type = data_type;
        column_desc.statistics = mock_description;
        return MergeTreeStatisticsFactory::instance().get(column_desc);
    };
    ColumnStatisticsPtr stats_a = mock_statistics("a");
    stats_a->build(std::move(a));
    ColumnStatisticsPtr stats_b = mock_statistics("b");
    stats_b->build(std::move(b));
    ColumnStatisticsPtr stats_c = mock_statistics("c");
    stats_c->build(std::move(c));

    ConditionSelectivityEstimatorBuilder estimator_builder(getContext().context);
    estimator_builder.addStatistics("a", stats_a);
    estimator_builder.addStatistics("b", stats_b);
    estimator_builder.addStatistics("c", stats_c);
    estimator_builder.incrementRowCount(10000);

    auto estimator = estimator_builder.getEstimator();

    auto test_impl = [&](const String & expression, Int64 real_result, Float64 eps)
    {
        ParserExpressionWithOptionalAlias exp_parser(false);
        ContextPtr context = getContext().context;
        RPNBuilderTreeContext tree_context(context, Block{{ DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }}, {});
        ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
        RPNBuilderTreeNode node(ast.get(), tree_context);
        auto estimate_result = estimator->estimateRelationProfile(nullptr, node);
        std::cout << expression << " " << real_result << " "<< estimate_result.rows << std::endl;
        EXPECT_LT(std::abs(real_result - static_cast<Int64>(estimate_result.rows)), 10000 * eps);
    };

    auto test_f = [&](const String & expression, Int64 real_result, Float64 eps = 0.001)
    {
        test_impl(expression, real_result, eps);
        /// Let's test 'not expression'
        test_impl("not(" + expression + ")", 10000-real_result, eps);
    };
    ///
    test_f("a in (1,2,3,4,5)", 5);
    test_f("a not in (1,2,3,4,5)", 10000-5);
    test_f("b in (2, 500, 500)", 5000);
    test_f("a < 3 and b = 500", 1);
    test_f("a < 3 and b = 500 and a < b", 1); /// unknown condition 'a < b' assumes 100% selectivity
    test_f("a < 3 or b = 600", 5001);
    test_f("not (a < 3 and b = 500)", 10000-1);
    test_f("c between -1000 and -10", 3);
    test_f("b != 500 and b != 600", 0);
    test_f("not (b != 500 and b != 600)", 10000);
    test_f("b != 500 or b != 600", 10000);
    test_f("not (b != 500 or b != 600)", 0);
    test_f("a < 3 and b != 600", 1);
    test_f("a > 3 and b != 600", 4998);
    test_f("(a > 3 or a < 10) and b != 600", 5000);
    test_f("(a > 3 and a < 10) and b != 600", 3);
    test_f("(a > 3 and a < 10) or (b != 600 and b != 500)", 6);
    test_f("(a > 3 and a < 10) or not (b != 600 and b != 500)", 10000);
    test_f("((a > 3 and a < 10) or (a > 900 and a < 1000) or (a > 9050 and a < 9060))", 114);
    test_f("(a > 3 and a < 1000) or (a > 3 and a < 1011) or (a > 3 and a < 2012)", 2008);
    test_f("(a > 3 and a < 1000) or (a > 3 and a < 1011) or (b = 500)", 5503);
    test_f("(a > 3 and a < 1000) or ((a > 3 and a < 1011) and (b = 500))", 1001, 0.05); /// 5% error
    test_f("((a > 3 and a < 1000) or (a > 3 and a < 1011)) and (b = 500)", 503);
    test_f("a = 5 and a != 6", 1);
}

TEST(Statistics, MinMaxEstimateLess)
{
    auto test_minmax = [](Field min_val, Field max_val, UInt64 row_count, Field val, Float64 expected)
    {
        StatisticsMinMax stats(min_val, max_val, row_count);
        auto result = stats.estimateLess(val);
        ASSERT_TRUE(result.has_value()) << "estimateLess returned nullopt";
        EXPECT_DOUBLE_EQ(*result, expected);
    };

    /// UInt64: interpolation over [0, 9] with 10 rows
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(0),  0.0);           /// at min    → (0/9)*10 = 0
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(9),  10.0);          /// at max    → (9/9)*10 = 10
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(10), 10.0);          /// above max → all rows
    test_minmax(UInt64(0), UInt64(9), 10, UInt64(5),  5.0/9.0*10.0); /// midpoint

    /// Int64: negative range [-100, 100] with 201 rows
    test_minmax(Int64(-100), Int64(100), 201, Int64(-200), 0.0);               /// below min
    test_minmax(Int64(-100), Int64(100), 201, Int64(200),  201.0);             /// above max
    test_minmax(Int64(-100), Int64(100), 201, Int64(0),    100.0/200.0*201.0); /// midpoint

    /// All rows have the same value: min == max
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(42), 50.0); /// v == min == max → all rows
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(43), 50.0); /// v > max         → all rows
    test_minmax(UInt64(42), UInt64(42), 50, UInt64(41), 0.0);  /// v < min         → 0 rows

    /// Precision: UInt64 values near 2^53 where Float64 loses consecutive integers.
    /// Float64(2^53 + 1) rounds to Float64(2^53), so naive conversion gives numerator = 0.
    /// interpolateLinear must use UInt128 internally to recover the correct result.
    const UInt64 base = (1ULL << 53); /// = 9007199254740992
    test_minmax(UInt64(base), UInt64(base + 2), 3, UInt64(base + 1), 1.5); /// (1/2)*3 = 1.5

    /// estimateLess returns nullopt when row_count = 0
    StatisticsMinMax empty(Field{}, Field{}, 0);
    EXPECT_FALSE(empty.estimateLess(Field(UInt64(42))).has_value());
}

namespace
{
/// Helper to create ColumnStatistics with specified types for merge testing
ColumnStatisticsPtr createTestStats(
    const std::vector<StatisticsType> & types,
    const DataTypePtr & data_type = std::make_shared<DataTypeInt32>())
{
    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    for (auto type : types)
        desc.types_to_desc.emplace(type, SingleStatisticsDescription(type, nullptr, false));

    return MergeTreeStatisticsFactory::instance().get(desc);
}
}

TEST(Statistics, EstimateGreaterUsesNonNullRows)
{
    /// Test that estimateGreater uses getNonNullRowCount() instead of rows.
    /// With NullCount stats, estimateGreater(val) = getNonNullRowCount() - estimateLess(val).
    /// For a column with 100 rows (50 NULL + 50 values 1..50), estimateGreater(0) ≈ 50.
    auto nullable_data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    MutableColumnPtr col = nullable_data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());

    for (Int32 i = 0; i < 50; ++i)
    {
        nullable_col->insertDefault(); /// NULL
        nullable_col->insert(i + 1);
    }

    auto stats = createTestStats({StatisticsType::NullCount, StatisticsType::MinMax}, nullable_data_type);
    stats->build(std::move(col));

    ASSERT_EQ(stats->getNumRows(), 100u);
    ASSERT_EQ(stats->getNonNullRowCount(), 50u);

    /// estimateGreater(0) should be approximately 50 (all non-NULL values are > 0)
    /// because estimateGreater = getNonNullRowCount() - estimateLess(0) ≈ 50 - 0 = 50
    auto estimate = stats->estimateGreater(Field(Int32(0)));
    ASSERT_TRUE(estimate.has_value());
    EXPECT_NEAR(*estimate, 50.0, 1.0);

    /// estimateGreater(50) should be approximately 0 (no non-NULL values > 50)
    auto estimate2 = stats->estimateGreater(Field(Int32(50)));
    ASSERT_TRUE(estimate2.has_value());
    EXPECT_NEAR(*estimate2, 0.0, 1.0);

    /// estimateGreater(25) should be approximately 25 (values 26..50)
    auto estimate3 = stats->estimateGreater(Field(Int32(25)));
    ASSERT_TRUE(estimate3.has_value());
    EXPECT_NEAR(*estimate3, 25.0, 2.0);
}

TEST(Statistics, EstimateRangeInfiniteWithoutNullUsesNonNullRows)
{
    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());

    for (Int32 i = 0; i < 100; ++i)
    {
        if (i % 10 == 0)
            nullable_col->insertDefault();
        else
            nullable_col->insert(i);
    }

    auto stats = createTestStats({StatisticsType::NullCount}, data_type);
    stats->build(std::move(col));

    ASSERT_EQ(stats->getNumRows(), 100u);
    ASSERT_EQ(stats->getNonNullRowCount(), 90u);

    auto estimate = stats->estimateRange(Range::createWholeUniverseWithoutNull());
    ASSERT_TRUE(estimate.has_value());
    EXPECT_DOUBLE_EQ(*estimate, 90.0);
}

TEST(Statistics, NullPredicateContradictionAndTautology)
{
    /// Test that x IS NULL AND x IS NOT NULL estimates to 0 (contradiction)
    /// and x IS NULL OR x IS NOT NULL estimates to 1 (tautology).

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Column x: 50% NULL, 50% non-NULL values 0..49
    MutableColumnPtr x = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(x.get());
    for (Int32 i = 0; i < 100; ++i)
    {
        if (i % 2 == 0)
            nullable_col->insertDefault(); /// NULL
        else
            nullable_col->insert(i);
    }

    ColumnStatisticsDescription desc;
    desc.data_type = data_type;
    desc.types_to_desc.emplace(StatisticsType::NullCount, SingleStatisticsDescription(StatisticsType::NullCount, nullptr, false));
    ColumnDescription column_desc;
    column_desc.name = "x";
    column_desc.type = data_type;
    column_desc.statistics = desc;

    ColumnStatisticsPtr stats_x = MergeTreeStatisticsFactory::instance().get(column_desc);
    stats_x->build(std::move(x));

    ConditionSelectivityEstimatorBuilder estimator_builder(getContext().context);
    estimator_builder.addStatistics("x", stats_x);
    estimator_builder.incrementRowCount(100);

    auto estimator = estimator_builder.getEstimator();

    auto test_impl = [&](const String & expression, Float64 expected_selectivity, Float64 eps)
    {
        ParserExpressionWithOptionalAlias exp_parser(false);
        ContextPtr context = getContext().context;
        RPNBuilderTreeContext tree_context(context, Block{{ DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy" }}, {});
        ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
        RPNBuilderTreeNode node(ast.get(), tree_context);
        auto estimate_result = estimator->estimateRelationProfile(nullptr, node);
        Float64 actual_selectivity = static_cast<Float64>(estimate_result.rows) / 100.0;
        EXPECT_NEAR(actual_selectivity, expected_selectivity, eps)
            << "Expression: " << expression << " expected_selectivity=" << expected_selectivity
            << " actual_selectivity=" << actual_selectivity << " rows=" << estimate_result.rows;
    };

    /// x IS NULL AND x IS NOT NULL → contradiction, selectivity must be 0
    test_impl("x IS NULL AND x IS NOT NULL", 0.0, 1e-9);

    /// x IS NOT NULL AND x IS NULL → same contradiction (different order)
    test_impl("x IS NOT NULL AND x IS NULL", 0.0, 1e-9);

    /// x IS NULL OR x IS NOT NULL → tautology, selectivity must be 1
    test_impl("x IS NULL OR x IS NOT NULL", 1.0, 1e-9);

    /// x IS NOT NULL OR x IS NULL → same tautology (different order)
    test_impl("x IS NOT NULL OR x IS NULL", 1.0, 1e-9);

    /// Equivalent forms via NOT:
    /// NOT (x IS NULL) is x IS NOT NULL, so NOT (x IS NULL) AND x IS NULL → contradiction
    test_impl("NOT x IS NULL AND x IS NULL", 0.0, 1e-9);

    /// NOT (x IS NOT NULL) is x IS NULL, so NOT (x IS NOT NULL) OR x IS NOT NULL → tautology
    test_impl("NOT (x IS NOT NULL) OR x IS NOT NULL", 1.0, 1e-9);
}


TEST(Statistics, SerializeDeserializeRoundTrip)
{
    /// Test that ColumnStatistics survives a serialize-deserialize round-trip
    /// with V3 format (per-type size prefix).

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 100; ++i)
    {
        if (i % 10 == 0)
            nullable_col->insertDefault(); /// NULL
        else
            nullable_col->insert(i);
    }

    auto stats = createTestStats({StatisticsType::MinMax, StatisticsType::NullCount, StatisticsType::TDigest}, data_type);
    stats->build(std::move(col));

    UInt64 original_rows = stats->getNumRows();
    auto original_null_count = stats->getNullCount();

    /// Serialize
    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Deserialize
    ReadBufferFromString read_buf(serialized);
    auto deserialized = ColumnStatistics::deserialize(read_buf, data_type);

    /// Verify row count
    EXPECT_EQ(deserialized->getNumRows(), original_rows);

    /// Verify NullCount
    EXPECT_EQ(deserialized->getNullCount(), original_null_count);

    /// Verify all stat types present
    const auto & deserialized_stats = deserialized->getStats();
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::MinMax));
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::NullCount));
    EXPECT_TRUE(deserialized_stats.contains(StatisticsType::TDigest));

    /// Verify buffer fully consumed
    EXPECT_TRUE(read_buf.eof());
}

TEST(Statistics, DeserializeV3SkipsTrailingBytes)
{
    /// Test that V3 deserialization correctly skips trailing bytes when a known
    /// stat type's deserialize consumes fewer bytes than stat_size.
    /// This simulates forward compatibility: a newer version may write extra
    /// bytes after a known stat, and an older version should skip them.

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Build a stats object and serialize it
    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 50; ++i)
    {
        if (i % 5 == 0)
            nullable_col->insertDefault();
        else
            nullable_col->insert(i);
    }

    auto stats = createTestStats({StatisticsType::NullCount}, data_type);
    stats->build(std::move(col));

    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Now manually inject trailing bytes after the NullCount stat payload.
    /// V3 format: version(2) + mask(8) + rows(8) + [stat_size(8) + stat_data(stat_size)]*
    /// We need to find where the NullCount stat_data ends and insert extra bytes,
    /// adjusting stat_size accordingly.

    /// Parse the serialized V3 format to locate stat_size and stat_data boundaries.
    /// Layout:
    ///   offset 0: UInt16 version
    ///   offset 2: UInt64 stat_types_mask
    ///   offset 10: UInt64 rows
    ///   offset 18: UInt64 stat_size (for the first stat)
    ///   offset 26: stat_data (stat_size bytes)
    ///   ...next stat or end

    ReadBufferFromString orig_buf(serialized);

    UInt16 version_raw;
    readIntBinary(version_raw, orig_buf);
    ASSERT_EQ(version_raw, static_cast<UInt16>(StatisticsFileVersion::V3));

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, orig_buf);

    UInt64 rows_value = 0;
    readIntBinary(rows_value, orig_buf);

    /// Read the stat_size for the first (and only) stat
    UInt64 stat_size = 0;
    readIntBinary(stat_size, orig_buf);

    /// Record the start of stat_data
    const char * stat_data_start = orig_buf.position();
    String stat_data(stat_data_start, stat_size);

    /// Now build a new serialized buffer with extra trailing bytes
    UInt64 new_stat_size = stat_size + 7; /// 7 extra trailing bytes
    String modified;
    WriteBufferFromString mod_buf(modified);
    writeIntBinary(version_raw, mod_buf);
    writeIntBinary(stat_types_mask, mod_buf);
    writeIntBinary(rows_value, mod_buf);
    writeIntBinary(new_stat_size, mod_buf);
    mod_buf.write(stat_data.data(), stat_size);
    /// Write 7 trailing bytes (simulating forward-compatible extra data)
    const char trailing[7] = {static_cast<char>(0xAA), static_cast<char>(0xBB), static_cast<char>(0xCC), static_cast<char>(0xDD), static_cast<char>(0xEE), static_cast<char>(0xFF), 0x00};
    mod_buf.write(trailing, 7);
    mod_buf.finalize();

    /// Deserialize from modified buffer — should successfully skip the 7 trailing bytes
    ReadBufferFromString mod_read_buf(modified);
    auto deserialized = ColumnStatistics::deserialize(mod_read_buf, data_type);

    EXPECT_EQ(deserialized->getNumRows(), rows_value);
    EXPECT_TRUE(deserialized->getStats().contains(StatisticsType::NullCount));
    EXPECT_EQ(deserialized->getNullCount(), stats->getNullCount());

    /// Buffer should be fully consumed (no leftover)
    EXPECT_TRUE(mod_read_buf.eof());
}

TEST(Statistics, DeserializeV3ThrowsOnOversizedStat)
{
    /// Test that V3 deserialization throws when a known stat type consumes
    /// more bytes than stat_size indicates (corrupted data).

    auto data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());

    /// Build and serialize a stats object with MinMax (which has a non-trivial payload)
    MutableColumnPtr col = data_type->createColumn();
    auto * nullable_col = assert_cast<ColumnNullable *>(col.get());
    for (Int32 i = 0; i < 50; ++i)
        nullable_col->insert(i);

    auto stats = createTestStats({StatisticsType::MinMax}, data_type);
    stats->build(std::move(col));

    String serialized;
    WriteBufferFromString write_buf(serialized);
    stats->serialize(write_buf);
    write_buf.finalize();

    /// Parse the V3 header to find stat_size and stat_data
    ReadBufferFromString orig_buf(serialized);

    UInt16 version_raw;
    readIntBinary(version_raw, orig_buf);

    UInt64 stat_types_mask = 0;
    readIntBinary(stat_types_mask, orig_buf);

    UInt64 rows_value = 0;
    readIntBinary(rows_value, orig_buf);

    UInt64 stat_size = 0;
    readIntBinary(stat_size, orig_buf);

    String stat_data(orig_buf.position(), stat_size);

    /// Build a modified buffer with stat_size smaller than actual payload
    UInt64 shrunk_stat_size = stat_size > 2 ? stat_size - 2 : 1;
    String modified;
    WriteBufferFromString mod_buf(modified);
    writeIntBinary(version_raw, mod_buf);
    writeIntBinary(stat_types_mask, mod_buf);
    writeIntBinary(rows_value, mod_buf);
    writeIntBinary(shrunk_stat_size, mod_buf);
    /// Write the full stat_data (more than shrunk_stat_size)
    mod_buf.write(stat_data.data(), stat_size);
    mod_buf.finalize();

    /// Deserialization should throw ILLEGAL_STATISTICS because consumed > stat_size
    ReadBufferFromString mod_read_buf(modified);
    EXPECT_THROW(ColumnStatistics::deserialize(mod_read_buf, data_type), DB::Exception);
}

TEST(Statistics, LikeSelectivity)
{
    /// Build a simple estimator to test LIKE / NOT LIKE / ILIKE / NOT ILIKE
    /// selectivity defaults and their complement behavior under NOT.
    DataTypePtr data_type = std::make_shared<DataTypeInt32>();

    MutableColumnPtr col = DataTypeInt32().createColumn();
    for (Int32 i = 0; i < 10000; i++)
        col->insert(i + 1);

    ColumnStatisticsDescription mock_description;
    mock_description.data_type = data_type;
    mock_description.types_to_desc.emplace(StatisticsType::TDigest, SingleStatisticsDescription(StatisticsType::TDigest, nullptr, false));

    ColumnDescription column_desc;
    column_desc.name = "a";
    column_desc.type = data_type;
    column_desc.statistics = mock_description;
    auto stats = MergeTreeStatisticsFactory::instance().get(column_desc);
    stats->build(std::move(col));

    ConditionSelectivityEstimatorBuilder estimator_builder(getContext().context);
    estimator_builder.addStatistics("a", stats);
    estimator_builder.incrementRowCount(10000);
    auto estimator = estimator_builder.getEstimator();

    /// Helper: estimate rows for a condition string.
    auto estimate = [&](const String & expression) -> UInt64
    {
        ParserExpressionWithOptionalAlias exp_parser(false);
        ContextPtr context = getContext().context;
        RPNBuilderTreeContext tree_context(context, Block{{DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}}, {});
        ASTPtr ast = parseQuery(exp_parser, expression, 10000, 10000, 10000);
        RPNBuilderTreeNode node(ast.get(), tree_context);
        return estimator->estimateRelationProfile(nullptr, node).rows;
    };

    /// default_like_factor = 0.1, total_rows = 10000.
    /// LIKE: 0.1 * 10000 = 1000 rows.
    UInt64 like_rows = estimate("a like '%pattern%'");
    EXPECT_EQ(like_rows, 1000u);

    /// NOT LIKE: (1 - 0.1) * 10000 = 9000 rows.
    UInt64 not_like_rows = estimate("not(a like '%pattern%')");
    EXPECT_EQ(not_like_rows, 9000u);

    /// Complement: LIKE + NOT LIKE = total rows.
    EXPECT_EQ(like_rows + not_like_rows, 10000u);

    /// ILIKE: same as LIKE.
    UInt64 ilike_rows = estimate("a ilike '%pattern%'");
    EXPECT_EQ(ilike_rows, 1000u);

    /// NOT ILIKE: same as NOT LIKE.
    UInt64 not_ilike_rows = estimate("not(a ilike '%pattern%')");
    EXPECT_EQ(not_ilike_rows, 9000u);

    /// notLike function directly: 0.9 * 10000 = 9000 rows.
    UInt64 notlike_direct_rows = estimate("a not like '%pattern%'");
    EXPECT_EQ(notlike_direct_rows, 9000u);

    /// notILike function directly: 0.9 * 10000 = 9000 rows.
    UInt64 notilike_direct_rows = estimate("a not ilike '%pattern%'");
    EXPECT_EQ(notilike_direct_rows, 9000u);
}
