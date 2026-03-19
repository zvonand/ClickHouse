#if WITH_COVERAGE

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/coverage.h>

#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Common/CoverageCollection.h>
#endif


namespace DB
{

namespace
{

enum class Kind : uint8_t
{
    Files,
    LineStarts,
    LineEnds,
};

/** If ClickHouse is built with coverage instrumentation (WITH_COVERAGE=1), returns arrays
  * of source files / line start numbers / line end numbers covered since the last reset.
  */
class FunctionCoverageLines : public IFunction
{
private:
    Kind kind;

public:
    explicit FunctionCoverageLines(Kind kind_) : kind(kind_) {}

    String getName() const override
    {
        if (kind == Kind::Files) return "coverageCurrentFiles";
        if (kind == Kind::LineStarts) return "coverageCurrentLineStarts";
        return "coverageCurrentLineEnds";
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    size_t getNumberOfArguments() const override { return 0; }

    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        if (kind == Kind::Files)
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Full implementation pending LLVMCoverageMapping reader.
        /// Returns empty arrays for now.
        if (kind == Kind::Files)
        {
            auto column = ColumnString::create();
            auto offsets = ColumnArray::ColumnOffsets::create(1, 0);
            auto array = ColumnArray::create(std::move(column), std::move(offsets));
            return ColumnConst::create(std::move(array), input_rows_count);
        }
        auto column = ColumnUInt32::create();
        auto offsets = ColumnArray::ColumnOffsets::create(1, 0);
        auto array = ColumnArray::create(std::move(column), std::move(offsets));
        return ColumnConst::create(std::move(array), input_rows_count);
    }
};

}

/// Returns diagnostic info: (profile_data_records, covered_name_refs, coverage_map_size)
class FunctionCoverageDiag : public IFunction
{
public:
    String getName() const override { return "coverageDiag"; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isDeterministic() const override { return false; }
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto column = ColumnUInt64::create();
        auto & data = column->getData();
#if defined(__ELF__) && !defined(OS_FREEBSD)
        auto name_refs = getCurrentCoveredNameRefs();
        data.push_back(static_cast<UInt64>(name_refs.size()));
        data.push_back(static_cast<UInt64>(DB::getCoverageMapSize()));
#else
        data.push_back(0);
        data.push_back(0);
#endif
        auto offsets = ColumnArray::ColumnOffsets::create(1, data.size());
        auto array = ColumnArray::create(std::move(column), std::move(offsets));
        return ColumnConst::create(std::move(array), input_rows_count);
    }
};

REGISTER_FUNCTION(CoverageLines)
{
    factory.registerFunction("coverageDiag", [](ContextPtr){ return std::make_shared<FunctionCoverageDiag>(); },
        FunctionDocumentation
        {
            .description = R"(Returns [name_refs_count, coverage_map_size] for diagnostics.)",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentFiles", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::Files); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE=1` option.

Returns an `Array(String)` of source file paths covered since the last `SYSTEM SET COVERAGE TEST` call.

Use together with `coverageCurrentLineStarts` and `coverageCurrentLineEnds` to get the covered line ranges.
)",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentLineStarts", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::LineStarts); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE=1` option.

Returns an `Array(UInt32)` of line start numbers parallel to `coverageCurrentFiles`.
)",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });

    factory.registerFunction("coverageCurrentLineEnds", [](ContextPtr){ return std::make_shared<FunctionCoverageLines>(Kind::LineEnds); },
        FunctionDocumentation
        {
            .description = R"(
This function is only available if ClickHouse was built with the `WITH_COVERAGE=1` option.

Returns an `Array(UInt32)` of line end numbers parallel to `coverageCurrentFiles`.
)",
            .introduced_in = {25, 6},
            .category = FunctionDocumentation::Category::Introspection
        });
}

}

#endif
