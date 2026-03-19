#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <Common/SymbolIndex.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Diagnostic variant of addressToSymbol.
/// Instead of returning empty string on failure, returns:
///   "no_object"                                     — address not in any mapped binary
///   "no_symbol[object=<path>:offset=0x<hex>]"       — in binary but no ELF symbol at that offset
///   "<demangled_name>"                               — resolved successfully (same as addressToSymbol)
///
/// Use this to categorize why coverageCurrent() produces empty symbols:
///
///   SELECT debugAddressToSymbol(arrayJoin(coverageCurrent())) AS diag,
///          count() AS cnt
///   GROUP BY diag
///   ORDER BY cnt DESC
///   LIMIT 20
class FunctionDebugAddressToSymbol : public IFunction
{
public:
    static constexpr auto name = "debugAddressToSymbol";

    static FunctionPtr create(ContextPtr context)
    {
        context->checkAccess(AccessType::addressToSymbol);
        return std::make_shared<FunctionDebugAddressToSymbol>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"address_of_binary_instruction", &isUInt64, nullptr, "UInt64"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const ColumnPtr & column = arguments[0].column;
        const ColumnUInt64 * column_concrete = checkAndGetColumn<ColumnUInt64>(column.get());

        if (!column_concrete)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}", column->getName(), getName());

        const typename ColumnVector<UInt64>::Container & data = column_concrete->getData();
        auto result_column = ColumnString::create();

        const SymbolIndex & symbol_index = SymbolIndex::instance();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            String diag = symbol_index.diagnose(reinterpret_cast<const void *>(data[i]));
            result_column->insertData(diag.data(), diag.size());
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(DebugAddressToSymbol)
{
    factory.registerFunction<FunctionDebugAddressToSymbol>(
        FunctionDocumentation
        {
            .description = R"(
Diagnostic variant of `addressToSymbol`. Returns a string explaining why symbol resolution succeeded or failed:
- Symbol name if resolution succeeded
- `"no_object"` if the address is not within any mapped binary's address range
- `"no_symbol[object=<path>:offset=0x<hex>]"` if the address maps into a binary but no ELF symbol covers that file offset

Requires `allow_introspection_functions = 1`.

Typical usage to categorize coverage failures:

```sql
SELECT debugAddressToSymbol(arrayJoin(coverageCurrent())) AS diag, count() AS cnt
GROUP BY diag ORDER BY cnt DESC LIMIT 20
```
)",
            .category = FunctionDocumentation::Category::Introspection
        }
    );
}

}

#endif
