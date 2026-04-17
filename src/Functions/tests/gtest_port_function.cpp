#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>

using namespace DB;

namespace
{

FunctionBasePtr buildFunction(const String & name, const DataTypes & argument_types)
{
    tryRegisterFunctions();

    auto context = getContext().context;
    auto resolver = FunctionFactory::instance().get(name, context);

    ColumnsWithTypeAndName arguments;
    arguments.reserve(argument_types.size());
    for (const auto & argument_type : argument_types)
        arguments.emplace_back(ColumnWithTypeAndName{nullptr, argument_type, ""});

    return resolver->build(arguments);
}

Field evaluateFunction(const String & name, const std::vector<String> & arguments_str)
{
    auto function = buildFunction(name, {std::make_shared<DataTypeString>()});

    ColumnsWithTypeAndName arguments;
    for (const auto & str : arguments_str)
    {
        auto column = ColumnString::create();
        column->insert(str);
        arguments.emplace_back(ColumnWithTypeAndName{std::move(column), std::make_shared<DataTypeString>(), ""});
    }

    chassert(!arguments.empty());
    auto result = function->execute(arguments, std::make_shared<DataTypeUInt16>(), arguments[0].column->size(), false);
    return (*result)[0];
}

}

TEST(PortFunction, UserinfoWithoutPort)
{
    // Test cases for URLs with userinfo but no port

    // URL: //paul@www.example.com - no port, should return 0
    EXPECT_EQ(evaluateFunction("port", {"//paul@www.example.com"}), UInt64(0));

    // URL: //user@host - no port, should return 0
    EXPECT_EQ(evaluateFunction("port", {"//user@host"}), UInt64(0));

    // URL: //@example.com - empty userinfo, no port
    EXPECT_EQ(evaluateFunction("port", {"//@example.com"}), UInt64(0));
}

TEST(PortFunction, UserinfoWithPort)
{
    // Test cases for URLs with userinfo AND port

    // URL: //user@example.com:8080
    EXPECT_EQ(evaluateFunction("port", {"//user@example.com:8080"}), UInt64(8080));

    // URL: //paul@www.example.com:8080
    EXPECT_EQ(evaluateFunction("port", {"//paul@www.example.com:8080"}), UInt64(8080));
}

TEST(PortFunction, NoUserinfo)
{
    // Test cases without userinfo

    // URL: //example.com - no port
    EXPECT_EQ(evaluateFunction("port", {"//example.com"}), UInt64(0));

    // URL: //www.example.com:9090
    EXPECT_EQ(evaluateFunction("port", {"//www.example.com:9090"}), UInt64(9090));

    // URL with scheme: https://example.com:443
    EXPECT_EQ(evaluateFunction("port", {"https://example.com:443"}), UInt64(443));
}

TEST(PortFunction, EdgeCases)
{
    // Empty URL - should return 0
    EXPECT_EQ(evaluateFunction("port", {""}), UInt64(0));

    // URL with just scheme separator
    EXPECT_EQ(evaluateFunction("port", {"//"}), UInt64(0));

    // URL with path but no host or port
    EXPECT_EQ(evaluateFunction("port", {"//host/path"}), UInt64(0));
}

TEST(PortFunction, RFCVersion)
{
    // Test portRFC function with userinfo URLs

    // URL: //user@example.com - no port
    EXPECT_EQ(evaluateFunction("portRFC", {"//user@example.com"}), UInt64(0));

    // URL: //paul@www.example.com:8080
    EXPECT_EQ(evaluateFunction("portRFC", {"//paul@www.example.com:8080"}), UInt64(8080));
}
