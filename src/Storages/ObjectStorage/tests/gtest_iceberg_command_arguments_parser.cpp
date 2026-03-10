#include <gtest/gtest.h>
#include <config.h>

#if USE_AVRO

#include <Common/tests/gtest_global_context.h>
#include <Common/Exception.h>
#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergCommandArgumentsParser.h>

using namespace DB;
using namespace DB::Iceberg;

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{

ASTPtr makeKV(const String & key, Field value)
{
    auto args = make_intrusive<ASTExpressionList>();
    args->children.push_back(make_intrusive<ASTLiteral>(key));
    args->children.push_back(make_intrusive<ASTLiteral>(std::move(value)));

    auto func = make_intrusive<ASTFunction>();
    func->name = "equals";
    func->arguments = args;
    func->children.push_back(func->arguments);
    return func;
}

ASTPtr makeArgList(ASTs children)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(children);
    return list;
}

ContextPtr ctx() { return getContext().context; }

}

/// ----- Parameterized: successful type conversion -----

struct ParseOKCase
{
    String name;
    ArgType type;
    Field input;
    Field expected;
};

class ParseOK : public ::testing::TestWithParam<ParseOKCase> {};

TEST_P(ParseOK, NamedArgParsesCorrectly)
{
    auto [name, type, input, expected] = GetParam();

    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg(name, type);

    auto result = parser.parse(makeArgList({makeKV(name, input)}), ctx());
    ASSERT_TRUE(result.has(name));
    ASSERT_EQ(result.getField(name), expected);
}

INSTANTIATE_TEST_SUITE_P(IcebergArgs, ParseOK, ::testing::Values(
    ParseOKCase{"count",  ArgType::Int64,  Field(Int64(42)),   Field(Int64(42))},
    ParseOKCase{"count",  ArgType::Int64,  Field(UInt64(100)), Field(Int64(100))},
    ParseOKCase{"flag",   ArgType::Bool,   Field(true),        Field(true)},
    ParseOKCase{"flag",   ArgType::Bool,   Field("True"),      Field(true)},
    ParseOKCase{"flag",   ArgType::Bool,   Field("false"),     Field(false)},
    ParseOKCase{"flag",   ArgType::Bool,   Field(UInt64(0)),   Field(false)},
    ParseOKCase{"name",   ArgType::String, Field("hello"),     Field("hello")},
    ParseOKCase{"any",    ArgType::Field,  Field(3.14),        Field(3.14)}
));

/// ----- Parameterized: type mismatch (should throw) -----

struct TypeMismatchCase
{
    String name;
    ArgType type;
    Field input;
};

class TypeMismatch : public ::testing::TestWithParam<TypeMismatchCase> {};

TEST_P(TypeMismatch, Throws)
{
    auto [name, type, input] = GetParam();

    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg(name, type);

    ASSERT_THROW(parser.parse(makeArgList({makeKV(name, input)}), ctx()), Exception);
}

INSTANTIATE_TEST_SUITE_P(IcebergArgs, TypeMismatch, ::testing::Values(
    TypeMismatchCase{"count", ArgType::Int64,  Field("not_a_number")},
    TypeMismatchCase{"name",  ArgType::String, Field(Int64(42))},
    TypeMismatchCase{"ids",   ArgType::Array,  Field(Int64(1))},
    TypeMismatchCase{"flag",  ArgType::Bool,   Field("maybe")}
));

/// ----- Non-parameterized tests for structural behavior -----

TEST(IcebergCommandArgumentsParser, ArrayArg)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("ids", ArgType::Array);

    Array arr{Field(Int64(1)), Field(Int64(2)), Field(Int64(3))};
    auto result = parser.parse(makeArgList({makeKV("ids", Field(arr))}), ctx());

    const auto & got = result.getArray("ids");
    ASSERT_EQ(got.size(), 3);
    ASSERT_EQ(got[0].safeGet<Int64>(), 1);
    ASSERT_EQ(got[2].safeGet<Int64>(), 3);
}

TEST(IcebergCommandArgumentsParser, MissingArgAndTryGet)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);
    parser.addNamedArg("y", ArgType::Bool);
    parser.addNamedArg("z", ArgType::String);

    auto result = parser.parse(nullptr, ctx());
    ASSERT_FALSE(result.has("x"));
    ASSERT_FALSE(result.tryGetInt64("x").has_value());
    ASSERT_FALSE(result.tryGetBool("y").has_value());
    ASSERT_FALSE(result.tryGetString("z").has_value());
    ASSERT_TRUE(result.positional().empty());
}

TEST(IcebergCommandArgumentsParser, Positional)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addPositional();
    parser.addNamedArg("dry_run", ArgType::Bool);

    auto result = parser.parse(makeArgList({
        make_intrusive<ASTLiteral>(Field("2025-01-01 00:00:00")),
        makeKV("dry_run", Field(true)),
    }), ctx());

    ASSERT_EQ(result.positional().size(), 1);
    ASSERT_EQ(result.positional()[0].safeGet<String>(), "2025-01-01 00:00:00");
    ASSERT_TRUE(result.getBool("dry_run"));
}

TEST(IcebergCommandArgumentsParser, MultipleNamedArgs)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("retention_period", ArgType::String);
    parser.addNamedArg("retain_last", ArgType::Int64);
    parser.addNamedArg("dry_run", ArgType::Bool);

    auto result = parser.parse(makeArgList({
        makeKV("retention_period", Field("3d")),
        makeKV("retain_last", Field(Int64(5))),
        makeKV("dry_run", Field(true)),
    }), ctx());

    ASSERT_EQ(result.getString("retention_period"), "3d");
    ASSERT_EQ(result.getInt64("retain_last"), 5);
    ASSERT_TRUE(result.getBool("dry_run"));
}

TEST(IcebergCommandArgumentsParser, UnknownArgThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);

    ASSERT_THROW(parser.parse(makeArgList({makeKV("bad", Field(Int64(1)))}), ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, TooManyPositionalsThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addPositional();

    ASSERT_THROW(parser.parse(makeArgList({
        make_intrusive<ASTLiteral>(Field("a")),
        make_intrusive<ASTLiteral>(Field("b")),
    }), ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, ConstraintValidation)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("a", ArgType::Int64);
    parser.addNamedArg("b", ArgType::Int64);
    parser.addConstraint([](const ParsedArguments & p) {
        if (p.has("a") && p.has("b"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a and b are mutually exclusive");
    });

    ASSERT_NO_THROW(parser.parse(makeArgList({makeKV("a", Field(Int64(1)))}), ctx()));
    ASSERT_THROW(parser.parse(makeArgList({makeKV("a", Field(Int64(1))), makeKV("b", Field(Int64(2)))}), ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, EmptyArgList)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);

    auto result = parser.parse(makeArgList({}), ctx());
    ASSERT_FALSE(result.has("x"));
}

#endif
