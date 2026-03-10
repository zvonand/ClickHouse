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

/// Build an ASTFunction representing `key = value` as the parser expects.
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

/// Build an ASTExpressionList from child ASTs.
ASTPtr makeArgList(ASTs children)
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children = std::move(children);
    return list;
}

ContextPtr ctx()
{
    return getContext().context;
}

}

// ---- Basic named argument parsing ----

TEST(IcebergCommandArgumentsParser, NamedInt64)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("count", ArgType::Int64);

    auto args = makeArgList({makeKV("count", Field(Int64(42)))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("count"));
    ASSERT_EQ(result.getInt64("count"), 42);
}

TEST(IcebergCommandArgumentsParser, NamedUInt64ConvertedToInt64)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("count", ArgType::Int64);

    auto args = makeArgList({makeKV("count", Field(UInt64(100)))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("count"));
    ASSERT_EQ(result.getInt64("count"), 100);
}

TEST(IcebergCommandArgumentsParser, NamedBoolFromTrue)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("flag", ArgType::Bool);

    auto args = makeArgList({makeKV("flag", Field(true))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("flag"));
    ASSERT_TRUE(result.getBool("flag"));
}

TEST(IcebergCommandArgumentsParser, NamedBoolFromStringCaseInsensitive)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("flag", ArgType::Bool);

    auto args = makeArgList({makeKV("flag", Field("True"))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.getBool("flag"));
}

TEST(IcebergCommandArgumentsParser, NamedBoolFromInt)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("flag", ArgType::Bool);

    auto args = makeArgList({makeKV("flag", Field(UInt64(0)))});
    auto result = parser.parse(args, ctx());

    ASSERT_FALSE(result.getBool("flag"));
}

TEST(IcebergCommandArgumentsParser, NamedString)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("name", ArgType::String);

    auto args = makeArgList({makeKV("name", Field("hello"))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("name"));
    ASSERT_EQ(result.getString("name"), "hello");
}

TEST(IcebergCommandArgumentsParser, NamedArray)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("ids", ArgType::Array);

    Array arr{Field(Int64(1)), Field(Int64(2)), Field(Int64(3))};
    auto args = makeArgList({makeKV("ids", Field(arr))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("ids"));
    const auto & got = result.getArray("ids");
    ASSERT_EQ(got.size(), 3);
    ASSERT_EQ(got[0].safeGet<Int64>(), 1);
    ASSERT_EQ(got[2].safeGet<Int64>(), 3);
}

TEST(IcebergCommandArgumentsParser, NamedField)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("anything", ArgType::Field);

    auto args = makeArgList({makeKV("anything", Field(3.14))});
    auto result = parser.parse(args, ctx());

    ASSERT_TRUE(result.has("anything"));
}

// ---- Missing arguments ----

TEST(IcebergCommandArgumentsParser, MissingArgReturnsFalseOnHas)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);

    auto result = parser.parse(nullptr, ctx());
    ASSERT_FALSE(result.has("x"));
}

TEST(IcebergCommandArgumentsParser, TryGetReturnsNulloptWhenMissing)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);
    parser.addNamedArg("y", ArgType::Bool);
    parser.addNamedArg("z", ArgType::String);

    auto result = parser.parse(nullptr, ctx());
    ASSERT_FALSE(result.tryGetInt64("x").has_value());
    ASSERT_FALSE(result.tryGetBool("y").has_value());
    ASSERT_FALSE(result.tryGetString("z").has_value());
}

// ---- Positional arguments ----

TEST(IcebergCommandArgumentsParser, SinglePositional)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addPositional();

    auto args = makeArgList({make_intrusive<ASTLiteral>(Field("2025-01-01 00:00:00"))});
    auto result = parser.parse(args, ctx());

    ASSERT_EQ(result.positional().size(), 1);
    ASSERT_EQ(result.positional()[0].safeGet<String>(), "2025-01-01 00:00:00");
}

TEST(IcebergCommandArgumentsParser, PositionalAndNamed)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addPositional();
    parser.addNamedArg("dry_run", ArgType::Bool);

    auto args = makeArgList({
        make_intrusive<ASTLiteral>(Field("2025-01-01 00:00:00")),
        makeKV("dry_run", Field(true)),
    });
    auto result = parser.parse(args, ctx());

    ASSERT_EQ(result.positional().size(), 1);
    ASSERT_TRUE(result.getBool("dry_run"));
}

// ---- Error cases ----

TEST(IcebergCommandArgumentsParser, UnknownArgThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);

    auto args = makeArgList({makeKV("unknown_arg", Field(Int64(1)))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, TooManyPositionalsThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addPositional();

    auto args = makeArgList({
        make_intrusive<ASTLiteral>(Field("a")),
        make_intrusive<ASTLiteral>(Field("b")),
    });
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, TypeMismatchStringForInt64Throws)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("count", ArgType::Int64);

    auto args = makeArgList({makeKV("count", Field("not_a_number"))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, TypeMismatchIntForStringThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("name", ArgType::String);

    auto args = makeArgList({makeKV("name", Field(Int64(42)))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, TypeMismatchIntForArrayThrows)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("ids", ArgType::Array);

    auto args = makeArgList({makeKV("ids", Field(Int64(1)))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

TEST(IcebergCommandArgumentsParser, BoolRejectsGarbageString)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("flag", ArgType::Bool);

    auto args = makeArgList({makeKV("flag", Field("maybe"))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

// ---- Constraints ----

TEST(IcebergCommandArgumentsParser, ConstraintPasses)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("a", ArgType::Int64);
    parser.addNamedArg("b", ArgType::Int64);

    parser.addConstraint([](const ParsedArguments & parsed) {
        if (parsed.has("a") && parsed.has("b"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a and b are mutually exclusive");
    });

    auto args = makeArgList({makeKV("a", Field(Int64(1)))});
    ASSERT_NO_THROW(parser.parse(args, ctx()));
}

TEST(IcebergCommandArgumentsParser, ConstraintFails)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("a", ArgType::Int64);
    parser.addNamedArg("b", ArgType::Int64);

    parser.addConstraint([](const ParsedArguments & parsed) {
        if (parsed.has("a") && parsed.has("b"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "a and b are mutually exclusive");
    });

    auto args = makeArgList({makeKV("a", Field(Int64(1))), makeKV("b", Field(Int64(2)))});
    ASSERT_THROW(parser.parse(args, ctx()), Exception);
}

// ---- Multiple named args ----

TEST(IcebergCommandArgumentsParser, MultipleNamedArgs)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("retention_period", ArgType::String);
    parser.addNamedArg("retain_last", ArgType::Int64);
    parser.addNamedArg("dry_run", ArgType::Bool);

    auto args = makeArgList({
        makeKV("retention_period", Field("3d")),
        makeKV("retain_last", Field(Int64(5))),
        makeKV("dry_run", Field(true)),
    });
    auto result = parser.parse(args, ctx());

    ASSERT_EQ(result.getString("retention_period"), "3d");
    ASSERT_EQ(result.getInt64("retain_last"), 5);
    ASSERT_TRUE(result.getBool("dry_run"));
}

// ---- Empty / null args ----

TEST(IcebergCommandArgumentsParser, NullArgsReturnsEmptyResult)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);
    parser.addPositional();

    auto result = parser.parse(nullptr, ctx());
    ASSERT_FALSE(result.has("x"));
    ASSERT_TRUE(result.positional().empty());
}

TEST(IcebergCommandArgumentsParser, EmptyArgListReturnsEmptyResult)
{
    IcebergCommandArgumentsParser parser("test_cmd");
    parser.addNamedArg("x", ArgType::Int64);

    auto args = makeArgList({});
    auto result = parser.parse(args, ctx());
    ASSERT_FALSE(result.has("x"));
}

#endif
