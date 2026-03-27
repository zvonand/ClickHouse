#include <gtest/gtest.h>

#include <Interpreters/PredicateAtomExtractor.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/tests/gtest_global_register.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

using namespace DB;

/// Helper to build a simple DAG node: function(column, constant)
static const ActionsDAG::Node & makePredicateNode(
    ActionsDAG & dag,
    const String & func_name,
    const String & col_name,
    const DataTypePtr & col_type,
    const Field & constant)
{
    const auto & col_node = dag.addInput(col_name, col_type);
    ColumnWithTypeAndName const_col;
    const_col.type = col_type;
    const_col.column = col_type->createColumnConst(1, constant);
    const_col.name = constant.dump();
    const auto & const_node = dag.addColumn(std::move(const_col));

    auto resolver = FunctionFactory::instance().get(func_name, nullptr);
    return dag.addFunction(resolver, {&col_node, &const_node}, func_name + "_result");
}


TEST(PredicateAtomExtractor, ClassifyEquals)
{
    EXPECT_EQ(classifyPredicateFunction("equals"), "Equality");
    EXPECT_EQ(classifyPredicateFunction("notEquals"), "Equality");
}

TEST(PredicateAtomExtractor, ClassifyRange)
{
    EXPECT_EQ(classifyPredicateFunction("less"), "Range");
    EXPECT_EQ(classifyPredicateFunction("greater"), "Range");
    EXPECT_EQ(classifyPredicateFunction("lessOrEquals"), "Range");
    EXPECT_EQ(classifyPredicateFunction("greaterOrEquals"), "Range");
}

TEST(PredicateAtomExtractor, ClassifyIn)
{
    EXPECT_EQ(classifyPredicateFunction("in"), "In");
    EXPECT_EQ(classifyPredicateFunction("globalIn"), "In");
    EXPECT_EQ(classifyPredicateFunction("notIn"), "In");
    EXPECT_EQ(classifyPredicateFunction("globalNotIn"), "In");
}

TEST(PredicateAtomExtractor, ClassifyLike)
{
    EXPECT_EQ(classifyPredicateFunction("like"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("ilike"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("notLike"), "LikeSubstring");
    EXPECT_EQ(classifyPredicateFunction("notILike"), "LikeSubstring");
}

TEST(PredicateAtomExtractor, ClassifyIsNull)
{
    EXPECT_EQ(classifyPredicateFunction("isNull"), "IsNull");
    EXPECT_EQ(classifyPredicateFunction("isNotNull"), "IsNull");
}

TEST(PredicateAtomExtractor, ClassifyOther)
{
    EXPECT_EQ(classifyPredicateFunction("has"), "Other");
    EXPECT_EQ(classifyPredicateFunction("startsWith"), "Other");
    EXPECT_EQ(classifyPredicateFunction("unknown_function"), "Other");
}

TEST(PredicateAtomExtractor, ExtractFromNullptr)
{
    auto atoms = extractPredicateAtoms(nullptr);
    EXPECT_TRUE(atoms.empty());
}

TEST(PredicateAtomExtractor, ExtractSingleAtom)
{
    tryRegisterFunctions();
    ActionsDAG dag;
    /// WHERE x = 1
    const auto & eq_node = makePredicateNode(dag, "equals", "x", std::make_shared<DataTypeUInt64>(), Field(UInt64(1)));

    auto atoms = extractPredicateAtoms(&eq_node);
    ASSERT_EQ(atoms.size(), 1);
    EXPECT_EQ(atoms[0].column_name, "x");
    EXPECT_EQ(atoms[0].function_name, "equals");
    EXPECT_EQ(atoms[0].predicate_class, "Equality");
}

TEST(PredicateAtomExtractor, ExtractConjunction)
{
    tryRegisterFunctions();
    ActionsDAG dag;
    /// WHERE x = 1 AND y > 5
    const auto & eq_node = makePredicateNode(dag, "equals", "x", std::make_shared<DataTypeUInt64>(), Field(UInt64(1)));
    const auto & gt_node = makePredicateNode(dag, "greater", "y", std::make_shared<DataTypeUInt64>(), Field(UInt64(5)));

    auto and_resolver = FunctionFactory::instance().get("and", nullptr);
    const auto & and_node = dag.addFunction(and_resolver, {&eq_node, &gt_node}, "and_result");

    auto atoms = extractPredicateAtoms(&and_node);
    ASSERT_EQ(atoms.size(), 2);

    /// Order may vary, so check both are present
    std::set<String> columns;
    for (const auto & atom : atoms)
        columns.insert(atom.column_name);
    EXPECT_TRUE(columns.count("x"));
    EXPECT_TRUE(columns.count("y"));
}

TEST(PredicateAtomExtractor, ExtractSkipsMultiColumnAtom)
{
    tryRegisterFunctions();
    ActionsDAG dag;
    /// WHERE x = y (two column inputs, no single column → skipped)
    const auto & x_node = dag.addInput("x", std::make_shared<DataTypeUInt64>());
    const auto & y_node = dag.addInput("y", std::make_shared<DataTypeUInt64>());

    auto eq_resolver = FunctionFactory::instance().get("equals", nullptr);
    const auto & eq_node = dag.addFunction(eq_resolver, {&x_node, &y_node}, "eq_result");

    auto atoms = extractPredicateAtoms(&eq_node);
    EXPECT_TRUE(atoms.empty());
}

TEST(PredicateAtomExtractor, ExtractThroughAlias)
{
    tryRegisterFunctions();
    ActionsDAG dag;
    /// WHERE alias(x = 1)
    const auto & eq_node = makePredicateNode(dag, "equals", "x", std::make_shared<DataTypeUInt64>(), Field(UInt64(1)));
    const auto & alias_node = dag.addAlias(eq_node, "my_alias");

    auto atoms = extractPredicateAtoms(&alias_node);
    ASSERT_EQ(atoms.size(), 1);
    EXPECT_EQ(atoms[0].column_name, "x");
    EXPECT_EQ(atoms[0].function_name, "equals");
}
