#include <Interpreters/MutationsNonDeterministicHelpers.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MutationCommands.h>
#include <Storages/IStorage.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTStatisticsDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/TableNode.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_statistics;
    extern const SettingsBool allow_nondeterministic_mutations;
    extern const SettingsBool validate_mutation_query;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_MUTATION_COMMAND;
    extern const int MULTIPLE_ASSIGNMENTS_TO_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int BAD_ARGUMENTS;
}

namespace
{

void validatePredicateColumns(const ASTPtr & predicate, const StoragePtr & storage, ContextPtr context)
{
    auto select = make_intrusive<ASTSelectQuery>();
    {
        auto filter = predicate->clone();
        select->setExpression(ASTSelectQuery::Expression::WHERE, std::move(filter));

        auto projection = make_intrusive<ASTExpressionList>();
        projection->children.push_back(makeASTFunction("count"));
        select->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(storage->getStorageID());
        table_exp->children.emplace_back(table_exp->database_and_table_name);
        table->table_expression = table_exp;
        tables->children.push_back(table);
        select->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    }

    auto query_tree = buildQueryTree(select, context);
    QueryAnalysisPass query_analysis_pass;
    query_analysis_pass.run(query_tree, context);
}

void validateDeterministicFunctions(const MutationCommand & command, ContextPtr context)
{
    const auto nondeterministic_func_data = findFirstNonDeterministicFunction(command, context);
    if (nondeterministic_func_data.subquery)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER UPDATE/ALTER DELETE statement with subquery may be nondeterministic, "
                                                    "see allow_nondeterministic_mutations setting");

    if (nondeterministic_func_data.nondeterministic_function_name)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The source storage is replicated so ALTER UPDATE/ALTER DELETE statements must use only deterministic functions. "
            "Function '{}' is non-deterministic", *nondeterministic_func_data.nondeterministic_function_name);
}

}

bool MutationCommand::isBarrierCommand() const
{
    return type == RENAME_COLUMN;
}

bool MutationCommand::isPureMetadataCommand() const
{
    return type == ALTER_WITHOUT_MUTATION;
}

bool MutationCommand::isEmptyCommand() const
{
    return type == EMPTY;
}

bool MutationCommand::isDropOrRename() const
{
    return type == Type::DROP_COLUMN
        || type == Type::DROP_INDEX
        || type == Type::DROP_PROJECTION
        || type == Type::DROP_STATISTICS
        || type == Type::RENAME_COLUMN;
}

bool MutationCommand::affectsAllColumns() const
{
    return type == DELETE
        || type == APPLY_DELETED_MASK
        || type == REWRITE_PARTS;
}

std::optional<MutationCommand> MutationCommand::parse(const ASTAlterCommand & command, bool parse_alter_commands, bool with_pure_metadata_commands)
{
    MutationCommand res;
    res.ast = command.clone();
    if (with_pure_metadata_commands)
    {
        res.type = ALTER_WITHOUT_MUTATION;
        return res;
    }

    if (command.type == ASTAlterCommand::DELETE)
    {
        res.type = DELETE;
        res.predicate = command.predicate->clone();
        if (command.partition)
            res.partition = command.partition->clone();
        return res;
    }
    if (command.type == ASTAlterCommand::UPDATE)
    {
        res.type = UPDATE;
        res.predicate = command.predicate->clone();
        if (command.partition)
            res.partition = command.partition->clone();
        for (const ASTPtr & assignment_ast : command.update_assignments->children)
        {
            const auto & assignment = assignment_ast->as<ASTAssignment &>();
            auto insertion = res.column_to_update_expression.emplace(assignment.column_name, assignment.expression());
            if (!insertion.second)
                throw Exception(ErrorCodes::MULTIPLE_ASSIGNMENTS_TO_COLUMN, "Multiple assignments in the single statement to column {}", backQuote(assignment.column_name));
        }
        return res;
    }
    if (command.type == ASTAlterCommand::APPLY_DELETED_MASK)
    {
        res.type = APPLY_DELETED_MASK;
        if (command.partition)
            res.partition = command.partition->clone();
        return res;
    }
    else if (command.type == ASTAlterCommand::APPLY_PATCHES)
    {
        res.type = APPLY_PATCHES;
        if (command.partition)
            res.partition = command.partition->clone();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_INDEX)
    {
        res.type = MATERIALIZE_INDEX;
        if (command.partition)
            res.partition = command.partition->clone();
        res.predicate = nullptr;
        res.index_name = command.index->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_STATISTICS)
    {
        res.type = MATERIALIZE_STATISTICS;
        if (command.partition)
            res.partition = command.partition->clone();
        res.predicate = nullptr;
        if (command.statistics_decl)
        {
            res.statistics_columns = command.statistics_decl->as<ASTStatisticsDeclaration &>().getColumnNames();
        }
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_PROJECTION)
    {
        res.type = MATERIALIZE_PROJECTION;
        if (command.partition)
            res.partition = command.partition->clone();
        res.predicate = nullptr;
        res.projection_name = command.projection->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_COLUMN)
    {
        res.type = MATERIALIZE_COLUMN;
        if (command.partition)
            res.partition = command.partition->clone();
        res.column_name = getIdentifierName(command.column);
        return res;
    }
    /// MODIFY COLUMN x REMOVE MATERIALIZED/RESET SETTING/MODIFY SETTING is a valid alter command, but doesn't have any specified column type,
    /// thus no mutation is needed
    if (parse_alter_commands && command.type == ASTAlterCommand::MODIFY_COLUMN && command.remove_property.empty()
        && nullptr == command.settings_changes && nullptr == command.settings_resets)
    {
        const auto & ast_col_decl = command.col_decl->as<ASTColumnDeclaration &>();

        if (ast_col_decl.getType() != nullptr)
        {
            res.type = MutationCommand::Type::READ_COLUMN;
            res.column_name = ast_col_decl.name;
            res.data_type = DataTypeFactory::instance().get(ast_col_decl.getType());
            return res;
        }

        const bool metadata_only_modification
            = ast_col_decl.getDefaultExpression() != nullptr
            || ast_col_decl.getComment() != nullptr
            || ast_col_decl.getCodec() != nullptr
            || ast_col_decl.getTTL() != nullptr;

        if (!metadata_only_modification)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MODIFY COLUMN mutation command doesn't specify type: {}", command.formatForErrorMessage());
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_COLUMN)
    {
        res.type = MutationCommand::Type::DROP_COLUMN;
        res.column_name = getIdentifierName(command.column);
        if (command.partition)
            res.partition = command.partition->clone();
        if (command.clear_column)
            res.clear = true;

        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_INDEX)
    {
        res.type = MutationCommand::Type::DROP_INDEX;
        res.column_name = command.index->as<ASTIdentifier &>().name();
        if (command.partition)
            res.partition = command.partition->clone();
        if (command.clear_index)
            res.clear = true;
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_STATISTICS)
    {
        res.type = MutationCommand::Type::DROP_STATISTICS;
        if (command.partition)
            res.partition = command.partition->clone();
        if (command.clear_statistics)
            res.clear = true;
        if (command.statistics_decl)
            res.statistics_columns = command.statistics_decl->as<ASTStatisticsDeclaration &>().getColumnNames();
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::DROP_PROJECTION)
    {
        res.type = MutationCommand::Type::DROP_PROJECTION;
        res.column_name = command.projection->as<ASTIdentifier &>().name();
        if (command.partition)
            res.partition = command.partition->clone();
        if (command.clear_projection)
            res.clear = true;
        return res;
    }
    if (parse_alter_commands && command.type == ASTAlterCommand::RENAME_COLUMN)
    {
        res.type = MutationCommand::Type::RENAME_COLUMN;
        res.column_name = command.column->as<ASTIdentifier &>().name();
        res.rename_to = command.rename_to->as<ASTIdentifier &>().name();
        return res;
    }
    if (command.type == ASTAlterCommand::MATERIALIZE_TTL)
    {
        res.type = MATERIALIZE_TTL;
        if (command.partition)
            res.partition = command.partition->clone();
        return res;
    }
    if (command.type == ASTAlterCommand::REWRITE_PARTS)
    {
        res.type = REWRITE_PARTS;
        if (command.partition)
            res.partition = command.partition->clone();
        return res;
    }

    res.type = ALTER_WITHOUT_MUTATION;
    return res;
}

boost::intrusive_ptr<ASTExpressionList> MutationCommands::ast(bool with_pure_metadata_commands) const
{
    auto res = make_intrusive<ASTExpressionList>();
    for (const MutationCommand & command : *this)
    {
        if (!command.isPureMetadataCommand() || with_pure_metadata_commands)
            res->children.push_back(command.ast->clone());
    }
    return res;
}


void MutationCommands::writeText(WriteBuffer & out, bool with_pure_metadata_commands) const
{
    writeEscapedString(ast(with_pure_metadata_commands)->formatWithSecretsOneLine(), out);
}

void MutationCommands::readText(ReadBuffer & in, bool with_pure_metadata_commands)
{
    String commands_str;
    readEscapedString(commands_str, in);

    ParserAlterCommandList p_alter_commands;
    auto commands_ast = parseQuery(
        p_alter_commands, commands_str.data(), commands_str.data() + commands_str.length(), "mutation commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    for (const auto & child : commands_ast->children)
    {
        auto * command_ast = child->as<ASTAlterCommand>();
        auto command = MutationCommand::parse(*command_ast, true, with_pure_metadata_commands);
        if (!command)
            throw Exception(ErrorCodes::UNKNOWN_MUTATION_COMMAND, "Unknown mutation command type: {}", DB::toString<int>(command_ast->type));
        command->ast = child;
        push_back(std::move(*command));
    }
}


std::string MutationCommands::toString(bool with_pure_metadata_commands) const
{
    return ast(with_pure_metadata_commands)->formatWithSecretsOneLine();
}


bool MutationCommands::hasNonEmptyMutationCommands() const
{
    for (const auto & command : *this)
    {
        if (!command.isEmptyCommand() && !command.isPureMetadataCommand())
            return true;
    }
    return false;
}

bool MutationCommands::hasAnyUpdateCommand() const
{
    return std::ranges::any_of(*this, [](const auto & command) { return command.type == MutationCommand::Type::UPDATE; });
}

bool MutationCommands::hasOnlyUpdateCommands() const
{
    return std::ranges::all_of(*this, [](const auto & command) { return command.type == MutationCommand::Type::UPDATE; });
}

bool MutationCommands::containBarrierCommand() const
{
    for (const auto & command : *this)
    {
        if (command.isBarrierCommand())
            return true;
    }
    return false;
}

NameSet MutationCommands::getAllUpdatedColumns() const
{
    NameSet res;
    for (const auto & command : *this)
        for (const auto & [column_name, _] : command.column_to_update_expression)
            res.insert(column_name);
    return res;
}

}
