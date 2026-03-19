#include <optional>
#include <string_view>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Common/FieldVisitorToString.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/RollupStep.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace QueryPlanFormat
{
    constexpr std::string_view TABLE_PREFIX = "__table";

    /// Matches `__table<digits>.` at position pos, returns the position after the dot or 0 on mismatch.
    size_t matchTablePrefix(std::string_view name, size_t pos)
    {
        if (!name.substr(pos).starts_with(TABLE_PREFIX))
            return 0;
        size_t j = pos + TABLE_PREFIX.size();
        while (j < name.size() && std::isdigit(static_cast<unsigned char>(name[j])))
            ++j;
        if (j > pos + TABLE_PREFIX.size() && j < name.size() && name[j] == '.')
            return j + 1;
        return 0;
    }

    String trimColumnIdentifier(std::string_view name)
    {
        if (name.find(TABLE_PREFIX) == std::string_view::npos)
            return String(name);

        String result;
        result.reserve(name.size());
        size_t seg_start = 0;
        for (size_t i = 0; i < name.size();)
        {
            if (size_t after = matchTablePrefix(name, i))
            {
                result.append(name, seg_start, i - seg_start);
                i = after;
                seg_start = after;
            }
            else
            {
                ++i;
            }
        }
        result.append(name, seg_start, name.size() - seg_start);
        return result;
    }

    void formatJoinOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix)
    {
        const auto & input_headers = step.getInputHeaders();
        if (input_headers.size() != 2 || !input_headers[0] || !input_headers[1])
            return;

        out << prefix << "Output:\n";

        if (!step.hasOutputHeader() || step.getOutputHeader()->empty())
        {
            out << prefix << "  Left:  Empty\n";
            out << prefix << "  Right: Empty\n";
            return;
        }

        const auto & output = *step.getOutputHeader();
        const auto & left_input = *input_headers[0];
        const auto & right_input = *input_headers[1];

        std::vector<String> left_columns;
        std::vector<String> right_columns;

        for (const auto & col : output)
        {
            if (left_input.has(col.name))
                left_columns.push_back(trimColumnIdentifier(col.name));
            else if (right_input.has(col.name))
                right_columns.push_back(trimColumnIdentifier(col.name));
        }

        out << prefix << "  Left:  ";
        if (left_columns.empty())
            out << "Empty";
        else
            out << fmt::format("{}", fmt::join(left_columns, ", "));
        out << "\n";

        out << prefix << "  Right: ";
        if (right_columns.empty())
            out << "Empty";
        else
            out << fmt::format("{}", fmt::join(right_columns, ", "));
        out << "\n";
    }

    void formatOutputColumns(WriteBuffer & out, const IQueryPlanStep & step, const String & prefix)
    {
        if (!step.hasOutputHeader() || step.getOutputHeader()->empty())
        {
            out << prefix << "Output: Empty\n";
            return;
        }

        out << prefix << "Output: ";
        bool first = true;
        for (const auto & elem : *step.getOutputHeader())
        {
            if (!first)
                out << ", ";
            first = false;
            out << trimColumnIdentifier(elem.name);
        }
        out << '\n';
    }

    namespace
    {
        struct OperatorInfo
        {
            std::string_view symbol;
            int precedence;
        };

        std::optional<OperatorInfo> getOperatorInfo(const std::string & func_name)
        {
            if (func_name == "or")                return OperatorInfo{"OR", 3};
            if (func_name == "and")               return OperatorInfo{"AND", 4};
            if (func_name == "not")               return OperatorInfo{{}, 5};
            if (func_name == "isNull")            return OperatorInfo{{}, 6};
            if (func_name == "isNotNull")         return OperatorInfo{{}, 6};
            if (func_name == "isNotDistinctFrom") return OperatorInfo{"<=>", 6};
            if (func_name == "isDistinctFrom")    return OperatorInfo{"IS DISTINCT FROM", 6};
            if (func_name == "equals")            return OperatorInfo{"=", 9};
            if (func_name == "notEquals")         return OperatorInfo{"!=", 9};
            if (func_name == "less")              return OperatorInfo{"<", 9};
            if (func_name == "greater")           return OperatorInfo{">", 9};
            if (func_name == "lessOrEquals")      return OperatorInfo{"<=", 9};
            if (func_name == "greaterOrEquals")    return OperatorInfo{">=", 9};
            if (func_name == "like")              return OperatorInfo{"LIKE", 9};
            if (func_name == "notLike")           return OperatorInfo{"NOT LIKE", 9};
            if (func_name == "ilike")             return OperatorInfo{"ILIKE", 9};
            if (func_name == "notILike")          return OperatorInfo{"NOT ILIKE", 9};
            if (func_name == "in" || func_name == "globalIn"
                || func_name == "nullIn" || func_name == "globalNullIn")
                                                  return OperatorInfo{"IN", 9};
            if (func_name == "notIn" || func_name == "globalNotIn"
                || func_name == "notNullIn" || func_name == "globalNotNullIn")
                                                  return OperatorInfo{"NOT IN", 9};
            if (func_name == "match")             return OperatorInfo{"REGEXP", 9};
            if (func_name == "concat")            return OperatorInfo{"||", 10};
            if (func_name == "plus")              return OperatorInfo{"+", 11};
            if (func_name == "minus")             return OperatorInfo{"-", 11};
            if (func_name == "multiply")          return OperatorInfo{"*", 12};
            if (func_name == "divide")            return OperatorInfo{"/", 12};
            if (func_name == "modulo")            return OperatorInfo{"%", 12};
            if (func_name == "intDiv")            return OperatorInfo{"DIV", 12};
            if (func_name == "negate")            return OperatorInfo{{}, 13};
            if (func_name == "tupleElement")      return OperatorInfo{{}, 14};
            if (func_name == "arrayElement")      return OperatorInfo{{}, 14};
            return std::nullopt;
        }

        String formatConstant(const ActionsDAG::Node * node)
        {
            if (!node->column || node->column->empty())
                return node->result_name;

            WhichDataType data_type(node->result_type);

            if (data_type.isDateOrDate32OrTimeOrTime64OrDateTimeOrDateTime64())
            {
                WriteBufferFromOwnString buf;
                writeChar('\'', buf);
                const auto & col = node->column->convertToFullColumnIfConst();
                node->result_type->getDefaultSerialization()->serializeText(*col, 0, buf, {});
                writeChar('\'', buf);
                return buf.str();
            }

            Field value;
            node->column->get(0, value);
            return applyVisitor(FieldVisitorToString(), value);
        }
    }

    String formatNodePretty(const ActionsDAG::Node * node, int parent_precedence)
    {
        using ActionType = ActionsDAG::ActionType;

        switch (node->type)
        {
            case ActionType::INPUT:
                return trimColumnIdentifier(node->result_name);

            case ActionType::COLUMN:
                return formatConstant(node);

            case ActionType::ALIAS:
                return formatNodePretty(node->children.front(), parent_precedence);

            case ActionType::ARRAY_JOIN:
                return "arrayJoin(" + formatNodePretty(node->children.front()) + ")";

            case ActionType::FUNCTION:
            {
                auto func_name = node->function_base->getName();

                if ((func_name == "_CAST" || func_name == "CAST") && node->children.size() == 2)
                {
                    auto inner = formatNodePretty(node->children[0]);
                    Field type_field;
                    node->children[1]->column->get(0, type_field);
                    return "CAST(" + inner + " AS " + type_field.safeGet<String>() + ")";
                }

                auto op_info = getOperatorInfo(func_name);

                if (func_name == "not" && node->children.size() == 1)
                {
                    String result = "NOT " + formatNodePretty(node->children[0], op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "negate" && node->children.size() == 1)
                {
                    String result = "-" + formatNodePretty(node->children[0], op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "isNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], op_info->precedence) + " IS NULL";

                if (func_name == "isNotNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], op_info->precedence) + " IS NOT NULL";

                if ((func_name == "and" || func_name == "or") && node->children.size() >= 2)
                {
                    String separator = fmt::format(" {} ", op_info->symbol);
                    std::vector<String> parts;
                    parts.reserve(node->children.size());
                    for (const auto * child : node->children)
                        parts.push_back(formatNodePretty(child, op_info->precedence));

                    String result = fmt::format("{}", fmt::join(parts, separator));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "arrayElement" && node->children.size() == 2)
                {
                    auto arr = formatNodePretty(node->children[0], op_info->precedence);
                    auto idx = formatNodePretty(node->children[1]);
                    return arr + "[" + idx + "]";
                }

                if (func_name == "tupleElement" && node->children.size() == 2)
                {
                    auto tup = formatNodePretty(node->children[0], op_info->precedence);
                    auto elem = formatNodePretty(node->children[1]);
                    return tup + "." + elem;
                }

                if (op_info && !op_info->symbol.empty() && node->children.size() == 2)
                {
                    String result = fmt::format("{} {} {}",
                        formatNodePretty(node->children[0], op_info->precedence),
                        op_info->symbol,
                        formatNodePretty(node->children[1], op_info->precedence));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                std::vector<String> args;
                args.reserve(node->children.size());
                for (const auto * child : node->children)
                    args.push_back(formatNodePretty(child));

                return func_name + "(" + fmt::format("{}", fmt::join(args, ", ")) + ")";
            }

            default:
                return node->result_name;
        }
    }

    String formatNamePrettyIfPossible(const ActionsDAG & dag, const String & name)
    {
        const auto * node = dag.tryFindInOutputs(name);
        return node ? QueryPlanFormat::formatNodePretty(node) : name;
    }

    String formatColumnPretty(const String & column_name, const ExplainFormatSettings & settings)
    {
        if (auto it = settings.pretty_names.find(column_name); it != settings.pretty_names.end())
            return it->second;
        return trimColumnIdentifier(column_name);
    }

    static void addAggregatesPrettyNames(const Aggregator::Params & params, std::unordered_map<String, String> & pretty_names)
    {
        for (const auto & agg : params.aggregates)
        {
            String pretty;
            if (agg.function)
                pretty += agg.function->getName();
            pretty += '(';
            bool first = true;
            for (const auto & arg : agg.argument_names)
            {
                if (!first)
                    pretty += ", ";
                first = false;
                if (auto it = pretty_names.find(arg); it != pretty_names.end())
                    pretty += it->second;
                else
                    pretty += trimColumnIdentifier(arg);
            }
            pretty += ')';
            pretty_names[agg.column_name] = std::move(pretty);
        }
    }

    static void buildPrettyNamesForNode(
        const QueryPlan::Node * node,
        std::unordered_map<String, String> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names)
    {
        for (const auto * child : node->children)
            buildPrettyNamesForNode(child, pretty_names, runtime_filter_names);

        const auto & step = node->step;
        const auto & step_name = step->getName();

        if (step_name == "Expression")
        {
            const auto & dag = static_cast<const ExpressionStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = formatNodePretty(output);
        }
        else if (step_name == "Filter")
        {
            const auto & dag = static_cast<const FilterStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = formatNodePretty(output);
        }
        else if (step_name == "Aggregating" || step_name == "AggregatingProjection")
        {
            addAggregatesPrettyNames(static_cast<const AggregatingStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "MergingAggregated")
        {
            addAggregatesPrettyNames(static_cast<const MergingAggregatedStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "Rollup")
        {
            addAggregatesPrettyNames(static_cast<const RollupStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "Cube")
        {
            addAggregatesPrettyNames(static_cast<const CubeStep *>(step.get())->getParams(), pretty_names);
        }
        else if (step_name == "BuildRuntimeFilter")
        {
            const auto * rf_step = static_cast<const BuildRuntimeFilterStep *>(step.get());
            size_t rf_index = runtime_filter_names.size() + 1;
            String pretty = fmt::format("RF{}", rf_index);
            String build_col = trimColumnIdentifier(rf_step->getFilterColumnName());
            runtime_filter_names[rf_step->getFilterName()] = {std::move(pretty), std::move(build_col)};
        }
    }

    void buildPrettyNamesMap(
        const QueryPlan & plan,
        std::unordered_map<String, String> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names)
    {
        if (plan.getRootNode())
            buildPrettyNamesForNode(plan.getRootNode(), pretty_names, runtime_filter_names);
    }

    namespace
    {
        bool isRuntimeFilterNode(const ActionsDAG::Node * node)
        {
            return node->type == ActionsDAG::ActionType::FUNCTION
                && node->function_base
                && node->function_base->getName() == "__applyFilter";
        }

        String getRuntimeFilterId(const ActionsDAG::Node * node)
        {
            Field value;
            node->children[0]->column->get(0, value);
            return value.safeGet<String>();
        }

        struct SplitResult
        {
            ActionsDAG::NodeRawConstPtrs user_atoms;
            ActionsDAG::NodeRawConstPtrs rf_atoms;
        };

        SplitResult splitByRuntimeFilters(const ActionsDAG & dag, const String & column_name)
        {
            SplitResult result;
            const auto * node = dag.tryFindInOutputs(column_name);
            if (!node)
                return result;

            auto atoms = ActionsDAG::extractConjunctionAtoms(node);
            for (const auto * atom : atoms)
            {
                if (isRuntimeFilterNode(atom))
                    result.rf_atoms.push_back(atom);
                else
                    result.user_atoms.push_back(atom);
            }
            return result;
        }

        void writeRuntimeFilters(
            WriteBuffer & out,
            const ActionsDAG::NodeRawConstPtrs & rf_atoms,
            const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
            const String & prefix)
        {
            out << prefix << "Runtime filters: ";
            bool first = true;
            for (const auto * atom : rf_atoms)
            {
                if (!first)
                    out << ", ";
                first = false;

                String filter_id = getRuntimeFilterId(atom);
                String probe_col = trimColumnIdentifier(atom->children[1]->result_name);

                if (auto it = runtime_filter_names.find(filter_id); it != runtime_filter_names.end())
                    out << it->second.pretty_name << ": " << probe_col << " = " << it->second.build_column_name;
                else
                    out << filter_id << ": " << probe_col;
            }
            out << '\n';
        }

        void writeFilterAtoms(
            WriteBuffer & out,
            const ActionsDAG::NodeRawConstPtrs & atoms,
            const String & label,
            const String & prefix)
        {
            out << prefix << label << " column: ";
            for (size_t i = 0; i < atoms.size(); ++i)
            {
                if (i > 0)
                    out << " AND ";
                out << formatNodePretty(atoms[i], 4);
            }
            out << '\n';
        }
    }

    String formatFilterColumn(const ActionsDAG & dag, const String & column_name, bool pretty)
    {
        return pretty ? formatNamePrettyIfPossible(dag, column_name) : column_name;
    }

    const RuntimeFilterInfo * findRuntimeFilter(const String & filter_id, const ExplainFormatSettings & settings)
    {
        if (auto it = settings.runtime_filter_names.find(filter_id); it != settings.runtime_filter_names.end())
            return &it->second;
        return nullptr;
    }

    void describeSourceFilter(
        WriteBuffer & out,
        const String & label,
        const ActionsDAG & dag,
        const String & column_name,
        bool remove_column,
        const ExplainFormatSettings & settings,
        const String & prefix)
    {
        if (settings.pretty)
        {
            auto split = splitByRuntimeFilters(dag, column_name);

            if (!split.user_atoms.empty())
                writeFilterAtoms(out, split.user_atoms, label, prefix);

            if (!split.rf_atoms.empty())
                writeRuntimeFilters(out, split.rf_atoms, settings.runtime_filter_names, prefix);
        }
        else
        {
            out << prefix << label << '\n';
            out << prefix << label << " column: " << column_name;
            if (remove_column)
                out << " (removed)";
            out << '\n';
        }
        
        if (!settings.compact)
        {
            auto expression = std::make_shared<ExpressionActions>(dag.clone());
            expression->describeActions(out, prefix);
        }
    }

}

}
