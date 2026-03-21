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
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>

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

    PrettyColumnName formatFilterPretty(
        const ActionsDAG & dag,
        const String & column_name,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names)
    {
        const auto * root = dag.tryFindInOutputs(column_name);
        if (!root)
            return PrettyColumnName(trimColumnIdentifier(column_name));

        auto atoms = ActionsDAG::extractConjunctionAtoms(root);

        std::vector<String> user_parts;
        std::vector<String> rf_parts;
        for (const auto * atom : atoms)
        {
            if (atom->type == ActionsDAG::ActionType::FUNCTION
                && atom->function_base
                && atom->function_base->getName() == "__applyFilter")
                rf_parts.push_back(formatNodePretty(atom, runtime_filter_names, 4));
            else
                user_parts.push_back(formatNodePretty(atom, runtime_filter_names, 4));
        }

        String expression;
        if (!user_parts.empty())
            expression = fmt::format(" {}", fmt::join(user_parts, " AND "));
        if (expression.empty() && rf_parts.empty())
            expression = fmt::format(" {}", trimColumnIdentifier(column_name));

        String annotation;
        if (!rf_parts.empty())
            annotation = fmt::format("Runtime filters: {}", fmt::join(rf_parts, " AND "));

        return {std::move(expression), std::move(annotation)};
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

        String getRuntimeFilterId(const ActionsDAG::Node * node)
        {
            Field value;
            node->children[0]->column->get(0, value);
            return value.safeGet<String>();
        }
    }

    String formatNodePretty(
        const ActionsDAG::Node * node,
        const std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names,
        int parent_precedence)
    {
        using ActionType = ActionsDAG::ActionType;

        switch (node->type)
        {
            case ActionType::INPUT:
                return trimColumnIdentifier(node->result_name);

            case ActionType::COLUMN:
                return formatConstant(node);

            case ActionType::ALIAS:
                return formatNodePretty(node->children.front(), runtime_filter_names, parent_precedence);

            case ActionType::ARRAY_JOIN:
                return "arrayJoin(" + formatNodePretty(node->children.front(), runtime_filter_names) + ")";

            case ActionType::FUNCTION:
            {
                auto func_name = node->function_base->getName();

                if (func_name == "__applyFilter")
                {
                    String filter_id = getRuntimeFilterId(node);
                    String probe_column = trimColumnIdentifier(node->children[1]->result_name);
                    if (auto it = runtime_filter_names.find(filter_id); it != runtime_filter_names.end())
                    {
                        const auto & pretty_filter_name = it->second.pretty_name;
                        const auto & build_column = it->second.build_column_name;
                        return fmt::format("{}({}, {})", pretty_filter_name, probe_column, build_column);
                    }
                    return fmt::format("{}({})", filter_id, probe_column);
                }

                if ((func_name == "_CAST" || func_name == "CAST") && node->children.size() == 2)
                {
                    auto inner = formatNodePretty(node->children[0], runtime_filter_names);
                    Field type_field;
                    node->children[1]->column->get(0, type_field);
                    return "CAST(" + inner + " AS " + type_field.safeGet<String>() + ")";
                }

                auto op_info = getOperatorInfo(func_name);

                if (func_name == "not" && node->children.size() == 1)
                {
                    String result = "NOT " + formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "negate" && node->children.size() == 1)
                {
                    String result = "-" + formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence);
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "isNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence) + " IS NULL";

                if (func_name == "isNotNull" && node->children.size() == 1)
                    return formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence) + " IS NOT NULL";

                if ((func_name == "and" || func_name == "or") && node->children.size() >= 2)
                {
                    String separator = fmt::format(" {} ", op_info->symbol);
                    std::vector<String> parts;
                    parts.reserve(node->children.size());
                    for (const auto * child : node->children)
                        parts.push_back(formatNodePretty(child, runtime_filter_names, op_info->precedence));

                    String result = fmt::format("{}", fmt::join(parts, separator));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                if (func_name == "arrayElement" && node->children.size() == 2)
                {
                    auto arr = formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence);
                    auto idx = formatNodePretty(node->children[1], runtime_filter_names);
                    return arr + "[" + idx + "]";
                }

                if (func_name == "tupleElement" && node->children.size() == 2)
                {
                    auto tup = formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence);
                    auto elem = formatNodePretty(node->children[1], runtime_filter_names);
                    return tup + "." + elem;
                }

                if (op_info && !op_info->symbol.empty() && node->children.size() == 2)
                {
                    String result = fmt::format("{} {} {}",
                        formatNodePretty(node->children[0], runtime_filter_names, op_info->precedence),
                        op_info->symbol,
                        formatNodePretty(node->children[1], runtime_filter_names, op_info->precedence));
                    if (op_info->precedence < parent_precedence)
                        result = "(" + std::move(result) + ")";
                    return result;
                }

                std::vector<String> args;
                args.reserve(node->children.size());
                for (const auto * child : node->children)
                    args.push_back(formatNodePretty(child, runtime_filter_names));

                return func_name + "(" + fmt::format("{}", fmt::join(args, ", ")) + ")";
            }

            default:
                return node->result_name;
        }
    }

    String formatColumnPretty(const String & column_name, const ExplainFormatSettings & settings)
    {
        if (auto it = settings.pretty_names.find(column_name); it != settings.pretty_names.end())
            return it->second.expression;
        return trimColumnIdentifier(column_name);
    }

    std::string_view getColumnAnnotation(const String & column_name, const ExplainFormatSettings & settings)
    {
        if (auto it = settings.pretty_names.find(column_name); it != settings.pretty_names.end())
            return it->second.annotation;
        return {};
    }

    static void addAggregatesPrettyNames(const Aggregator::Params & params, std::unordered_map<String, PrettyColumnName> & pretty_names)
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
                    pretty += it->second.expression;
                else
                    pretty += trimColumnIdentifier(arg);
            }
            pretty += ')';
            pretty_names[agg.column_name] = PrettyColumnName(std::move(pretty));
        }
    }

    static void buildPrettyNamesForNode(
        const QueryPlan::Node * node,
        std::unordered_map<String, PrettyColumnName> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names)
    {
        for (auto it = node->children.rbegin(); it != node->children.rend(); ++it)
            buildPrettyNamesForNode(*it, pretty_names, runtime_filter_names);

        const auto & step = node->step;
        const auto & step_name = step->getName();

        if (step_name == "Expression")
        {
            const auto & dag = static_cast<const ExpressionStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, runtime_filter_names));
        }
        else if (step_name == "Filter")
        {
            const auto & dag = static_cast<const FilterStep *>(step.get())->getExpression();
            for (const auto * output : dag.getOutputs())
                if (output->type != ActionsDAG::ActionType::INPUT)
                    pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, runtime_filter_names));
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
        else if (step_name == "TotalsHaving")
        {
            const auto * having_step = static_cast<const TotalsHavingStep *>(step.get());
            if (const auto * dag = having_step->getActions())
            {
                for (const auto * output : dag->getOutputs())
                    if (output->type != ActionsDAG::ActionType::INPUT)
                        pretty_names[output->result_name] = PrettyColumnName(formatNodePretty(output, runtime_filter_names));
            }
        }
        else if (step_name == "BuildRuntimeFilter")
        {
            const auto * rf_step = static_cast<const BuildRuntimeFilterStep *>(step.get());
            String pretty_name = fmt::format("RF{}", runtime_filter_names.size() + 1);
            String build_column = trimColumnIdentifier(rf_step->getFilterColumnName());
            runtime_filter_names.try_emplace(rf_step->getFilterName(), RuntimeFilterInfo{std::move(pretty_name), std::move(build_column)});
        }

        if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(step.get()))
        {
            if (auto prewhere = source->getPrewhereInfo())
            {
                pretty_names[prewhere->prewhere_column_name] = formatFilterPretty(
                    prewhere->prewhere_actions,
                    prewhere->prewhere_column_name,
                    runtime_filter_names);
            }
            if (auto row_level = source->getRowLevelFilter())
            {
                pretty_names[row_level->column_name] = formatFilterPretty(
                    row_level->actions,
                    row_level->column_name,
                    runtime_filter_names);
            }

            if (step_name == "ReadFromMergeTree")
            {
                const auto * read_from_merge_tree_step = static_cast<const ReadFromMergeTree *>(step.get());
                if (auto deferred_row_level_filter = read_from_merge_tree_step->getDeferredRowLevelFilter())
                {
                    pretty_names[deferred_row_level_filter->column_name] = formatFilterPretty(
                        deferred_row_level_filter->actions,
                        deferred_row_level_filter->column_name,
                        runtime_filter_names);
                }
                if (auto deferred_prewhere = read_from_merge_tree_step->getDeferredPrewhereInfo())
                {
                    pretty_names[deferred_prewhere->prewhere_column_name] = formatFilterPretty(
                        deferred_prewhere->prewhere_actions,
                        deferred_prewhere->prewhere_column_name,
                        runtime_filter_names);
                }
            }
        }
    }

    void buildPrettyNamesMap(
        const QueryPlan & plan,
        std::unordered_map<String, PrettyColumnName> & pretty_names,
        std::unordered_map<String, RuntimeFilterInfo> & runtime_filter_names)
    {
        if (plan.getRootNode())
            buildPrettyNamesForNode(plan.getRootNode(), pretty_names, runtime_filter_names);
    }

}

}
