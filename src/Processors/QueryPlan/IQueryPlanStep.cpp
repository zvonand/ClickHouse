#include <optional>
#include <string_view>
#include <Columns/IColumn.h>
#include <Common/CurrentThread.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace QueryPlanFormat
{
    std::string_view trimColumnIdentifier(std::string_view name)
    {
        if (name.find("__table") == std::string_view::npos)
            return name;

        auto dot_pos = name.rfind('.');
        if (dot_pos != std::string_view::npos)
            return name.substr(dot_pos + 1);

        return name;
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

        std::vector<std::string_view> left_columns;
        std::vector<std::string_view> right_columns;

        for (const auto & col : output)
        {
            auto trimmed = trimColumnIdentifier(col.name);
            if (left_input.has(col.name))
                left_columns.push_back(trimmed);
            else if (right_input.has(col.name))
                right_columns.push_back(trimmed);
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
            if (func_name == "greaterOrEquals")   return OperatorInfo{">=", 9};
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
                return String(trimColumnIdentifier(node->result_name));

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

    String formatColumnForExplain(const String & column_name, const IQueryPlanStep::FormatSettings & settings)
    {
        if (settings.pretty)
        {
            for (const auto * dag : settings.input_dags)
            {
                if (const auto * node = dag->tryFindInOutputs(column_name))
                    return formatNodePretty(node);
            }
            return String(trimColumnIdentifier(column_name));
        }
        return column_name;
    }
}

IQueryPlanStep::IQueryPlanStep()
{
    step_index = CurrentThread::isInitialized() ? CurrentThread::get().getNextPlanStepIndex() : 0;
}

void IQueryPlanStep::updateInputHeaders(SharedHeaders input_headers_)
{
    input_headers = std::move(input_headers_);
    updateOutputHeader();
}

void IQueryPlanStep::updateInputHeader(SharedHeader input_header, size_t idx)
{
    if (idx >= input_headers.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot update input header {} for step {} because it has only {} headers",
            idx, getName(), input_headers.size());

    input_headers[idx] = input_header;
    updateOutputHeader();
}

void IQueryPlanStep::setRuntimeDataflowStatisticsCacheUpdater(RuntimeDataflowStatisticsCacheUpdaterPtr updater)
{
    if (!supportsDataflowStatisticsCollection())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Step {} doesn't support dataflow statistics collection", getName());
    dataflow_cache_updater = std::move(updater);
}

IQueryPlanStep::RemovedUnusedColumns IQueryPlanStep::removeUnusedColumns(NameMultiSet /*required_outputs*/, bool /*remove_inputs*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "removeUnusedColumns is not implemented for step {}", getName());
}

bool IQueryPlanStep::canRemoveColumnsFromOutput() const
{
    return false;
}

bool IQueryPlanStep::hasCorrelatedExpressions() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot check {} plan step for correlated expressions", getName());
}

const SharedHeader & IQueryPlanStep::getOutputHeader() const
{
    if (!hasOutputHeader())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "QueryPlanStep {} does not have output stream.", getName());

    return output_header;
}

std::string_view IQueryPlanStep::getStepDescription() const
{
    if (std::holds_alternative<std::string_view>(step_description))
        return std::get<std::string_view>(step_description);
    if (std::holds_alternative<std::string>(step_description))
        return std::get<std::string>(step_description);

    return {};
}

void IQueryPlanStep::setStepDescription(std::string description, size_t limit)
{
    if (description.size() > limit)
    {
        description.resize(limit);
        description.shrink_to_fit();
    }

    step_description = std::move(description);
}

void IQueryPlanStep::setStepDescription(const IQueryPlanStep & step)
{
    step_description = step.step_description;
}

QueryPlanStepPtr IQueryPlanStep::clone() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot clone {} plan step", getName());
}

const SortDescription & IQueryPlanStep::getSortDescription() const
{
    static SortDescription empty;
    return empty;
}

static void doDescribeHeader(const Block & header, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    String prefix(settings.offset, settings.indent_char);
    prefix += "Header";

    if (count > 1)
        prefix += " × " + std::to_string(count) + " ";

    prefix += ": ";

    settings.out << prefix;

    if (header.empty())
    {
        settings.out << " empty\n";
        return;
    }

    prefix.assign(prefix.size(), settings.indent_char);
    bool first = true;

    for (const auto & elem : header)
    {
        if (!first)
            settings.out << prefix;

        first = false;
        elem.dumpNameAndType(settings.out);
        settings.out << ": ";
        elem.dumpStructure(settings.out);
        settings.out << '\n';
    }
}

static void doDescribeProcessor(const IProcessor & processor, size_t count, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << processor.getName();
    if (count > 1)
        settings.out << " × " << std::to_string(count);

    size_t num_inputs = processor.getInputs().size();
    size_t num_outputs = processor.getOutputs().size();
    if (num_inputs != 1 || num_outputs != 1)
        settings.out << " " << std::to_string(num_inputs) << " → " << std::to_string(num_outputs);

    settings.out << '\n';

    if (settings.write_header)
    {
        const Block * last_header = nullptr;
        size_t num_equal_headers = 0;

        for (const auto & port : processor.getOutputs())
        {
            if (last_header && !blocksHaveEqualStructure(*last_header, port.getHeader()))
            {
                doDescribeHeader(*last_header, num_equal_headers, settings);
                num_equal_headers = 0;
            }

            ++num_equal_headers;
            last_header = &port.getHeader();
        }

        if (last_header)
            doDescribeHeader(*last_header, num_equal_headers, settings);
    }

    if (!processor.getDescription().empty())
        settings.out << String(settings.offset, settings.indent_char) << "Description: " << processor.getDescription() << '\n';

    settings.offset += settings.base_indent;
}

void IQueryPlanStep::describePipeline(const Processors & processors, FormatSettings & settings)
{
    const IProcessor * prev = nullptr;
    size_t count = 0;

    for (auto it = processors.rbegin(); it != processors.rend(); ++it)
    {
        if (prev && prev->getName() != (*it)->getName())
        {
            doDescribeProcessor(*prev, count, settings);
            count = 0;
        }

        ++count;
        prev = it->get();
    }

    if (prev)
        doDescribeProcessor(*prev, count, settings);
}

void IQueryPlanStep::appendExtraProcessors(const Processors & extra_processors)
{
    processors.insert(processors.end(), extra_processors.begin(), extra_processors.end());
}

String IQueryPlanStep::getUniqID() const
{
    return fmt::format("{}_{}", getName(), step_index);
}

void IQueryPlanStep::serialize(Serialization & /*ctx*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serialize is not implemented for {}", getName());
}

void IQueryPlanStep::updateOutputHeader() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented"); }

}
