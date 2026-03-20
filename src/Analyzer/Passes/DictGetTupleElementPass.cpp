#include <Analyzer/Passes/DictGetTupleElementPass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_dictget_tuple_element;
}

namespace
{

/** Extract the element index (0-based) from a tupleElement call.
  * Supports both positional (1-based UInt64) and named (String) access.
  * Returns std::nullopt if not determinable at analysis time.
  */
std::optional<size_t> getTupleElementIndex(const ConstantNode & index_node, const Strings & attribute_names)
{
    Field value = index_node.getValue();

    if (value.getType() == Field::Types::String)
    {
        /// Named access: dictGet returns a named tuple, so `.country` becomes tupleElement(..., 'country')
        const auto & name = value.safeGet<String>();
        for (size_t i = 0; i < attribute_names.size(); ++i)
        {
            if (attribute_names[i] == name)
                return i;
        }
        return std::nullopt;
    }

    if (value.getType() == Field::Types::UInt64)
    {
        UInt64 idx = value.safeGet<UInt64>();
        if (idx >= 1 && idx <= attribute_names.size())
            return static_cast<size_t>(idx - 1);
    }

    return std::nullopt;
}

/** Try to extract attribute names from the second argument of dictGet/dictGetOrDefault.
  * Returns empty vector if not a constant tuple of strings.
  */
Strings extractAttributeNames(const QueryTreeNodePtr & arg)
{
    Strings result;

    if (const auto * constant_node = arg->as<ConstantNode>())
    {
        Field value = constant_node->getValue();

        if (value.getType() == Field::Types::Tuple)
        {
            const auto & tuple = value.safeGet<Tuple>();
            for (const auto & elem : tuple)
            {
                if (elem.getType() != Field::Types::String)
                    return {};
                result.push_back(elem.safeGet<String>());
            }
            return result;
        }
    }
    else if (const auto * function_node = arg->as<FunctionNode>())
    {
        if (function_node->getFunctionName() == "tuple")
        {
            for (const auto & child : function_node->getArguments().getNodes())
            {
                const auto * child_constant = child->as<ConstantNode>();
                if (!child_constant)
                    return {};

                Field value = child_constant->getValue();
                if (value.getType() != Field::Types::String)
                    return {};

                result.push_back(value.safeGet<String>());
            }
            return result;
        }
    }

    return {};
}


class DictGetTupleElementVisitor : public InDepthQueryTreeVisitorWithContext<DictGetTupleElementVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DictGetTupleElementVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_dictget_tuple_element])
            return;

        auto * tuple_element_function = node->as<FunctionNode>();
        if (!tuple_element_function || tuple_element_function->getFunctionName() != "tupleElement")
            return;

        auto & tuple_element_args = tuple_element_function->getArguments().getNodes();
        if (tuple_element_args.size() != 2)
            return;

        auto * dict_get_function = tuple_element_args[0]->as<FunctionNode>();
        if (!dict_get_function)
            return;

        const auto & dict_get_name = dict_get_function->getFunctionName();
        bool is_dict_get = (dict_get_name == "dictGet");
        bool is_dict_get_or_default = (dict_get_name == "dictGetOrDefault");

        if (!is_dict_get && !is_dict_get_or_default)
            return;

        auto & dict_get_args = dict_get_function->getArguments().getNodes();

        /// dictGet has at least 3 args: dict_name, attr_names, key_expr [, ...]
        /// dictGetOrDefault has at least 4: dict_name, attr_names, key_expr, default_value [, ...]
        if (dict_get_args.size() < 3)
            return;

        /// Extract attribute names from the second argument
        Strings attribute_names = extractAttributeNames(dict_get_args[1]);
        if (attribute_names.size() < 2)
            return;

        /// Get the tuple element index
        const auto * index_node = tuple_element_args[1]->as<ConstantNode>();
        if (!index_node)
            return;

        auto maybe_index = getTupleElementIndex(*index_node, attribute_names);
        if (!maybe_index)
            return;

        size_t element_index = *maybe_index;

        /// Replace the tuple of attribute names with a single attribute name
        dict_get_args[1] = std::make_shared<ConstantNode>(attribute_names[element_index]);

        /// For dictGetOrDefault, the default value argument is the last one.
        /// If it's a tuple, extract the corresponding element.
        if (is_dict_get_or_default)
        {
            size_t default_arg_idx = dict_get_args.size() - 1;
            auto & default_arg = dict_get_args[default_arg_idx];

            if (const auto * default_constant = default_arg->as<ConstantNode>())
            {
                Field default_value = default_constant->getValue();
                if (default_value.getType() == Field::Types::Tuple)
                {
                    const auto & default_tuple = default_value.safeGet<Tuple>();
                    if (element_index < default_tuple.size())
                        dict_get_args[default_arg_idx] = std::make_shared<ConstantNode>(default_tuple[element_index]);
                    else
                        return; /// Cannot optimize — index out of range
                }
            }
            else if (auto * default_function = default_arg->as<FunctionNode>())
            {
                if (default_function->getFunctionName() == "tuple")
                {
                    auto & default_tuple_args = default_function->getArguments().getNodes();
                    if (element_index < default_tuple_args.size())
                        dict_get_args[default_arg_idx] = default_tuple_args[element_index];
                    else
                        return; /// Cannot optimize — index out of range
                }
            }
        }

        /// Re-resolve the dictGet function with the modified arguments
        resolveOrdinaryFunctionNodeByName(*dict_get_function, dict_get_name, getContext());

        /// Replace the tupleElement node with the modified dictGet node
        node = std::move(tuple_element_args[0]);
    }
};

}

void DictGetTupleElementPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DictGetTupleElementVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
