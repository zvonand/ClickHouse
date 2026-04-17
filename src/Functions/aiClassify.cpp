#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace
{

bool isArrayOfStrings(const IDataType & type)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(&type);
    return (array_type && isString(array_type->getNestedType()));
}

class FunctionAiClassify final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiClassify";

    explicit FunctionAiClassify(ContextPtr context) : FunctionBaseAI(context) {}

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiClassify>(context); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"collection", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), &isColumnConst, "const String"},
            {"text", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"categories", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArrayOfStrings), &isColumnConst, "const Array(String)"},
        };
        FunctionArgumentDescriptors optional_args{
            {"temperature", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), &isColumnConst, "const Number"},
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

private:
    static constexpr float default_temp = 0.0f;
    static constexpr size_t prompt_arg_index = 1;
    static constexpr size_t categories_arg_index = 2;
    static constexpr size_t temp_arg_idx = 3;

    String functionName() const override { return name; }

    float defaultTemperature() const override { return default_temp; }
    size_t promptArgumentIndex() const override { return prompt_arg_index; }
    size_t temperatureArgumentIndex() const override { return temp_arg_idx; }

    Array getCategories(const ColumnsWithTypeAndName & arguments) const
    {
        const auto & col_const = assert_cast<const ColumnConst &>(*arguments[categories_arg_index].column);
        return (*col_const.getDataColumnPtr())[0].safeGet<Array>();
    }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto categories = getCategories(arguments);

        String labels;
        bool first = true;
        for (const auto & elem : categories)
        {
            if (!first)
                labels += ", ";
            first = false;
            labels += elem.safeGet<String>();
        }

        return "You are a text classifier. Classify the given text into exactly one of these categories: "
            + labels
            + ". Respond with ONLY the category label, nothing else.";
    }

    std::string_view buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        return arguments[prompt_arg_index].column->getDataAt(row);
    }

    Poco::JSON::Object::Ptr buildResponseFormat(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto categories = getCategories(arguments);

        Poco::JSON::Array::Ptr enum_array = new Poco::JSON::Array;
        for (const auto & elem : categories)
            enum_array->add(elem.safeGet<String>());

        Poco::JSON::Object::Ptr category_prop = new Poco::JSON::Object;
        category_prop->set("type", "string");
        category_prop->set("enum", enum_array);

        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        properties->set("category", category_prop);

        Poco::JSON::Array::Ptr required = new Poco::JSON::Array;
        required->add("category");

        Poco::JSON::Object::Ptr schema = new Poco::JSON::Object;
        schema->set("type", "object");
        schema->set("properties", properties);
        schema->set("required", required);
        schema->set("additionalProperties", false);

        Poco::JSON::Object::Ptr json_schema = new Poco::JSON::Object;
        json_schema->set("name", "classification");
        json_schema->set("strict", true);
        json_schema->set("schema", schema);

        Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
        root->set("type", "json_schema");
        root->set("json_schema", json_schema);
        return root;
    }

    String postProcessResponse(const String & raw_response) const override
    {
        if (raw_response.empty() || raw_response.front() != '{')
            return raw_response;

        try
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(raw_response);
            auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
            if (obj && obj->has("category"))
                return obj->getValue<String>("category");
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
        return raw_response;
    }
};

}

REGISTER_FUNCTION(AiClassify)
{
    factory.registerFunction<FunctionAiClassify>(FunctionDocumentation{
        .description = R"(
Classifies the given text into one of the provided categories using an LLM provider.

The function sends the text together with a fixed classification prompt and a JSON-schema response format
constraining the model to return exactly one of the supplied labels. When the response is returned as a JSON
object of the form `{"category": "..."}`, the label is unwrapped and the label string is returned.

The first argument is a named collection that specifies the provider, model, endpoint, and API key.
)",
        .syntax = "aiClassify(collection, text, categories[, temperature])",
        .arguments = {
            {"collection", "Name of a named collection containing provider credentials and configuration.", {"String"}},
            {"text", "Text to classify.", {"String"}},
            {"categories", "Constant list of candidate category labels.", {"Array(String)"}},
            {"temperature", "Sampling temperature controlling randomness. Default: `0.0`.", {"Float64"}},
        },
        .returned_value = {"One of the provided category labels, or the default value for the column type (empty string) if the request failed and `ai_function_throw_on_error` is disabled.", {"String"}},
        .examples = {
            {"Classify sentiment", "SELECT aiClassify('ai_credentials', 'I love this product!', ['positive', 'negative', 'neutral'])", "positive"},
            {"Classify a column", "SELECT body, aiClassify('ai_credentials', body, ['bug', 'question', 'feature']) AS kind FROM issues LIMIT 5", ""},
        },
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
