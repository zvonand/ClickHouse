#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct ITokenizer;

class ExecutableFunctionMatchPhrase : public IExecutableFunction
{
public:
    static constexpr auto name = "matchPhrase";

    explicit ExecutableFunctionMatchPhrase(
        std::shared_ptr<const ITokenizer> tokenizer_, std::vector<String> phrase_tokens_)
        : tokenizer(std::move(tokenizer_))
        , phrase_tokens(std::move(phrase_tokens_))
    {
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    std::vector<String> phrase_tokens;
};

class FunctionBaseMatchPhrase : public IFunctionBase
{
public:
    static constexpr auto name = "matchPhrase";

    FunctionBaseMatchPhrase(
        std::shared_ptr<const ITokenizer> tokenizer_,
        std::vector<String> phrase_tokens_,
        DataTypes argument_types_,
        DataTypePtr result_type_)
        : tokenizer(std::move(tokenizer_))
        , phrase_tokens(std::move(phrase_tokens_))
        , argument_types(std::move(argument_types_))
        , result_type(std::move(result_type_))
    {
    }

    String getName() const override { return name; }
    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return result_type; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override;

private:
    std::shared_ptr<const ITokenizer> tokenizer;
    std::vector<String> phrase_tokens;
    DataTypes argument_types;
    DataTypePtr result_type;
};

class FunctionMatchPhraseOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "matchPhrase";

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_unique<FunctionMatchPhraseOverloadResolver>(context);
    }

    explicit FunctionMatchPhraseOverloadResolver(ContextPtr context);

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override;
};

}
