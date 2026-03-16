"""
Tests for Targeting.normalize_symbol — extracts the qualified C++ function name
from a full demangled symbol (with return type and argument list).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.find_tests import Targeting

ns = Targeting.normalize_symbol


# ---------------------------------------------------------------------------
# Basic cases
# ---------------------------------------------------------------------------


def test_no_return_type():
    assert ns("DB::Foo::bar(int)") == "DB::Foo::bar"


def test_void_return_type():
    assert ns("void DB::Foo::bar(int)") == "DB::Foo::bar"


def test_complex_return_type():
    assert (
        ns("std::shared_ptr<DB::Type> DB::Foo::bar()") == "DB::Foo::bar"
    )


def test_no_args():
    assert ns("void DB::Foo::bar()") == "DB::Foo::bar"


def test_const_method():
    assert ns("int DB::Foo::get() const") == "DB::Foo::get"


# ---------------------------------------------------------------------------
# Anonymous namespace
# ---------------------------------------------------------------------------


def test_anonymous_namespace_no_return():
    assert (
        ns("(anonymous namespace)::DistributedIndexAnalyzer::method()")
        == "(anonymous namespace)::DistributedIndexAnalyzer::method"
    )


def test_anonymous_namespace_with_return():
    assert (
        ns("void (anonymous namespace)::Foo::bar()")
        == "(anonymous namespace)::Foo::bar"
    )


# ---------------------------------------------------------------------------
# Function templates
# ---------------------------------------------------------------------------


def test_function_template_stripped():
    # Trailing template args stripped because func ends with '>'
    assert (
        ns("void DB::JoinStuff::JoinUsedFlags::setUsed<true, true>()")
        == "DB::JoinStuff::JoinUsedFlags::setUsed"
    )


def test_class_template_stripped():
    # Class template args stripped so hasAllTokens matches all instantiations
    assert ns("void DB::Foo<int>::bar()") == "DB::Foo::bar"


# ---------------------------------------------------------------------------
# Already-stripped symbols (no arg list)
# ---------------------------------------------------------------------------


def test_no_parens_returned_as_is():
    assert ns("DB::Foo::bar") == "DB::Foo::bar"


def test_empty_string():
    assert ns("") == ""


# ---------------------------------------------------------------------------
# Conversion operators  (the bug reported by clickhouse-gh)
# ---------------------------------------------------------------------------


def test_conversion_operator_simple():
    # "DB::Foo::operator int() const"
    # Before fix: returned "int"
    assert ns("DB::Foo::operator int() const") == "DB::Foo::operator int"


def test_conversion_operator_qualified_type():
    # last_space lands after "std::string" (no space there, actually no space in std::string)
    # but the pattern still works
    assert (
        ns("DB::Foo::operator std::string() const")
        == "DB::Foo::operator std::string"
    )


def test_conversion_operator_multiword_type():
    # Real case from CIDB: last_space lands after "Array", not after "operator"
    # Before fix: returned "const&" (wrong)
    assert (
        ns("AMQP::Field::operator AMQP::Array const&() const")
        == "AMQP::Field::operator AMQP::Array const&"
    )


def test_conversion_operator_multiword_unsigned():
    # "operator unsigned int" has a space inside the target type
    assert (
        ns("AMQP::Field::operator unsigned int() const")
        == "AMQP::Field::operator unsigned int"
    )


def test_conversion_operator_bool():
    # Real case from CIDB: DB::BackgroundSchedulePoolTaskHolder::operator bool
    assert (
        ns("DB::BackgroundSchedulePoolTaskHolder::operator bool() const")
        == "DB::BackgroundSchedulePoolTaskHolder::operator bool"
    )


def test_conversion_operator_class_template():
    # Real case from CIDB: class template before ::operator; template args stripped
    assert (
        ns("AMQP::NumericField<float, (char)102, std::__1::enable_if<true, float>>::operator short() const")
        == "AMQP::NumericField::operator short"
    )


def test_conversion_operator_with_return_type_prefix():
    # Hypothetical: explicit return type prefix (does not occur in practice for conversion
    # operators, but the algorithm should still strip it if present)
    assert (
        ns("int DB::Foo::operator int() const") == "DB::Foo::operator int"
    )


def test_conversion_operator_no_class():
    # Top-level conversion operator (free function; unusual but must not crash)
    assert ns("operator int()") == "operator int"


# ---------------------------------------------------------------------------
# Non-conversion operators  (must NOT be affected by the fix)
# ---------------------------------------------------------------------------


def test_comparison_operator_no_space():
    # operator== has no space — last_space lands before the function, fix not triggered
    assert ns("bool DB::Foo::operator==(DB::Foo const&) const") == "DB::Foo::operator=="


def test_increment_operator():
    assert ns("void DB::Foo::operator++(int)") == "DB::Foo::operator++"


def test_call_operator():
    # operator() — the first '(' at depth 0 IS the '(' of operator() itself,
    # so first_paren lands there and the result is "DB::Foo::operator" (without "()").
    # The "()" tokens are non-alpha and carry no hasAllTokens signal anyway.
    assert ns("bool DB::Foo::operator()(int) const") == "DB::Foo::operator"


# ---------------------------------------------------------------------------
# Should not treat "custom_operator" as a conversion operator
# ---------------------------------------------------------------------------


def test_custom_operator_suffix_ignored():
    # "custom_operator" ends with "operator" but preceded by '_', not '::' or ' '
    # so the fix must NOT be applied; return type "void" is stripped normally
    assert ns("void DB::custom_operator(int)") == "DB::custom_operator"


def test_custom_operator_suffix_with_space():
    # "some_operator" ends with "operator" but preceded by '_'
    assert ns("int DB::some_operator(int)") == "DB::some_operator"


# ---------------------------------------------------------------------------
# Class template + function template  (all template args stripped)
# ---------------------------------------------------------------------------


def test_class_template_plus_function_template():
    # Both class template args and function template args stripped
    assert ns(
        "DB::SortColumnDescription* std::__1::vector<DB::SortColumnDescription,"
        " std::__1::allocator<DB::SortColumnDescription>>::__emplace_back_slow_path"
        "<DB::SortColumnDescription const&>(DB::SortColumnDescription const&)"
    ) == "std::__1::vector::__emplace_back_slow_path"


def test_class_template_method_stripped():
    # Class template args stripped even when method name follows the '>'
    assert ns(
        "DB::(anonymous namespace)::AggregateFunctionUniqUpToVariadic<false, true>"
        "::merge(char*, char const*, DB::Arena*) const"
    ) == "DB::(anonymous namespace)::AggregateFunctionUniqUpToVariadic::merge"


def test_function_template_with_complex_return_type():
    # Return type is itself a template; function is also a template — strip function template
    assert ns(
        "DB::Decimal<wide::integer<256ul, int>> DB::(anonymous namespace)"
        "::DivideDecimalsImpl::execute<DB::Decimal<wide::integer<128ul, int>>,"
        " DB::Decimal<wide::integer<128ul, int>>>(DB::Decimal<wide::integer<128ul, int>>,"
        " DB::Decimal<wide::integer<128ul, int>>, unsigned short, unsigned short, unsigned short)"
    ) == "DB::(anonymous namespace)::DivideDecimalsImpl::execute"


# ---------------------------------------------------------------------------
# Constructors and destructors
# ---------------------------------------------------------------------------


def test_copy_constructor():
    assert (
        ns("DB::DeduplicationHash::DeduplicationHash(DB::DeduplicationHash const&)")
        == "DB::DeduplicationHash::DeduplicationHash"
    )


# ---------------------------------------------------------------------------
# Exception constructor with variadic template args
# ---------------------------------------------------------------------------


def test_exception_constructor_variadic_template():
    # Variadic template constructor — function template args stripped
    assert ns(
        "DB::Exception::Exception<char const*, std::__1::basic_string<char,"
        " std::__1::char_traits<char>, std::__1::allocator<char>>, unsigned long&,"
        " std::__1::basic_string<char, std::__1::char_traits<char>,"
        " std::__1::allocator<char>>>(int, FormatStringHelperImpl<"
        "std::__1::type_identity<char const*>::type>, char const*&&,"
        " std::__1::basic_string<char>&&)"
    ) == "DB::Exception::Exception"


# ---------------------------------------------------------------------------
# Simple real-world cases (regression coverage)
# ---------------------------------------------------------------------------


def test_simple_no_args():
    assert ns("DB::RemoveRecursiveOperation::finalize()") == "DB::RemoveRecursiveOperation::finalize"


def test_simple_bool_arg():
    assert ns("DB::IAST::getTreeHash(bool) const") == "DB::IAST::getTreeHash"


def test_settings_subscript_operator():
    # operator[] is not a conversion operator — no space after "operator"
    assert (
        ns(
            "DB::MergeTreeSettings::operator[](DB::SettingFieldEnum<DB::MergeSelectorAlgorithm,"
            " DB::SettingFieldMergeSelectorAlgorithmTraits> DB::MergeTreeSettingsImpl::*) const"
        )
        == "DB::MergeTreeSettings::operator[]"
    )


# ---------------------------------------------------------------------------
# Destructor
# ---------------------------------------------------------------------------


def test_destructor():
    assert ns("DB::Foo::~Foo()") == "DB::Foo::~Foo"


def test_destructor_noexcept():
    assert ns("DB::Foo::~Foo() noexcept") == "DB::Foo::~Foo"


# ---------------------------------------------------------------------------
# Operators that contain < or > (depth-tracking risk)
# ---------------------------------------------------------------------------


def test_operator_shift_left():
    assert (
        ns("std::ostream& DB::Foo::operator<<(std::ostream&)")
        == "DB::Foo::operator<<"
    )


def test_operator_shift_right():
    assert ns("DB::Bar DB::Foo::operator>>(int)") == "DB::Foo::operator>>"


def test_operator_arrow():
    assert ns("DB::Foo* DB::Foo::operator->()") == "DB::Foo::operator->"


# ---------------------------------------------------------------------------
# operator new / operator delete
# ---------------------------------------------------------------------------


def test_operator_new():
    assert (
        ns("void* DB::Foo::operator new(unsigned long)")
        == "DB::Foo::operator new"
    )


def test_operator_delete():
    assert (
        ns("void DB::Foo::operator delete(void*)")
        == "DB::Foo::operator delete"
    )


# ---------------------------------------------------------------------------
# Free functions (no class)
# ---------------------------------------------------------------------------


def test_free_function_void_return():
    assert ns("void freeFunc(int)") == "freeFunc"


def test_free_function_int_return():
    assert ns("int topLevelHelper()") == "topLevelHelper"


# ---------------------------------------------------------------------------
# Nested anonymous namespaces
# ---------------------------------------------------------------------------


def test_nested_anonymous_namespace():
    assert (
        ns(
            "void DB::(anonymous namespace)::(anonymous namespace)::Helper::run()"
        )
        == "DB::(anonymous namespace)::(anonymous namespace)::Helper::run"
    )


def test_function_template_in_anonymous_namespace():
    assert (
        ns("void DB::(anonymous namespace)::apply<true>(int)")
        == "DB::(anonymous namespace)::apply"
    )


# ---------------------------------------------------------------------------
# Pointer / reference return types
# ---------------------------------------------------------------------------


def test_pointer_return_type():
    assert ns("char const* DB::Foo::data() const") == "DB::Foo::data"


def test_reference_return_type():
    assert (
        ns("DB::Foo& DB::Foo::operator=(DB::Foo const&)") == "DB::Foo::operator="
    )


def test_deeply_nested_template_return_type():
    assert (
        ns("std::optional<std::vector<DB::Block>> DB::Foo::getBlocks() const")
        == "DB::Foo::getBlocks"
    )


# ---------------------------------------------------------------------------
# volatile qualifier
# ---------------------------------------------------------------------------


def test_volatile_method():
    assert ns("void DB::Foo::bar() volatile") == "DB::Foo::bar"


# ---------------------------------------------------------------------------
# operator[] with template return type (not a conversion operator)
# ---------------------------------------------------------------------------


def test_subscript_operator_with_template_return():
    assert (
        ns("DB::Column& DB::Block::operator[](unsigned long)")
        == "DB::Block::operator[]"
    )


# ---------------------------------------------------------------------------
# Cast expressions inside template args  e.g. (char8_t)15, (SomeEnum)1
# The '(' in the cast is at depth > 0 during the arg-list scan (steps 1-2)
# so it must not be mistaken for the arg-list opener.
# After the scan, all template args (including the casts) are stripped.
# ---------------------------------------------------------------------------


def test_integer_cast_in_template_arg():
    # (char8_t)15 inside class template — template args stripped
    assert (
        ns(
            "DB::AggregateFunctionUniqCombined<char8_t, (char8_t)15, unsigned long>"
            "::add(char*, DB::IColumn const**, unsigned long, DB::Arena*) const"
        )
        == "DB::AggregateFunctionUniqCombined::add"
    )


def test_enum_cast_in_template_arg_destructor():
    # (DB::DictionaryKeyType)1 inside class template, also a destructor
    assert (
        ns("DB::HashedArrayDictionary<(DB::DictionaryKeyType)1, false>::~HashedArrayDictionary()")
        == "DB::HashedArrayDictionary::~HashedArrayDictionary"
    )


def test_enum_cast_in_template_arg_no_args():
    # (DB::IPStringToNumExceptionMode)2 in template, no-arg method
    assert (
        ns(
            "DB::FunctionIPv6StringToNum<(DB::IPStringToNumExceptionMode)2>"
            "::useDefaultImplementationForNulls() const"
        )
        == "DB::FunctionIPv6StringToNum::useDefaultImplementationForNulls"
    )


def test_multiple_enum_casts_in_template_args():
    # Three enum casts inside one class template — all stripped
    assert (
        ns(
            "DB::AggregateFunctionUniqCombinedVariadic<false, false, (char8_t)16, unsigned long>"
            "::merge(char*, char const*, DB::Arena*) const"
        )
        == "DB::AggregateFunctionUniqCombinedVariadic::merge"
    )


# ---------------------------------------------------------------------------
# _BitInt type  — has its own '(N)' suffix that must not confuse the scanner
# ---------------------------------------------------------------------------


def test_bitint_in_template_arg():
    # _BitInt(8) has its own '(N)' suffix; must not be mistaken for arg-list start
    assert (
        ns(
            "DB::AggregateFunctionQuantile<_BitInt(8), DB::QuantileExactInclusive<_BitInt(8)>,"
            " DB::NameQuantilesExactInclusive, void, double, true, false>"
            "::serialize(char const*, DB::WriteBuffer&, std::__1::optional<unsigned long>) const"
        )
        == "DB::AggregateFunctionQuantile::serialize"
    )


# ---------------------------------------------------------------------------
# Non-DB return type + class-template + function-template + anon namespace
# in the class template args (all four at once)
# ---------------------------------------------------------------------------


def test_non_db_return_type_class_plus_func_template_anon_ns():
    # Non-DB return type, class template with anon-namespace args, function template —
    # all template args stripped
    assert (
        ns(
            "wide::integer<128ul, unsigned int>"
            " DB::GCDLCMImpl<char8_t, wide::integer<128ul, unsigned int>,"
            " DB::(anonymous namespace)::GCDImpl<char8_t, wide::integer<128ul, unsigned int>>,"
            " DB::(anonymous namespace)::NameGCD>"
            "::apply<wide::integer<128ul, unsigned int>>"
            "(char8_t, wide::integer<128ul, unsigned int>)"
        )
        == "DB::GCDLCMImpl::apply"
    )


# ---------------------------------------------------------------------------
# Deeply nested class template with anon-namespace inside, no return type
# ---------------------------------------------------------------------------


def test_shared_ptr_emplace_deep_template():
    # Deeply nested class template — all template args stripped
    assert (
        ns(
            "std::__1::__shared_ptr_emplace<"
            "DB::AggregateFunctionQuantile<"
            "DB::Decimal<wide::integer<128ul, int>>,"
            " DB::(anonymous namespace)::QuantileReservoirSampler<DB::Decimal<wide::integer<128ul, int>>>,"
            " DB::NameQuantile, void, void, false, false>,"
            " std::__1::allocator<"
            "DB::AggregateFunctionQuantile<"
            "DB::Decimal<wide::integer<128ul, int>>,"
            " DB::(anonymous namespace)::QuantileReservoirSampler<DB::Decimal<wide::integer<128ul, int>>>,"
            " DB::NameQuantile, void, void, false, false"
            ">>>::__on_zero_shared_weak()"
        )
        == "std::__1::__shared_ptr_emplace::__on_zero_shared_weak"
    )


# ---------------------------------------------------------------------------
# std::function<void (char*&)> inside arg list  — '(' inside function-type
# template arg must not confuse the scanner (it's after first_paren anyway)
# ---------------------------------------------------------------------------


def test_std_function_in_arg_list():
    # std::function<void (char*&)> has '(' inside template in the arg list;
    # the arg list is after first_paren so it never confuses the scanner.
    # Class template args on IAggregateFunctionHelper are stripped.
    assert (
        ns(
            "DB::IAggregateFunctionHelper<DB::(anonymous namespace)::AggregateFunctionAny<"
            "DB::SingleValueDataFixed<DB::Decimal<wide::integer<128ul, int>>>>>"
            "::addBatchLookupTable8(unsigned long, unsigned long, char**, unsigned long,"
            " std::__1::function<void (char*&)>,"
            " char8_t const*, DB::IColumn const**, DB::Arena*) const"
        )
        == "DB::IAggregateFunctionHelper::addBatchLookupTable8"
    )


# ---------------------------------------------------------------------------
# Deeply nested template args in both class and arg list (NumComparisonImpl)
# ---------------------------------------------------------------------------


def test_complex_class_template_method():
    # Class template with deeply nested args + complex PODArray arg list — all stripped
    assert (
        ns(
            "DB::NumComparisonImpl<wide::integer<256ul, int>, unsigned short,"
            " DB::NotEqualsOp<wide::integer<256ul, int>, unsigned short>>"
            "::vectorConstantImplAVX512F("
            "DB::PODArray<wide::integer<256ul, int>, 4096ul, Allocator<false, false>, 63ul, 64ul> const&,"
            " unsigned short,"
            " DB::PODArray<char8_t, 4096ul, Allocator<false, false>, 63ul, 64ul>&)"
        )
        == "DB::NumComparisonImpl::vectorConstantImplAVX512F"
    )


# ---------------------------------------------------------------------------
# Clang ABI tags  [abi:fe210105] / [abi:ne210105]
# These appear as a suffix on the function name; they must be preserved since
# they disambiguate overloads and are part of the linkage name.
# ---------------------------------------------------------------------------


def test_abi_tag_on_destructor():
    # Class template stripped, ABI tag retained on destructor
    assert (
        ns("std::__1::shared_ptr<DB::DatabaseFilesystem>::~shared_ptr[abi:ne210105]()")
        == "std::__1::shared_ptr::~shared_ptr[abi:ne210105]"
    )


def test_abi_tag_on_regular_method():
    assert (
        ns("std::__1::vector<int>::push_back[abi:fe210105](int&&)")
        == "std::__1::vector::push_back[abi:fe210105]"
    )


# ---------------------------------------------------------------------------
# Top-level (anonymous namespace) without DB::  (e.g. LLVM helper classes)
# ---------------------------------------------------------------------------


def test_top_level_anonymous_namespace_no_db():
    assert (
        ns("(anonymous namespace)::AAInstanceInfoImpl::initialize(llvm::Attributor&)")
        == "(anonymous namespace)::AAInstanceInfoImpl::initialize"
    )


# ---------------------------------------------------------------------------
# decltype(auto) return type — known limitation
# The '(' in 'decltype(' is at bracket depth 0 so it is treated as the
# arg-list opener; the result is the bare token 'decltype'.
# This only affects STL variant/tuple dispatch internals which are never
# queried from DWARF results for changed ClickHouse source files.
# ---------------------------------------------------------------------------


def test_decltype_auto_return_known_limitation():
    assert ns(
        "decltype(auto) std::__1::__variant_detail::__visitation::__base"
        "::__dispatcher<6ul>::__dispatch[abi:ne210105]<int>(int)"
    ) == "decltype"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"FAIL  {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        sys.exit(1)
