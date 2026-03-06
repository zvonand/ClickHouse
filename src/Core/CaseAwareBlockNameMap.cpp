#include <Core/CaseAwareBlockNameMap.h>

#include <cctype>
#include <memory>
#include <unordered_map>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <base/StringViewHash.h>
#include <sparsehash/dense_hash_map>
#include "Common/Exception.h"

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int LOGICAL_ERROR;
}

void ICaseAwareBlockNameMap::getNamesToIndexesMap(const Block & block)
{
    const auto & index_by_name = block.getIndexByName();
    for (const auto & [name, index] : index_by_name)
    {
        add(name, index);
    }
}


std::unique_ptr<ICaseAwareBlockNameMap> ICaseAwareBlockNameMap::construct(const FormatSettings & settings, size_t expected_size)
{
    switch (settings.input_format_with_names_case_insensitive_column_matching)
    {
        case FormatSettings::InputFormatCaseSensitivity::MATCH_CASE:
            return std::make_unique<MatchCaseBlockNameMap>(expected_size);
        case FormatSettings::InputFormatCaseSensitivity::IGNORE_CASE:
            return std::make_unique<IgnoreCaseBlockNameMap>(expected_size);
        case FormatSettings::InputFormatCaseSensitivity::AUTO:
            return std::make_unique<AutoCaseBlockNameMap>(expected_size);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid value for setting `input_format_with_names_case_insensitive_column_matching`");
}

MatchCaseBlockNameMap::MatchCaseBlockNameMap(size_t size)
    : map(size)
{
    map.set_empty_key(std::string_view{});
}

void MatchCaseBlockNameMap::add(const std::string_view & key, size_t idx)
{
    map[key] = idx;
}
size_t MatchCaseBlockNameMap::get(const std::string_view & key)
{
    auto it = map.find(key);
    if (it == map.end())
    {
        return -1;
    }
    return it->second;
}

IgnoreCaseBlockNameMap::IgnoreCaseBlockNameMap(size_t size)
    : map(size)
{
    map.set_empty_key(std::string_view{});
}

void IgnoreCaseBlockNameMap::add(const std::string_view & key_view, size_t idx)
{
    String key (key_view);
    if (map.find(key) != map.end())
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Ambiguous field (`{}` at position {}) when processing data.", key, idx);
    }
    map[key] = idx;
}

size_t IgnoreCaseBlockNameMap::get(const std::string_view & key)
{
    auto it = map.find(key);
    if (it == map.end())
    {
        return -1;
    }
    return it->second;
}

AutoCaseBlockNameMap::AutoCaseBlockNameMap(size_t size)
    : map(size)
    , i_map(size)
    , ambiguity(size)
{
    map.set_empty_key(std::string_view{});
    i_map.set_empty_key(std::string_view{});
    ambiguity.set_empty_key(std::string_view{});
}

void AutoCaseBlockNameMap::add(const std::string_view & key, size_t idx)
{
    map[key] = idx;
    i_map[key] = idx;

    auto it = ambiguity.find(key);
    if (it == ambiguity.end())
    {
        ambiguity[key] = 1;
    }
    else
    {
        it->second++;
    }
}

size_t AutoCaseBlockNameMap::get(const std::string_view & key)
{
    // First check if the key has an exact match
    auto it = map.find(key);
    if (it != map.end())
    {
        return it->second;
    }
    // Check for ambiguity first
    AmbiguityCheck(key);

    // Check if the key has a match ignoring case
    auto i_it = i_map.find(key);
    if (i_it != i_map.end())
    {
        return i_it->second;
    }

    return -1;
}

void AutoCaseBlockNameMap::AmbiguityCheck(const std::string_view & key)
{
    auto it = ambiguity.find(key);
    if (it == ambiguity.end())
    {
        return;
    }

    if (it->second > 1)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Ambiguous field (`{}`) when processing data.", key);
    }
}
}
