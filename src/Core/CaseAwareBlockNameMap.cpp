#include <Core/CaseAwareBlockNameMap.h>

#include <cctype>
#include <memory>
#include <string_view>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <base/StringViewHash.h>
#include <sparsehash/dense_hash_map>
#include "Common/Exception.h"
#include "base/defines.h"

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

/// TODO: Not a very good hash function, there are others we could use
struct CaseInsensitiveHash
{
    size_t operator()(const std::string_view key) const
    {
        size_t h = 0;
        for (const char c : key)
        {
            h += tolower(c);
        }
        return h;
    }
};

struct CaseInsensitiveEquality
{
    bool operator()(const std::string_view left, const std::string_view right) const{
        return CaseInsensitiveEquality::compare(left, right);
    }

    static bool compare(const std::string_view left, const std::string_view right)
    {
        if (left.size() != right.size())
        {
            return false;
        }
        return std::equal(left.begin(), left.end(), right.begin(), [](char a, char b) { return tolower(a) == tolower(b); });
    }
};

/// Case aware map
class CaseAwareBlockNameMap::MatchCaseBlockNameMap
{
public:
    explicit MatchCaseBlockNameMap(size_t size)
        : map(size)
    {
        map.set_empty_key(std::string_view{});
    }

    ~MatchCaseBlockNameMap() = default;

    void add(const std::string_view key, size_t idx) { map[key] = idx; }

    size_t get(const std::string_view key) const
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return NOT_FOUND;
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const{
        return left == right;
    }

private:
    ::google::dense_hash_map<std::string_view, size_t, StringViewHash> map;
};

/// Case independent map
class CaseAwareBlockNameMap::IgnoreCaseBlockNameMap
{
public:
    explicit IgnoreCaseBlockNameMap(size_t size)
        : map(size)
    {
        map.set_empty_key(std::string_view{});
    }

    ~IgnoreCaseBlockNameMap() = default;

    void add(const std::string_view key, size_t idx)
    {
        if (map.find(key) != map.end())
        {
            ambiguous_keys.insert(key);
        }
        map[key] = idx;
    }

    /// Retrieves the position of a given key
    /// Can throw in the case where `key` is ambiguous
    /// For example: Name and namE will both map to the same key (name)
    size_t get(const std::string_view key) const
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return NOT_FOUND;
        }
        if(ambiguous_keys.contains(key)){
            throw Exception(ErrorCodes::INCORRECT_DATA, "Ambiguous field (`{}`) when processing data.", key);
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const{
        return CaseInsensitiveEquality::compare(left, right);
    }

protected:
    ::google::dense_hash_map<std::string_view, size_t, CaseInsensitiveHash, CaseInsensitiveEquality> map;
    std::unordered_set<std::string_view, CaseInsensitiveHash, CaseInsensitiveEquality> ambiguous_keys;
};

/// Auto case map
/// First tries a case aware search, if it fails then it tries case independent
class CaseAwareBlockNameMap::AutoCaseBlockNameMap
{
public:
    explicit AutoCaseBlockNameMap(size_t size)
        : map(size)
        , i_map(size)
    {
    }

    ~AutoCaseBlockNameMap() = default;

    void add(const std::string_view key, size_t idx)
    {
        map.add(key, idx);
        i_map.add(key, idx);
    }

    /** Retrieves the position of a given key
      * Can throw in the case where `key` is ambiguous
      * For example:
      *  map: {Name: 0, namE: 1}
      *
      *  map[Name] -> 0
      *  map[namE] -> 1
      *  map[name] -> error, ambiguous
      */
    size_t get(const std::string_view key) const
    {
        // First check if the key has an exact match
        auto idx = map.get(key);
        if(idx != NOT_FOUND){
            return idx;
        }

        // Check if the key has a match ignoring case
        return i_map.get(key);
    }

    bool stringCompare(std::string_view left, std::string_view right) const{
        return map.stringCompare(left, right) || i_map.stringCompare(left, right);
    }

private:
    MatchCaseBlockNameMap map;
    IgnoreCaseBlockNameMap i_map;
};

CaseAwareBlockNameMap::CaseAwareBlockNameMap(FormatSettings::InputFormatCaseSensitivity input_mode, const Block & block)
    : mode(input_mode)
{
    expected_size = block.getIndexByName().size();
    switch (mode)
    {
        case FormatSettings::InputFormatCaseSensitivity::MATCH_CASE:
        {
            this->match_case = std::make_unique<MatchCaseBlockNameMap>(expected_size);
            break;
        }
        case FormatSettings::InputFormatCaseSensitivity::IGNORE_CASE:
        {
            this->ignore_case = std::make_unique<IgnoreCaseBlockNameMap>(expected_size);
            break;
        }
        case FormatSettings::InputFormatCaseSensitivity::AUTO:
        {
            this->auto_case = std::make_unique<AutoCaseBlockNameMap>(expected_size);
            break;
        }
    }
    getNamesToIndexesMap(block);
}


CaseAwareBlockNameMap::~CaseAwareBlockNameMap()= default;

void CaseAwareBlockNameMap::getNamesToIndexesMap(const Block & block)
{
    const auto & index_by_name = block.getIndexByName();
    for (const auto & [name, index] : index_by_name)
    {
        add(name, index);
    }
}

void CaseAwareBlockNameMap::add(std::string_view column_name, size_t idx){
    switch (mode) {
        case FormatSettings::InputFormatCaseSensitivity::MATCH_CASE:
        {
            chassert(this->match_case);
            this->match_case->add(column_name, idx);
            break;
        }
        case FormatSettings::InputFormatCaseSensitivity::IGNORE_CASE:
        {
            chassert(this->ignore_case);
            this->ignore_case->add(column_name, idx);
            break;
        }
        case FormatSettings::InputFormatCaseSensitivity::AUTO:
        {
            chassert(this->auto_case);
            this->auto_case->add(column_name, idx);
            break;
        }
    }
}

size_t CaseAwareBlockNameMap::get(std::string_view column_name) const{
    switch (mode) {
        case FormatSettings::InputFormatCaseSensitivity::MATCH_CASE:
        {
            chassert(this->match_case);
            return this->match_case->get(column_name);
        }
        case FormatSettings::InputFormatCaseSensitivity::IGNORE_CASE:
        {
            chassert(this->ignore_case);
            return this->ignore_case->get(column_name);
        }
        case FormatSettings::InputFormatCaseSensitivity::AUTO:
        {
            chassert(this->auto_case);
            return this->auto_case->get(column_name);
        }
    }
}


bool CaseAwareBlockNameMap::stringCompare(std::string_view left, std::string_view right) const{
    switch (mode) {
        case FormatSettings::InputFormatCaseSensitivity::MATCH_CASE:
        {
            chassert(this->match_case);
            return this->match_case->stringCompare(left, right);
        }
        case FormatSettings::InputFormatCaseSensitivity::IGNORE_CASE:
        {
            chassert(this->ignore_case);
            return this->ignore_case->stringCompare(left, right);
        }
        case FormatSettings::InputFormatCaseSensitivity::AUTO:
        {
            chassert(this->auto_case);
            return this->auto_case->stringCompare(left, right);
        }
    }
}

}
