#include <Core/CaseAwareBlockNameMap.h>

#include <cctype>
#include <memory>
#include <string_view>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <base/StringViewHash.h>
#include <base/defines.h>
#include <sparsehash/dense_hash_map>
#include <Poco/String.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

struct CaseInsensitiveHash
{
    size_t operator()(const std::string_view key) const
    {
        // TODO: there is probably a way to calculate this without copying the string
        std::string key_s{key};
        Poco::toLowerInPlace(key_s);
        return sipHash64(key_s);
    }
};

struct CaseInsensitiveEquality
{
    bool operator()(const std::string_view left, const std::string_view right) const { return CaseInsensitiveEquality::equal(left, right); }

    static bool equal(const std::string_view left, const std::string_view right)
    {
        if (left.size() != right.size())
            return false;
        return Poco::toLower(std::string(left)) == Poco::toLower(std::string(right));
    }
};

/// Case aware map
class CaseSensitiveBlockNameMap
{
public:
    CaseSensitiveBlockNameMap() { map.set_empty_key(std::string_view{}); }

    void setSize(size_t size)
    {
        expected_size = size;
        map.resize(size);
    }

    void add(const std::string_view key, size_t idx) { map[key] = idx; }

    size_t size() const { return expected_size; }

    size_t get(const std::string_view key) const
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return CaseAwareBlockNameMap::NOT_FOUND;
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const { return left == right; }

private:
    ::google::dense_hash_map<std::string_view, size_t, StringViewHash> map;
    size_t expected_size{0};
};

/// Case independent map
class CaseInsensitiveBlockNameMap
{
public:
    CaseInsensitiveBlockNameMap() { map.set_empty_key(std::string_view{}); }

    void setSize(size_t size)
    {
        expected_size = size;
        map.resize(size);
    }

    void add(const std::string_view key, size_t idx)
    {
        if (map.find(key) != map.end())
        {
            ambiguous_keys.insert(key);
        }
        map[key] = idx;
    }

    size_t size() const { return expected_size; }

    /// Retrieves the position of a given key
    /// Can throw in the case where `key` is ambiguous
    /// For example: Name and namE will both map to the same key (name)
    size_t get(const std::string_view key) const
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return CaseAwareBlockNameMap::NOT_FOUND;
        }
        if (ambiguous_keys.contains(key))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Ambiguous field (`{}`) when processing data.", key);
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const { return CaseInsensitiveEquality::equal(left, right); }

protected:
    ::google::dense_hash_map<std::string_view, size_t, CaseInsensitiveHash, CaseInsensitiveEquality> map;
    std::unordered_set<std::string_view, CaseInsensitiveHash, CaseInsensitiveEquality> ambiguous_keys;
    size_t expected_size{0};
};

/// Auto case map
/// First tries a case aware search, if it fails then it tries case independent
class AutoCaseBlockNameMap
{
public:
    void setSize(size_t size)
    {
        expected_size = size;
        map.setSize(size);
        i_map.setSize(size);
    }

    void add(const std::string_view key, size_t idx)
    {
        map.add(key, idx);
        i_map.add(key, idx);
    }

    size_t size() const { return expected_size; }

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
        if (idx != CaseAwareBlockNameMap::NOT_FOUND)
        {
            return idx;
        }

        // Check if the key has a match ignoring case
        return i_map.get(key);
    }

    bool stringCompare(std::string_view left, std::string_view right) const { return map.stringCompare(left, right); }

private:
    CaseSensitiveBlockNameMap map;
    CaseInsensitiveBlockNameMap i_map;
    size_t expected_size{0};
};

CaseAwareBlockNameMap::CaseAwareBlockNameMap(FormatSettings::InputFormatColumnMatchingCaseSensitivity input_mode)
    : mode(input_mode)
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            this->match_case = std::make_unique<CaseSensitiveBlockNameMap>();
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            this->ignore_case = std::make_unique<CaseInsensitiveBlockNameMap>();
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            this->auto_case = std::make_unique<AutoCaseBlockNameMap>();
            break;
        }
    }
}

CaseAwareBlockNameMap::~CaseAwareBlockNameMap() = default;

void CaseAwareBlockNameMap::getNamesToIndexesMap(const Block & block)
{
    setSize(block.getIndexByName().size());
    const auto & index_by_name = block.getIndexByName();
    for (const auto & [name, index] : index_by_name)
    {
        add(name, index);
    }
}

void CaseAwareBlockNameMap::add(std::string_view column_name, size_t idx)
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            chassert(this->match_case);
            this->match_case->add(column_name, idx);
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            chassert(this->ignore_case);
            this->ignore_case->add(column_name, idx);
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            chassert(this->auto_case);
            this->auto_case->add(column_name, idx);
            break;
        }
    }
}

size_t CaseAwareBlockNameMap::get(std::string_view column_name) const
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            chassert(this->match_case);
            return this->match_case->get(column_name);
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            chassert(this->ignore_case);
            return this->ignore_case->get(column_name);
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            chassert(this->auto_case);
            return this->auto_case->get(column_name);
        }
    }
}


bool CaseAwareBlockNameMap::equal(std::string_view left, std::string_view right) const
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            chassert(this->match_case);
            return this->match_case->stringCompare(left, right);
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            chassert(this->ignore_case);
            return this->ignore_case->stringCompare(left, right);
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            chassert(this->auto_case);
            return this->auto_case->stringCompare(left, right);
        }
    }
}

size_t CaseAwareBlockNameMap::size() const
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            chassert(this->match_case);
            return this->match_case->size();
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            chassert(this->ignore_case);
            return this->ignore_case->size();
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            chassert(this->auto_case);
            return this->auto_case->size();
        }
    }
}

void CaseAwareBlockNameMap::setSize(size_t size)
{
    switch (mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            chassert(this->match_case);
            this->match_case->setSize(size);
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            chassert(this->ignore_case);
            this->ignore_case->setSize(size);
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            chassert(this->auto_case);
            this->auto_case->setSize(size);
            break;
        }
    }
}
}
