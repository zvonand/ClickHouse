#pragma once

#include <cctype>
#include <unordered_map>
#include "Formats/FormatSettings.h"
#include <sparsehash/dense_hash_map>
#include <base/StringViewHash.h>

namespace DB {

class MatchCaseBlockNameMap;
class IgnoreCaseBlockNameMap;
class AutoCaseBlockNameMap;
class Block;

/// Interface for case aware map between column name and position in a Block
class ICaseAwareBlockNameMap{
public:
    virtual ~ICaseAwareBlockNameMap() = default;

    /// Adds a new pair column_name and position
    virtual void add(const std::string_view& column_name, size_t idx) = 0;

    /// Fetches the position of the given column_name
    virtual size_t get(const std::string_view& column_name) = 0;

    void getNamesToIndexesMap(const Block & block);

    static std::unique_ptr<ICaseAwareBlockNameMap> construct(const FormatSettings& settings, size_t expected_size);
};


class MatchCaseBlockNameMap : public ICaseAwareBlockNameMap {
public:
    explicit MatchCaseBlockNameMap(size_t size);
    void add(const std::string_view& key, size_t idx) override;
    size_t get(const std::string_view& key) override;

private:
    ::google::dense_hash_map<std::string_view, size_t, StringViewHash> map;
};

class IgnoreCaseBlockNameMap : public ICaseAwareBlockNameMap {
public:
    explicit IgnoreCaseBlockNameMap(size_t size);

    void add(const std::string_view& key, size_t idx) override;

    size_t get(const std::string_view& key) override;

protected:
    friend AutoCaseBlockNameMap;
    // TODO: Not a very good hash function, there are other we could use
    struct CaseInsensitiveHash
    {
        size_t operator()(const std::string_view & key) const
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
        bool operator()(const std::string_view & left, const std::string_view & right) const
        {
            if (left.size() != right.size())
            {
                return false;
            }
            return std::equal(left.begin(), left.end(), right.begin(), [](char a, char b) { return tolower(a) == tolower(b); });
        }
    };
    ::google::dense_hash_map<std::string_view, size_t, CaseInsensitiveHash, CaseInsensitiveEquality> map;
};

class AutoCaseBlockNameMap : public ICaseAwareBlockNameMap {
public:
    explicit AutoCaseBlockNameMap(size_t size);

    void add(const std::string_view& key, size_t idx) override;

    size_t get(const std::string_view& key) override;
private:
    void AmbiguityCheck(const std::string_view& key);

    /// Maps from string to position
    ::google::dense_hash_map<std::string_view, size_t, StringViewHash> map;
    /// Maps from string (ignoring case) to position. Efectivelly, it transforms every string into its lower case representation
    ::google::dense_hash_map<std::string_view, size_t, IgnoreCaseBlockNameMap::CaseInsensitiveHash, IgnoreCaseBlockNameMap::CaseInsensitiveEquality> i_map;
    /// Counts the number of keys which when transformed to lower case map to the same string
    /// For example: `Name` and `nAme` both map to `name`
    ::google::dense_hash_map<std::string_view, size_t, IgnoreCaseBlockNameMap::CaseInsensitiveHash, IgnoreCaseBlockNameMap::CaseInsensitiveEquality> ambiguity;
};

}
