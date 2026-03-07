#pragma once

#include <cctype>
#include <string_view>
#include "Formats/FormatSettings.h"

namespace DB {

class Block;

/// Interface for case aware map between column name and position in a Block
class CaseAwareBlockNameMap{
public:
    enum SearchResult : size_t {
        NOT_FOUND = size_t(-1), /// Return value of `get` method whenever the key is not found
    };

    /// Constructs a map between column name and position in the block
    explicit CaseAwareBlockNameMap(FormatSettings::InputFormatCaseSensitivity input_mode, const Block & block);
    ~CaseAwareBlockNameMap();
    /// Adds a new pair column_name and position
    void add(std::string_view column_name, size_t idx);
    /// Fetches the position of the given column_name
    /// Returns NOT_FOUND in case the column_name is not in the map
    size_t get(std::string_view column_name);

    /// Compares two strings, using the same method as the one used internally
    bool stringCompare(std::string_view left, std::string_view right);
private:
    /// Constructs the map from the given block
    void getNamesToIndexesMap(const Block & block);

    /// Input mode that will be used by this object
    const FormatSettings::InputFormatCaseSensitivity mode;

    /// PIMP idiom, these classes are only relevant to this specific scenario. So, there is
    /// no need for it to "leak" outside of this scope
    class MatchCaseBlockNameMap;
    class IgnoreCaseBlockNameMap;
    class AutoCaseBlockNameMap;
    std::unique_ptr<MatchCaseBlockNameMap> match_case;
    std::unique_ptr<IgnoreCaseBlockNameMap> ignore_case;
    std::unique_ptr<AutoCaseBlockNameMap> auto_case;
};
}
