#pragma once

#include <string_view>
#include <Formats/FormatSettings.h>

namespace DB
{

class Block;

/// Interface for BlockNameMaps
class IBlockNameMap
{
public:
    enum SearchResult : size_t
    {
        NOT_FOUND = size_t(-1), /// Return value of `get` method whenever the key is not found
    };

    virtual ~IBlockNameMap() = default;

    /// Adds a new element to the map
    virtual void add(std::string_view key, size_t idx) = 0;
    /// Sets the size of the map
    virtual void setSize(size_t size) = 0;
    /// Gets the size
    virtual size_t size() const = 0;
    /// Gets the value for the given key, returns NOT_FOUND if key is not present
    virtual size_t get(std::string_view key) const = 0;
    /// Method used by the map to compare strings
    virtual bool stringCompare(std::string_view left, std::string_view right) const = 0;
};

/// Case aware map between column name and position in a Block
class CaseAwareBlockNameMap
{
public:
    explicit CaseAwareBlockNameMap(FormatSettings::InputFormatColumnMatchingCaseSensitivity input_mode);
    /// Adds a new pair column_name and position
    void add(std::string_view column_name, size_t idx);
    /// Fetches the position of the given column_name
    /// Returns NOT_FOUND in case the column_name is not in the map
    size_t get(std::string_view column_name) const;

    size_t size() const;

    /// Sets the expected size of the map
    void setSize(size_t size);

    /// Constructs the map from the given block
    void initFromBlock(const Block & block);

    /// Compares two strings, using the same method as the one used internally
    bool equal(std::string_view left, std::string_view right) const;
private:
    std::unique_ptr<IBlockNameMap> map;
};
}
