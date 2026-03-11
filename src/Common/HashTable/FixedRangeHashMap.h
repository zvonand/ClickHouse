#pragma once

#include <Common/HashTable/FixedHashMapCell.h>
#include <Common/HashTable/FixedRangeHashTable.h>
#include <Common/HashTable/HashMap.h>


template <typename Key, typename Mapped, typename Cell = FixedHashMapCell<Key, Mapped>, typename Allocator = HashTableAllocator>
class FixedRangeHashMap : public FixedRangeHashTable<Key, Cell, Allocator>
{
public:
    using Base = FixedRangeHashTable<Key, Cell, Allocator>;
    using LookupResult = typename Base::LookupResult;

    using Base::Base;

    FixedRangeHashMap() = default;
    FixedRangeHashMap(size_t) { } /// NOLINT

    template <typename SourceHashMap>
    FixedRangeHashMap(const SourceHashMap & source_map, Key min_key_, Key max_key_)
        : Base(min_key_, max_key_)
    {
        for (auto it = source_map.begin(); it != source_map.end(); ++it)
        {
            LookupResult res;
            bool inserted;
            this->emplace(it->getKey(), res, inserted);
            if (inserted)
                res->getMapped() = it->getMapped();
        }
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        for (auto & v : *this)
            func(v.getKey(), v.getMapped());
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        for (auto & v : *this)
        {
            if constexpr (std::is_same_v<decltype(func(v.getMapped())), bool>)
            {
                if (!func(v.getMapped()))
                    break;
            }
            else
            {
                func(v.getMapped());
            }
        }
    }

    Mapped & ALWAYS_INLINE operator[](const Key & x)
    {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);
        if (inserted)
            new (&it->getMapped()) Mapped();

        return it->getMapped();
    }
};
