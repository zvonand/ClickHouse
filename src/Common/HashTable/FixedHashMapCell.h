#pragma once

#include <Common/HashTable/HashMap.h>


template <typename Key, typename TMapped, typename TState = HashTableNoState>
struct FixedHashMapCell
{
    using Mapped = TMapped;
    using State = TState;

    using value_type = PairNoInit<Key, Mapped>;
    using mapped_type = TMapped;

    bool full;
    Mapped mapped;

    FixedHashMapCell() { } /// NOLINT
    FixedHashMapCell(const Key &, const State &)
        : full(true)
    {
    }
    FixedHashMapCell(const value_type & value_, const State &)
        : full(true)
        , mapped(value_.second)
    {
    }

    const VoidKey getKey() const { return {}; } /// NOLINT
    Mapped & getMapped() { return mapped; }
    const Mapped & getMapped() const { return mapped; }

    bool isZero(const State &) const { return !full; }
    void setZero() { full = false; }

    /// Similar to FixedHashSetCell except that we need to contain a pointer to the Mapped field.
    ///  Note that we have to assemble a continuous layout for the value_type on each call of getValue().
    struct CellExt
    {
        CellExt() { } /// NOLINT
        CellExt(Key && key_, const FixedHashMapCell * ptr_)
            : key(key_)
            , ptr(const_cast<FixedHashMapCell *>(ptr_))
        {
        }
        void update(Key && key_, const FixedHashMapCell * ptr_)
        {
            key = key_;
            ptr = const_cast<FixedHashMapCell *>(ptr_);
        }
        Key key;
        FixedHashMapCell * ptr;

        const Key & getKey() const { return key; }
        Mapped & getMapped() { return ptr->mapped; }
        const Mapped & getMapped() const { return ptr->mapped; }
        const value_type getValue() const { return {key, ptr->mapped}; } /// NOLINT
    };
};
