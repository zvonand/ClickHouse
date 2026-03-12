#pragma once

#include <Common/HashTable/HashTable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
}

/** Similar to FixedHashTable, but covers an arbitrary [min_key, max_key] subrange
  *  instead of the full key type range. Allocates a flat array of `max_key - min_key + 1`
  *  cells and indexes by `key - min_key`, requiring no hashing, no collision chains,
  *  and no key comparison. Keys are not stored in cells but inferred from the array
  *  index inside iterators.
  *
  * Used as an optimization for hash join when the key range is small enough to fit in memory.
  */
template <typename Key, typename Cell, typename Allocator>
class FixedRangeHashTable : private boost::noncopyable, protected Allocator, protected Cell::State
{
    size_t num_cells = 0;
    Key min_key{};

protected:
    friend class const_iterator;
    friend class iterator;
    friend class Reader;

    using Self = FixedRangeHashTable;

    Cell * buf = nullptr; /// A piece of memory for all elements.
    std::atomic<size_t> m_size = 0;

    void alloc() { buf = reinterpret_cast<Cell *>(Allocator::alloc(num_cells * sizeof(Cell))); }

    void free()
    {
        if (buf)
        {
            Allocator::free(buf, getBufferSizeInBytes());
            buf = nullptr;
        }
    }

    void destroyElements()
    {
        if (!std::is_trivially_destructible_v<Cell>)
            for (iterator it = begin(), it_end = end(); it != it_end; ++it)
                it.ptr->~Cell();
    }

    void increaseSize() { m_size.fetch_add(1); }
    void clearSize() { m_size.store(0); }
    void setSize(size_t to) { m_size.store(to); }


    template <typename Derived, bool is_const>
    class iterator_base /// NOLINT
    {
        using Container = std::conditional_t<is_const, const Self, Self>;
        using cell_type = std::conditional_t<is_const, const Cell, Cell>;

        Container * container;
        cell_type * ptr;

        friend class FixedRangeHashTable;

    public:
        iterator_base() { } /// NOLINT
        iterator_base(Container * container_, cell_type * ptr_)
            : container(container_)
            , ptr(ptr_)
        {
            cell.update(static_cast<Key>(container->min_key + (ptr - container->buf)), ptr);
        }

        bool operator==(const iterator_base & rhs) const { return ptr == rhs.ptr; }
        bool operator!=(const iterator_base & rhs) const { return ptr != rhs.ptr; }

        Derived & operator++()
        {
            ++ptr;

            /// Skip empty cells in the main buffer.
            const auto * buf_end = container->buf + container->num_cells;
            while (ptr < buf_end && ptr->isZero(*container))
                ++ptr;

            return static_cast<Derived &>(*this);
        }

        auto & operator*()
        {
            auto key = static_cast<Key>(ptr - container->buf) + container->min_key;
            if (cell.key != key)
                cell.update(std::move(key), ptr);
            return cell;
        }
        auto * operator->()
        {
            auto key = static_cast<Key>(ptr - container->buf) + container->min_key;
            if (cell.key != key)
                cell.update(std::move(key), ptr);
            return &cell;
        }

        auto getPtr() const { return ptr; }
        size_t getHash() const { return ptr - container->buf; }
        size_t getCollisionChainLength() const { return 0; }
        typename cell_type::CellExt cell;
    };


public:
    using key_type = Key;
    using mapped_type = typename Cell::mapped_type;
    using value_type = typename Cell::value_type;
    using cell_type = Cell;

    using LookupResult = Cell *;
    using ConstLookupResult = const Cell *;


    size_t hash(const Key & x) const { return static_cast<size_t>(x - min_key); }

    FixedRangeHashTable() = default;

    FixedRangeHashTable(Key min_key_, Key max_key_)
        : min_key(min_key_)
    {
        size_t range = static_cast<size_t>(max_key_ - min_key_);
        if (range == std::numeric_limits<size_t>::max())
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Range too large and will overflow");

        num_cells = range + 1;
        alloc();
    }

    FixedRangeHashTable(FixedRangeHashTable && rhs) noexcept
        : buf(nullptr)
    {
        *this = std::move(rhs);
    } /// NOLINT

    ~FixedRangeHashTable()
    {
        destroyElements();
        free();
    }

    FixedRangeHashTable & operator=(FixedRangeHashTable && rhs) noexcept
    {
        destroyElements();
        free();

        std::swap(num_cells, rhs.num_cells);
        std::swap(min_key, rhs.min_key);
        std::swap(buf, rhs.buf);
        setSize(rhs.size());

        Allocator::operator=(std::move(rhs));
        Cell::State::operator=(std::move(rhs));

        return *this;
    }

    class iterator : public iterator_base<iterator, false> /// NOLINT
    {
    public:
        using iterator_base<iterator, false>::iterator_base;
    };

    class const_iterator : public iterator_base<const_iterator, true> /// NOLINT
    {
    public:
        using iterator_base<const_iterator, true>::iterator_base;
    };


    const_iterator begin() const
    {
        if (!buf)
            return end();

        return const_iterator(this, firstPopulatedCell());
    }

    const_iterator cbegin() const { return begin(); }

    iterator begin()
    {
        if (!buf)
            return end();

        return iterator(this, firstPopulatedCell());
    }

    const_iterator end() const { return const_iterator(this, buf ? buf + num_cells : buf); }

    const_iterator cend() const { return end(); }

    iterator end() { return iterator(this, buf ? buf + num_cells : buf); }


    /// The last parameter is unused but exists for compatibility with HashTable interface.
    void ALWAYS_INLINE emplace(const Key & x, LookupResult & it, bool & inserted, size_t /* hash */ = 0)
    {
        size_t index = static_cast<size_t>(x - min_key);
        if (index >= num_cells)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Emplaced key out of range");

        it = &buf[index];

        if (!buf[index].isZero(*this))
        {
            inserted = false;
            return;
        }

        new (&buf[index]) Cell(x, *this);
        inserted = true;
        increaseSize();
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        std::pair<LookupResult, bool> res;
        emplace(Cell::getKey(x), res.first, res.second);
        if (res.second)
            res.first->setMapped(x);

        return res;
    }

    LookupResult ALWAYS_INLINE find(const Key & x)
    {
        size_t index = static_cast<size_t>(x - min_key);
        if (index >= num_cells)
            return nullptr;
        return !buf[index].isZero(*this) ? &buf[index] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & x) const { return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x); }

    LookupResult ALWAYS_INLINE find(const Key &, size_t hash_value)
    {
        if (hash_value >= num_cells)
            return nullptr;
        return !buf[hash_value].isZero(*this) ? &buf[hash_value] : nullptr;
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & key, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(key, hash_value);
    }

    bool ALWAYS_INLINE has(const Key & x) const
    {
        size_t index = static_cast<size_t>(x - min_key);
        if (index >= num_cells)
            return false;
        return !buf[index].isZero(*this);
    }

    bool ALWAYS_INLINE has(const Key &, size_t hash_value) const
    {
        if (hash_value >= num_cells)
            return false;
        return !buf[hash_value].isZero(*this);
    }

    const Cell * ALWAYS_INLINE firstPopulatedCell() const
    {
        const Cell * ptr = buf;
        while (ptr < buf + num_cells && ptr->isZero(*this))
            ++ptr;

        return ptr;
    }

    size_t size() const { return m_size.load(); }
    bool empty() const { return m_size.load() == 0; }

    void clear()
    {
        destroyElements();
        clearSize();

        memset(static_cast<void *>(buf), 0, num_cells * sizeof(*buf));
    }

    /// After executing this function, the table can only be destroyed,
    ///  and also you can use the methods `size`, `empty`, `begin`, `end`.
    void clearAndShrink()
    {
        destroyElements();
        clearSize();
        free();
    }

    size_t getBufferSizeInBytes() const { return num_cells * sizeof(Cell); }

    size_t getBufferSizeInCells() const { return num_cells; }

    /// Return offset for result in internal buffer.
    /// Result can have value up to `getBufferSizeInCells() + 1`
    /// because offset for zero value considered to be 0
    /// and for other values it will be `offset in buffer + 1`
    size_t offsetInternal(ConstLookupResult ptr) const
    {
        if (ptr->isZero(*this))
            return 0;
        return ptr - buf + 1;
    }

    const Cell * data() const { return buf; }
    Cell * data() { return buf; }

#ifdef DBMS_HASH_MAP_COUNT_COLLISIONS
    size_t getCollisions() const { return 0; }
#endif
};
