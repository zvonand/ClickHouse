#pragma once

#include <base/types.h>

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace roaring
{
class Roaring;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** UNIQUE KEY per-part delete bitmap — row positions (within a part, 0-based)
  * that are logically deleted. Wraps a 32-bit `roaring::Roaring` — this is the
  * only bitmap type we need because MergeTree parts are addressed by `UInt32`
  * row numbers (`_part_offset`), and a single part caps out well below 2^32
  * rows in practice. Using the 32-bit variant halves serialized size vs.
  * `Roaring64Map` and matches the existing `PostingList = roaring::Roaring`
  * alias already used by `MergeTreeIndexText`.
  *
  * Persistence: one file per bitmap version, named
  *   `delete_bitmap_{block_number}.rbm`
  * inside the part directory. Format (all little-endian):
  *   magic(4) "RBM1" | version(4) | roaring_size(4) | roaring_data[roaring_size] | crc32(4)
  * where crc32 covers magic..roaring_data inclusive.
  */
class DeleteBitmap
{
public:
    DeleteBitmap();
    ~DeleteBitmap();

    DeleteBitmap(const DeleteBitmap &) = delete;
    DeleteBitmap & operator=(const DeleteBitmap &) = delete;
    DeleteBitmap(DeleteBitmap &&) noexcept;
    DeleteBitmap & operator=(DeleteBitmap &&) noexcept;

    /// True if `row` is set.
    bool contains(UInt32 row) const;

    /// Bulk point-containment via roaring `BulkContext`; writes 1 to
    /// `out_keep[i]` when `rows[i]` is *not* in the bitmap, 0 otherwise.
    /// Caller sizes `out_keep >= n`. `n == 0` is a no-op.
    void containsBulk(const UInt32 * rows, size_t n, uint8_t * out_keep) const;

    /// Set `row`.
    void add(UInt32 row);
    /// Set every entry of `rows`. Empty input is a no-op.
    void addMany(const std::vector<UInt32> & rows);
    /// In-place union: `*this |= other`.
    void merge(const DeleteBitmap & other);

    /// Number of set bits.
    size_t cardinality() const;
    /// True if no bits are set.
    bool empty() const;

    /// |bitmap ∩ [begin, end)|, computed as `rank(end-1) - rank(begin-1)`.
    /// O(log N) per `rank` on bitset containers, O(log K) on array
    /// containers. UInt64 inputs are clamped against the UInt32 row
    /// ceiling.
    size_t rangeCardinality(UInt64 begin, UInt64 end) const;

    /// All set row indices in ascending order. O(cardinality).
    std::vector<UInt32> toVector() const;

    /// Portable serialized size + a small entry overhead. Returns a stable
    /// size proxy: roaring's true in-memory footprint depends on container
    /// internals and is not a stable public API, while the serialized size
    /// is a faithful proxy for the on-disk `.rbm` cost. Empty bitmap returns
    /// a small non-zero constant.
    size_t memoryUsage() const;

    /// Serialize to the on-disk format. Writes magic + version + payload + crc.
    void serialize(WriteBuffer & out) const;

    /// Deserialize; validates magic / version / crc and throws on mismatch.
    /// Returned bitmap is independent — `in` can be destroyed afterwards.
    static std::unique_ptr<DeleteBitmap> deserialize(ReadBuffer & in);

    /// File name convention: `delete_bitmap_{block_number}.rbm`.
    static std::string fileNameForBlockNumber(UInt64 block_number);

    /// Parse `delete_bitmap_{N}.rbm` → N. Returns std::nullopt on non-match.
    static std::optional<UInt64> parseBlockNumberFromFileName(std::string_view file_name);

    /// File-format constants. Exposed so tests can corrupt bytes deterministically.
    static constexpr UInt32 MAGIC = 0x314D4252; /// "RBM1" little-endian
    static constexpr UInt32 VERSION = 1;

private:
    std::unique_ptr<roaring::Roaring> bitmap;
};

using DeleteBitmapPtr = std::shared_ptr<DeleteBitmap>;

}
