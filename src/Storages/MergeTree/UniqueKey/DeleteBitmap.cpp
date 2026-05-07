#include <Storages/MergeTree/UniqueKey/DeleteBitmap.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <roaring/roaring.hh>
#include <zlib.h>

#include <charconv>
#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}

namespace
{
    /// CRC over two contiguous regions (header prefix + serialized roaring body).
    UInt32 computeCRC32(const char * header, size_t header_size, const char * body, size_t body_size)
    {
        uLong crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, reinterpret_cast<const Bytef *>(header), static_cast<uInt>(header_size));
        if (body_size)
            crc = crc32(crc, reinterpret_cast<const Bytef *>(body), static_cast<uInt>(body_size));
        return static_cast<UInt32>(crc);
    }

    constexpr std::string_view FILE_PREFIX = "delete_bitmap_";
    constexpr std::string_view FILE_SUFFIX = ".rbm";

    /// Small constant for empty-bitmap footprint so size proxies stay non-zero.
    constexpr size_t EMPTY_BITMAP_OVERHEAD = 64;

    /// Corruption guard: reject implausibly large payload lengths before allocating.
    constexpr UInt32 MAX_SERIALIZED_BODY_SIZE = 256U * 1024U * 1024U;
}

DeleteBitmap::DeleteBitmap() : bitmap(std::make_unique<roaring::Roaring>())
{
}

DeleteBitmap::~DeleteBitmap() = default;

DeleteBitmap::DeleteBitmap(DeleteBitmap &&) noexcept = default;
DeleteBitmap & DeleteBitmap::operator=(DeleteBitmap &&) noexcept = default;

bool DeleteBitmap::contains(UInt32 row) const
{
    return bitmap->contains(row);
}

void DeleteBitmap::containsBulk(const UInt32 * rows, size_t n, uint8_t * out_keep) const
{
    if (n == 0)
        return;

    if (bitmap->isEmpty())
    {
        std::memset(out_keep, 1, n);
        return;
    }

    /// `BulkContext` caches the last-touched container so consecutive probes in the same upper-16 bin skip roaring's outer search.
    roaring::BulkContext ctx;
    for (size_t i = 0; i < n; ++i)
        out_keep[i] = bitmap->containsBulk(ctx, rows[i]) ? 0 : 1;
}

void DeleteBitmap::add(UInt32 row)
{
    bitmap->add(row);
}

void DeleteBitmap::addMany(const std::vector<UInt32> & rows)
{
    if (rows.empty())
        return;
    bitmap->addMany(rows.size(), rows.data());
}

void DeleteBitmap::merge(const DeleteBitmap & other)
{
    *bitmap |= *other.bitmap;
}

size_t DeleteBitmap::cardinality() const
{
    return bitmap->cardinality();
}

bool DeleteBitmap::empty() const
{
    return bitmap->isEmpty();
}

size_t DeleteBitmap::rangeCardinality(UInt64 begin, UInt64 end) const
{
    /// Computes |bitmap ∩ [begin, end)| as `rank(end-1) - rank(begin-1)`.
    /// Roaring32 only addresses UInt32, so begin/end are clamped against the UInt32 ceiling.
    if (end <= begin || bitmap->isEmpty())
        return 0;

    constexpr UInt64 max_row = std::numeric_limits<UInt32>::max();
    if (begin > max_row)
        return 0;
    const UInt64 hi_inclusive = std::min(end - 1, max_row);
    if (hi_inclusive < begin)
        return 0;

    const uint64_t upper = bitmap->rank(static_cast<uint32_t>(hi_inclusive));
    const uint64_t lower = (begin == 0) ? 0 : bitmap->rank(static_cast<uint32_t>(begin - 1));
    return static_cast<size_t>(upper - lower);
}

std::vector<UInt32> DeleteBitmap::toVector() const
{
    std::vector<UInt32> out;
    const size_t card = bitmap->cardinality();
    if (card == 0)
        return out;
    out.resize(card);
    /// `toUint32Array` writes `cardinality` UInt32 values in ascending order.
    bitmap->toUint32Array(out.data());
    return out;
}

size_t DeleteBitmap::memoryUsage() const
{
    /// Use the portable-serialized size as a portable approximation of the
    /// in-memory footprint. Roaring's internal containers have per-container
    /// overhead that the portable serializer already accounts for, and the
    /// serialized size matches the on-disk `.rbm` size.
    size_t serialized = bitmap->getSizeInBytes(/*portable=*/true);
    if (serialized == 0)
        return EMPTY_BITMAP_OVERHEAD;
    return serialized + EMPTY_BITMAP_OVERHEAD;
}

void DeleteBitmap::serialize(WriteBuffer & out) const
{
    const size_t body_size = bitmap->getSizeInBytes(/*portable=*/true);
    if (body_size > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap serialized body too large: {} bytes (max {})", body_size, std::numeric_limits<UInt32>::max());

    std::vector<char> body(body_size);
    if (body_size)
    {
        size_t written = bitmap->write(body.data(), /*portable=*/true);
        if (written != body_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "DeleteBitmap roaring::write returned {} bytes, expected {}", written, body_size);
    }

    /// Serialize header into a fixed buffer so we can CRC it as one blob.
    char header[sizeof(UInt32) * 3];
    UInt32 body_size_u32 = static_cast<UInt32>(body_size);
    std::memcpy(header + 0,                   &MAGIC,         sizeof(UInt32));
    std::memcpy(header + sizeof(UInt32),      &VERSION,       sizeof(UInt32));
    std::memcpy(header + sizeof(UInt32) * 2,  &body_size_u32, sizeof(UInt32));

    UInt32 crc = computeCRC32(header, sizeof(header), body.data(), body_size);

    out.write(header, sizeof(header));
    if (body_size)
        out.write(body.data(), body_size);
    writePODBinary(crc, out);
}

std::unique_ptr<DeleteBitmap> DeleteBitmap::deserialize(ReadBuffer & in)
{
    char header[sizeof(UInt32) * 3];
    in.readStrict(header, sizeof(header));

    UInt32 magic = 0;
    UInt32 version = 0;
    UInt32 body_size_u32 = 0;
    std::memcpy(&magic,         header + 0,                  sizeof(UInt32));
    std::memcpy(&version,       header + sizeof(UInt32),     sizeof(UInt32));
    std::memcpy(&body_size_u32, header + sizeof(UInt32) * 2, sizeof(UInt32));

    if (magic != MAGIC)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap magic mismatch: expected {:#x}, got {:#x}", MAGIC, magic);

    if (version != VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "DeleteBitmap version {} is not supported (expected {})", version, VERSION);

    if (body_size_u32 > MAX_SERIALIZED_BODY_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap serialized body too large: {} bytes (max {})", body_size_u32, MAX_SERIALIZED_BODY_SIZE);

    std::vector<char> body(body_size_u32);
    if (body_size_u32)
        in.readStrict(body.data(), body_size_u32);

    UInt32 stored_crc = 0;
    readPODBinary(stored_crc, in);

    UInt32 computed_crc = computeCRC32(header, sizeof(header), body.data(), body_size_u32);
    if (stored_crc != computed_crc)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "DeleteBitmap CRC mismatch: stored {:#x}, computed {:#x}", stored_crc, computed_crc);

    auto result = std::make_unique<DeleteBitmap>();
    if (body_size_u32)
    {
        /// readSafe validates container counts against maxbytes, so a malformed
        /// roaring payload that survived the CRC (extremely unlikely) still
        /// can't over-read.
        *result->bitmap = roaring::Roaring::readSafe(body.data(), body_size_u32);
    }
    return result;
}

std::string DeleteBitmap::fileNameForBlockNumber(UInt64 block_number)
{
    return fmt::format("{}{}{}", FILE_PREFIX, block_number, FILE_SUFFIX);
}

std::optional<UInt64> DeleteBitmap::parseBlockNumberFromFileName(std::string_view file_name)
{
    if (file_name.size() <= FILE_PREFIX.size() + FILE_SUFFIX.size())
        return std::nullopt;
    if (!file_name.starts_with(FILE_PREFIX))
        return std::nullopt;
    if (!file_name.ends_with(FILE_SUFFIX))
        return std::nullopt;

    auto number_part = file_name.substr(FILE_PREFIX.size(), file_name.size() - FILE_PREFIX.size() - FILE_SUFFIX.size());
    if (number_part.empty())
        return std::nullopt;

    UInt64 value = 0;
    auto [ptr, ec] = std::from_chars(number_part.data(), number_part.data() + number_part.size(), value);
    if (ec != std::errc{} || ptr != number_part.data() + number_part.size())
        return std::nullopt;
    return value;
}

}
