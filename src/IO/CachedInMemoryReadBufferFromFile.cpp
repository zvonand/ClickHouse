#include <IO/CachedInMemoryReadBufferFromFile.h>
#include <Common/PODArray.h>
#include <base/scope_guard.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event PageCacheReadBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}

CachedInMemoryReadBufferFromFile::CachedInMemoryReadBufferFromFile(
    PageCacheKey cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_)
    : ReadBufferFromFileBase(0, nullptr, 0, in_->getFileSize()), cache_key(cache_key_), cache(cache_)
    , settings(settings_)
    , in(std::move(in_)), read_until_position(file_size.value())
    , inner_read_until_position(read_until_position)
    , inner_supports_read_at(in->supportsReadAt())
{
    cache_key.offset = 0;
}

String CachedInMemoryReadBufferFromFile::getFileName() const
{
    return cache_key.path;
}

String CachedInMemoryReadBufferFromFile::getInfoForLog()
{
    return "CachedInMemoryReadBufferFromFile(" + in->getInfoForLog() + ")";
}

bool CachedInMemoryReadBufferFromFile::isSeekCheap()
{
    /// Seek is cheap in the sense that seek()+nextImpl() is never much slower than ignore()+nextImpl()
    /// (which is what the caller cares about).
    return true;
}

off_t CachedInMemoryReadBufferFromFile::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    size_t offset = static_cast<size_t>(off);
    if (offset > file_size.value())
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", off);

    if (offset >= file_offset_of_buffer_end - working_buffer.size() && offset <= file_offset_of_buffer_end)
    {
        pos = working_buffer.end() - (file_offset_of_buffer_end - offset);
        chassert(getPosition() == off);
        return off;
    }

    resetWorkingBuffer();

    file_offset_of_buffer_end = offset;
    chunk.reset();

    chassert(getPosition() == off);
    return off;
}

off_t CachedInMemoryReadBufferFromFile::getPosition()
{
    return file_offset_of_buffer_end - available();
}

size_t CachedInMemoryReadBufferFromFile::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}

void CachedInMemoryReadBufferFromFile::setReadUntilPosition(size_t position)
{
    read_until_position = std::min(position, file_size.value());
    if (position < static_cast<size_t>(getPosition()))
    {
        resetWorkingBuffer();
        chunk.reset();
    }
    else if (position < file_offset_of_buffer_end)
    {
        size_t diff = file_offset_of_buffer_end - position;
        working_buffer.resize(working_buffer.size() - diff);
        file_offset_of_buffer_end -= diff;
    }
}

void CachedInMemoryReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(file_size.value());
}

bool CachedInMemoryReadBufferFromFile::nextImpl()
{
    chassert(read_until_position <= file_size.value());
    if (file_offset_of_buffer_end >= read_until_position)
        return false;

    size_t block_size = settings.page_cache_block_size;

    if (chunk != nullptr)
    {
        chassert(chunk->key.hash() == cache_key.hash());
        if (file_offset_of_buffer_end < cache_key.offset || file_offset_of_buffer_end >= cache_key.offset + block_size)
            chunk.reset();
    }

    if (chunk == nullptr)
    {
        cache_key.offset = file_offset_of_buffer_end / block_size * block_size;
        cache_key.size = std::min(block_size, file_size.value() - cache_key.offset);

        chunk = cache->getOrSet(cache_key, settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction, [&](auto cell)
        {
            Buffer prev_in_buffer = in->internalBuffer();
            SCOPE_EXIT({ in->set(prev_in_buffer.begin(), prev_in_buffer.size()); });

            size_t pos = 0;
            while (pos < cache_key.size)
            {
                char * piece_start = cell->data() + pos;
                size_t piece_size = cache_key.size - pos;
                in->set(piece_start, piece_size);
                if (pos == 0)
                {
                    /// Do in->setReadUntilPosition if needed.
                    /// If the next few blocks are likely cache misses, include them too, to reduce
                    /// the number of requests (usually `in` makes a new HTTP request after each
                    /// nontrivial seek or setReadUntilPosition call).
                    /// Use aligned groups of blocks (rather than sliding window) to work better
                    /// with distributed cache.
                    size_t lookahead_bytes = block_size * std::max<size_t>(1, settings.page_cache_lookahead_blocks);
                    size_t lookahead_block_end = std::min({
                        file_size.value(),
                        (cache_key.offset / lookahead_bytes + 1) * lookahead_bytes,
                        (read_until_position + block_size - 1) / block_size * block_size});

                    if (inner_read_until_position < cache_key.offset + cache_key.size ||
                        inner_read_until_position > lookahead_block_end)
                    {
                        /// Use precomputed base hash to probe lookahead blocks without
                        /// constructing full PageCacheKey objects (avoids string copies).
                        SipHash base = cache_key.baseHash();
                        size_t probe_offset = cache_key.offset;
                        size_t probe_size = cache_key.size;
                        do
                        {
                            probe_offset += probe_size;
                            probe_size = std::min(block_size, file_size.value() - probe_offset);
                            chassert(probe_offset <= lookahead_block_end);
                        }
                        while (probe_offset < lookahead_block_end
                            && !cache->contains(
                                PageCacheKey::hashForBlock(base, probe_offset, probe_size),
                                settings.page_cache_inject_eviction));
                        inner_read_until_position = probe_offset;
                        in->setReadUntilPosition(inner_read_until_position);
                    }

                    in->seek(cache_key.offset, SEEK_SET);
                }
                else
                    chassert(!in->available());

                if (in->eof())
                    throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                        getFileName(), cache_key.offset + pos, file_size.value());

                chassert(in->position() >= piece_start && in->buffer().end() <= piece_start + piece_size);
                chassert(in->getPosition() == static_cast<off_t>(cache_key.offset + pos));

                size_t n = in->available();
                chassert(n);
                if (in->position() != piece_start)
                    memmove(piece_start, in->position(), n);
                in->position() += n;
                pos += n;
            }

            return cell;
        });
    }

    nextimpl_working_buffer_offset = file_offset_of_buffer_end - cache_key.offset;
    working_buffer = Buffer(
        chunk->data(),
        chunk->data() + std::min(chunk->size(), read_until_position - cache_key.offset));
    pos = working_buffer.begin() + nextimpl_working_buffer_offset;

    if (!internal_buffer.empty())
    {
        /// We were given an external buffer to read into. We currently don't allow this as it would
        /// require unnecessary memcpy.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CachedInMemoryReadBufferFromFile doesn't support using external buffer");
    }

    size_t size = available();
    file_offset_of_buffer_end += size;
    ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, size);

    return true;
}

std::vector<PageCache::MappedPtr> CachedInMemoryReadBufferFromFile::populateBlockRange(size_t offset, size_t n) const
{
    if (n == 0 || offset >= file_size.value())
        return {};

    size_t block_size = settings.page_cache_block_size;
    size_t end_offset = std::min(offset + n, file_size.value());

    size_t first_block_start = offset / block_size * block_size;
    size_t num_blocks = (end_offset - first_block_start + block_size - 1) / block_size;

    SipHash base_hash = cache_key.baseHash();

    bool detached_if_missing = settings.read_from_page_cache_if_exists_otherwise_bypass_cache;
    bool inject_eviction = settings.page_cache_inject_eviction;

    /// Phase 1: probe cache for all blocks, record hits.
    std::vector<PageCache::MappedPtr> cells(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i)
    {
        size_t block_start = first_block_start + i * block_size;
        size_t block_data_size = std::min(block_size, file_size.value() - block_start);
        UInt128 key_hash = PageCacheKey::hashForBlock(base_hash, block_start, block_data_size);
        cells[i] = cache->get(key_hash, inject_eviction);
    }

    /// Phase 2: coalesce consecutive misses into single fetches.
    size_t i = 0;
    while (i < num_blocks)
    {
        if (cells[i])
        {
            ++i;
            continue;
        }

        /// Scan ahead to find the extent of consecutive misses.
        size_t miss_begin = i;
        while (i < num_blocks && !cells[i])
            ++i;
        size_t miss_end = i; /// exclusive

        if (miss_end - miss_begin == 1)
        {
            /// Single-block miss: read directly into the cache cell to avoid
            /// a temporary buffer allocation and an extra memcpy.
            size_t block_start = first_block_start + miss_begin * block_size;
            size_t block_data_size = std::min(block_size, file_size.value() - block_start);
            UInt128 key_hash = PageCacheKey::hashForBlock(base_hash, block_start, block_data_size);

            cells[miss_begin] = cache->getOrSet(key_hash,
                [&]() -> PageCacheKey
                {
                    return PageCacheKey{cache_key.path, cache_key.file_version, block_start, block_data_size};
                },
                detached_if_missing, inject_eviction,
                [&](const auto & c)
                {
                    size_t bytes_read = in->readBigAt(c->data(), block_data_size, block_start, nullptr);
                    if (bytes_read < block_data_size)
                        throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                            cache_key.path, block_start + bytes_read, file_size.value());
                });
        }
        else
        {
            /// Multi-block coalesced miss: fetch the entire range in one request,
            /// then distribute into individual cache cells.
            size_t range_start = first_block_start + miss_begin * block_size;
            size_t range_end = std::min(first_block_start + miss_end * block_size, file_size.value());
            size_t range_size = range_end - range_start;

            PODArray<char> buf(range_size);
            size_t bytes_read = in->readBigAt(buf.data(), range_size, range_start, nullptr);
            if (bytes_read < range_size)
                throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                    cache_key.path, range_start + bytes_read, file_size.value());

            for (size_t j = miss_begin; j < miss_end; ++j)
            {
                size_t block_start = first_block_start + j * block_size;
                size_t block_data_size = std::min(block_size, file_size.value() - block_start);
                size_t buf_offset = block_start - range_start;
                UInt128 key_hash = PageCacheKey::hashForBlock(base_hash, block_start, block_data_size);

                cells[j] = cache->getOrSet(key_hash,
                    [&]() -> PageCacheKey
                    {
                        return PageCacheKey{cache_key.path, cache_key.file_version, block_start, block_data_size};
                    },
                    detached_if_missing, inject_eviction,
                    [&](const auto & c)
                    {
                        memcpy(c->data(), buf.data() + buf_offset, block_data_size);
                    });
            }
        }
    }

    return cells;
}

size_t CachedInMemoryReadBufferFromFile::readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t m)> & progress_callback) const
{
    size_t block_size = settings.page_cache_block_size;
    size_t end_offset = std::min(offset + n, file_size.value());
    size_t first_block_start = offset / block_size * block_size;

    auto cells = populateBlockRange(offset, n);

    size_t bytes_copied = 0;
    for (size_t i = 0; i < cells.size() && offset + bytes_copied < end_offset; ++i)
    {
        size_t block_start = first_block_start + i * block_size;
        size_t block_data_size = std::min(block_size, file_size.value() - block_start);
        size_t offset_in_block = (offset + bytes_copied > block_start) ? offset + bytes_copied - block_start : 0;
        size_t to_copy = std::min(block_data_size - offset_in_block, end_offset - (offset + bytes_copied));

        memcpy(to + bytes_copied, cells[i]->data() + offset_in_block, to_copy);
        bytes_copied += to_copy;

        ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, to_copy);

        if (progress_callback && progress_callback(bytes_copied))
            break;
    }

    return bytes_copied;
}

std::vector<SeekableReadBuffer::CachedRegion> CachedInMemoryReadBufferFromFile::readBigAtRetainCells(size_t n, size_t offset) const
{
    size_t block_size = settings.page_cache_block_size;
    size_t end_offset = std::min(offset + n, file_size.value());
    size_t first_block_start = offset / block_size * block_size;

    auto cells = populateBlockRange(offset, n);

    std::vector<CachedRegion> regions;
    size_t current_offset = offset;
    for (size_t i = 0; i < cells.size() && current_offset < end_offset; ++i)
    {
        size_t block_start = first_block_start + i * block_size;
        size_t block_data_size = std::min(block_size, file_size.value() - block_start);
        size_t offset_in_block = (current_offset > block_start) ? current_offset - block_start : 0;
        size_t usable = std::min(block_data_size - offset_in_block, end_offset - current_offset);

        const char * data_ptr = cells[i]->data() + offset_in_block;
        regions.push_back(CachedRegion{
            .handle = std::move(cells[i]),
            .data = data_ptr,
            .size = usable,
            .file_offset = current_offset,
        });

        current_offset += usable;
        ProfileEvents::increment(ProfileEvents::PageCacheReadBytes, usable);
    }

    return regions;
}

bool CachedInMemoryReadBufferFromFile::isContentCached(size_t offset, size_t /*size*/)
{
    /// Usually this is called immediately after seek()ing to `offset`.

    if (!working_buffer.empty())
    {
        chassert(chunk);
        return chunk->key.offset <= offset && chunk->key.offset + chunk->key.size > offset;
    }

    size_t block_size = settings.page_cache_block_size;
    cache_key.offset = offset / block_size * block_size;
    cache_key.size = std::min(block_size, file_size.value() - cache_key.offset);

    /// Use get() instead of contains() to populate `chunk`, so the subsequent nextImpl() call
    /// can reuse it without a second cache lookup.
    /// Use hash-based lookup to avoid recomputing hash from the full key.
    UInt128 key_hash = PageCacheKey::hashForBlock(cache_key.baseHash(), cache_key.offset, cache_key.size);
    chunk = cache->get(key_hash, settings.page_cache_inject_eviction);

    return chunk != nullptr;
}

}
