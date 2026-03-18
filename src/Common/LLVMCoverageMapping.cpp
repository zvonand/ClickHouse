#if defined(__ELF__) && WITH_COVERAGE

#include <Common/LLVMCoverageMapping.h>

#include <elf.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
#include <openssl/md5.h>

#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace
{

/// Compute MD5Hash the same way LLVM does: MD5 digest, return first 8 bytes as little-endian uint64.
/// This matches IndexedInstrProf::ComputeHash() which is used to compute FilenamesRef.
uint64_t computeMD5Hash(const uint8_t * data, size_t len)
{
    unsigned char digest[MD5_DIGEST_LENGTH]; // NOLINT
    MD5(data, len, digest); // NOLINT
    uint64_t result;
    memcpy(&result, digest, 8);
    return result;
}

/// Read an unsigned LEB128 value from buf, advance buf, return value.
/// Sets ok = false and returns 0 on truncated input or overflow.
uint64_t readULEB128(const uint8_t *& buf, const uint8_t * end, bool & ok)
{
    uint64_t result = 0;
    int shift = 0;
    while (buf < end)
    {
        const uint8_t byte = *buf++;
        result |= static_cast<uint64_t>(byte & 0x7fu) << shift;
        if (!(byte & 0x80u))
            return result;
        shift += 7;
        if (shift >= 64)
        {
            ok = false;
            return 0;
        }
    }
    ok = false;
    return 0;
}

/// One parsed filename table from a `__llvm_covmap` header block.
struct FilenameTable
{
    uint64_t hash;                    /// MD5Hash of the raw filename blob (= FilenamesRef in covfun records)
    std::vector<std::string> names;   /// decoded filenames
};

/// Parse all filename tables from the `__llvm_covmap` section.
/// Each block starts with a 16-byte header followed by FilenamesSize bytes of
/// compressed (or uncompressed) filename data.
///
/// Header layout (all uint32_t, little-endian):
///   NRecords     (always 0 for LLVM coverage format V4+)
///   FilenamesSize
///   CoverageSize (always 0 for V4+)
///   Version      (3 = V4, 4 = V5, 5 = V6, …)
///
/// The filename blob format (ULEB128 header + raw bytes):
///   NumFilenames   ULEB128
///   UncompressedLen ULEB128
///   CompressedLen   ULEB128
///   If CompressedLen > 0: zlib-compressed bytes
///   Else: raw uncompressed bytes
///
/// Uncompressed content for V5+ (i.e. format version 4+):
///   For each filename: ULEB128 length followed by that many bytes
std::vector<FilenameTable> parseCovMapFilenames(
    const uint8_t * covmap_data,
    size_t covmap_size)
{
    std::vector<FilenameTable> tables;

    const uint8_t * p = covmap_data;
    const uint8_t * const section_end = covmap_data + covmap_size;

    while (p < section_end)
    {
        /// Align start of each block to 8 bytes within the section.
        {
            const uintptr_t off = static_cast<uintptr_t>(p - covmap_data);
            if (off % 8 != 0)
                p += 8 - (off % 8);
        }
        if (p + 16 > section_end)
            break;

        uint32_t n_records;
        uint32_t filenames_size;
        uint32_t coverage_size;
        uint32_t version;
        memcpy(&n_records, p, 4);
        memcpy(&filenames_size, p + 4, 4);
        memcpy(&coverage_size, p + 8, 4);
        memcpy(&version, p + 12, 4);

        p += 16;

        if (p + filenames_size > section_end)
            break;

        const uint8_t * const fnames_start = p;
        const uint8_t * const fnames_end   = p + filenames_size;
        p = fnames_end;

        /// Compute MD5Hash of the raw filename blob — this is exactly what LLVM stores as FilenamesRef.
        const uint64_t block_hash = computeMD5Hash(fnames_start, filenames_size);

        /// Decode the filename blob.
        const uint8_t * fp = fnames_start;
        bool ok = true;

        const uint64_t num_filenames    = readULEB128(fp, fnames_end, ok);
        const uint64_t uncompressed_len = readULEB128(fp, fnames_end, ok);
        const uint64_t compressed_len   = readULEB128(fp, fnames_end, ok);
        if (!ok || num_filenames == 0)
            continue;

        std::string uncompressed;
        if (compressed_len > 0)
        {
            if (fp + compressed_len > fnames_end)
                continue;

            uncompressed.resize(uncompressed_len);
            uLongf dest_len = static_cast<uLongf>(uncompressed_len);
            if (uncompress(
                    reinterpret_cast<Bytef *>(uncompressed.data()),
                    &dest_len,
                    fp,
                    static_cast<uLong>(compressed_len)) != Z_OK)
                continue;

            fp += compressed_len;
        }
        else
        {
            if (fp + uncompressed_len > fnames_end)
                continue;
            uncompressed.assign(reinterpret_cast<const char *>(fp), uncompressed_len);
            fp += uncompressed_len;
        }

        /// Decode individual filenames.
        /// Format (V5 / format version ≥ 4): ULEB128 length then bytes.
        std::vector<std::string> filenames;
        filenames.reserve(static_cast<size_t>(num_filenames));

        const uint8_t * up  = reinterpret_cast<const uint8_t *>(uncompressed.data());
        const uint8_t * uend = up + uncompressed.size();

        for (uint64_t i = 0; i < num_filenames; ++i)
        {
            bool ok2 = true;
            const uint64_t len = readULEB128(up, uend, ok2);
            if (!ok2 || up + len > uend)
                break;
            filenames.emplace_back(reinterpret_cast<const char *>(up), static_cast<size_t>(len));
            up += len;
        }

        if (!filenames.empty())
            tables.push_back({block_hash, std::move(filenames)});
    }

    return tables;
}

} // anonymous namespace


std::vector<CoverageRegion> readLLVMCoverageMapping(const char * binary_path)
{
    std::vector<CoverageRegion> result;

    const int fd = ::open(binary_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return result;

    struct stat st;
    if (::fstat(fd, &st) < 0)
    {
        [[maybe_unused]] int err = ::close(fd);
        return result;
    }

    const size_t size = static_cast<size_t>(st.st_size);
    void * mapped = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    [[maybe_unused]] int err = ::close(fd); /// mmap keeps the mapping valid after close
    if (mapped == MAP_FAILED)
        return result;

    const uint8_t * const base = static_cast<const uint8_t *>(mapped);

    auto cleanup = [&]
    {
        ::munmap(mapped, size);
    };

    /// Validate ELF magic and class (64-bit only).
    if (size < sizeof(Elf64_Ehdr)
        || memcmp(base, ELFMAG, SELFMAG) != 0
        || base[EI_CLASS] != ELFCLASS64)
    {
        cleanup();
        return result;
    }

    const Elf64_Ehdr * const elf = reinterpret_cast<const Elf64_Ehdr *>(base);

    if (elf->e_shoff == 0
        || elf->e_shstrndx == SHN_UNDEF
        || elf->e_shstrndx >= elf->e_shnum)
    {
        cleanup();
        return result;
    }

    const Elf64_Shdr * const shdrs =
        reinterpret_cast<const Elf64_Shdr *>(base + elf->e_shoff);
    const char * const shstrtab =
        reinterpret_cast<const char *>(base + shdrs[elf->e_shstrndx].sh_offset);

    const uint8_t * covmap_data = nullptr;
    size_t covmap_size = 0;
    const uint8_t * covfun_data = nullptr;
    size_t covfun_size = 0;

    for (int i = 0; i < elf->e_shnum; ++i)
    {
        const Elf64_Shdr * const sh = &shdrs[i];
        const char * const name = shstrtab + sh->sh_name;
        if (strcmp(name, "__llvm_covmap") == 0)
        {
            covmap_data = base + sh->sh_offset;
            covmap_size = static_cast<size_t>(sh->sh_size);
        }
        else if (strcmp(name, "__llvm_covfun") == 0)
        {
            covfun_data = base + sh->sh_offset;
            covfun_size = static_cast<size_t>(sh->sh_size);
        }
    }

    if (!covmap_data || !covfun_data)
    {
        cleanup();
        return result;
    }

    /// Parse filename tables from `__llvm_covmap`.
    const std::vector<FilenameTable> fname_tables =
        parseCovMapFilenames(covmap_data, covmap_size);

    if (fname_tables.empty())
    {
        cleanup();
        return result;
    }

    /// Build a lookup: MD5Hash → filename list.
    /// The hash matches FilenamesRef stored in covfun records.
    std::unordered_map<uint64_t, const FilenameTable *> hash_to_table;
    hash_to_table.reserve(fname_tables.size());
    for (const FilenameTable & t : fname_tables)
        hash_to_table.emplace(t.hash, &t);

    /// Parse `__llvm_covfun` records.
    ///
    /// Each record is LLVM_PACKED (no implicit padding) and 8-byte aligned:
    ///
    ///   int64_t  NameRef        [0..7]   MD5 hash of the mangled function name
    ///   uint32_t DataSize       [8..11]  byte length of the inline CoverageMapping
    ///   uint64_t FuncHash       [12..19] function body hash
    ///   uint64_t FilenamesRef   [20..27] byte offset of the filename table in __llvm_covmap
    ///   uint8_t  CoverageMapping[DataSize] inline region encoding
    ///
    /// The inline CoverageMapping encoding (all ULEB128):
    ///   NumExpressions
    ///   2 × NumExpressions operands (LHS, RHS counter IDs — skipped)
    ///   Then one or more "file groups":
    ///     ULEB128  file_id_delta  (file index within the FilenamesRef table)
    ///     ULEB128  region_count   (number of regions that follow)
    ///     For each region:
    ///       ULEB128  delta_line_start
    ///       ULEB128  col_start
    ///       ULEB128  line_end_delta
    ///       ULEB128  col_end_combined  (col | (region_kind << 28))
    ///       ULEB128  counter_id
    ///
    /// We only need the first code region (region_kind == 0) of the first
    /// file group to obtain the source filename and line range.

    const uint8_t * p   = covfun_data;
    const uint8_t * end = covfun_data + covfun_size;

    while (p < end)
    {
        /// 8-byte alignment within the section.
        {
            const uintptr_t off = static_cast<uintptr_t>(p - covfun_data);
            if (off % 8 != 0)
                p += 8 - (off % 8);
        }
        if (p + 28 > end)
            break;

        int64_t  name_ref_raw;
        uint32_t data_size;
        uint64_t filenames_ref;

        memcpy(&name_ref_raw,  p,      8);
        memcpy(&data_size,     p + 8,  4);
        memcpy(&filenames_ref, p + 20, 8);

        const uint64_t name_hash = static_cast<uint64_t>(name_ref_raw);
        p += 28;

        if (p + data_size > end)
            break;

        const uint8_t * const mp   = p;
        const uint8_t * const mend = p + data_size;
        p += data_size;

        if (data_size == 0)
            continue;

        /// Decode the inline coverage mapping for this function.
        /// Format (all ULEB128):
        ///   1. NumFileMappings — number of files in virtual file mapping
        ///   2. NumFileMappings × FilenameIndex (index into FilenamesRef table)
        ///   3. NumExpressions
        ///   4. NumExpressions × 2 (LHS, RHS counter refs)
        ///   5. For each file (0..NumFileMappings-1):
        ///      a. NumRegions
        ///      b. For each region:
        ///         - EncodedCounterAndRegion (combined counter/kind)
        ///         - LineStartDelta (added to accumulator LineStart)
        ///         - ColumnStart
        ///         - NumLines
        ///         - ColumnEnd
        const uint8_t * cur = mp;
        bool ok = true;

        /// Step 1: virtual file mapping
        const uint64_t num_file_mappings = readULEB128(cur, mend, ok);
        if (!ok || num_file_mappings == 0)
            continue;

        uint64_t first_filename_idx = 0;
        for (uint64_t fi = 0; fi < num_file_mappings; ++fi)
        {
            const uint64_t fname_idx = readULEB128(cur, mend, ok);
            if (!ok) break;
            if (fi == 0) first_filename_idx = fname_idx;
        }
        if (!ok)
            continue;

        /// Step 2: skip expressions
        const uint64_t num_expr = readULEB128(cur, mend, ok);
        if (!ok)
            continue;
        for (uint64_t e = 0; e < num_expr * 2; ++e)
        {
            readULEB128(cur, mend, ok);
            if (!ok) break;
        }
        if (!ok || cur >= mend)
            continue;

        /// Step 3: read first file's regions to get line start/end
        const uint64_t num_regions = readULEB128(cur, mend, ok);
        if (!ok || num_regions == 0)
            continue;

        /// First region: EncodedCounterAndRegion (counter/kind), then line/col.
        /// The counter field doubles as the region kind when tag == Zero.
        const uint64_t encoded = readULEB128(cur, mend, ok);
        if (!ok) continue;

        /// Check if this is a non-code region (kind != CodeRegion).
        /// Counter::EncodingTagMask == 3; EncodingCounterTagAndExpansionRegionTagBits == 4
        /// If tag != 0 (non-zero counter), it's a code region.
        /// If tag == 0 AND bit[2] is set, it's an expansion region.
        /// If tag == 0 AND bits[31:4] encode a region kind != 0, skip.
        static constexpr uint64_t kTagMask          = 3u;           /// Counter::EncodingTagMask
        static constexpr uint64_t kExpansionBit     = 4u;           /// EncodingExpansionRegionBit
        static constexpr uint64_t kKindShift        = 4u;           /// EncodingCounterTagAndExpansionRegionTagBits
        const uint64_t tag = encoded & kTagMask;
        bool is_code_region = true;
        if (tag == 0)
        {
            if (encoded & kExpansionBit)
                is_code_region = false;  /// expansion region
            else
            {
                uint64_t kind = encoded >> kKindShift;
                /// CodeRegion = 0, SkippedRegion = 1, BranchRegion = 2, ...
                if (kind != 0)
                    is_code_region = false;
            }
        }

        if (!is_code_region)
            continue;

        /// Read line start delta, col start, num lines, col end
        const uint64_t line_start_delta = readULEB128(cur, mend, ok);
        if (!ok) continue;
        /* col_start = */ readULEB128(cur, mend, ok);
        if (!ok) continue;
        const uint64_t num_lines = readULEB128(cur, mend, ok);
        if (!ok) continue;

        const uint32_t line_start = static_cast<uint32_t>(line_start_delta);  /// delta from 0 for first region
        const uint32_t line_end   = line_start + static_cast<uint32_t>(num_lines);

        if (line_start == 0)
            continue;

        /// Resolve the filename table using FilenamesRef = MD5Hash of the covmap filename blob.
        auto it = hash_to_table.find(filenames_ref);
        if (it == hash_to_table.end())
            continue;

        const std::vector<std::string> & filenames = it->second->names;
        const size_t file_idx = static_cast<size_t>(first_filename_idx);
        if (file_idx >= filenames.size())
            continue;

        CoverageRegion region;
        region.name_hash  = name_hash;
        region.file       = filenames[file_idx];
        region.line_start = line_start;
        region.line_end   = line_end;
        result.push_back(std::move(region));
    }

    cleanup();
    return result;
}

}

#endif
