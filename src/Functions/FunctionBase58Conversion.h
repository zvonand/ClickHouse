#pragma once

#include <Functions/FunctionBaseXXConversion.h>

#include <Common/Base58.h>

namespace DB
{
struct Base58EncodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        auto const src_length = src_column.getChars().size();
        /// Base58 has efficiency of 73% (8/11) [https://monerodocs.org/cryptography/base58/],
        /// and we take double scale to avoid any reallocation.
        constexpr auto oversize = 2;
        return static_cast<size_t>(ceil(oversize * src_length + 1));
    }

    static size_t perform(std::string_view src, UInt8 * dst)
    {
        if (src.size() == 32)
            return encodeBase58_32(reinterpret_cast<const UInt8 *>(src.data()), dst);
        else if (src.size() == 64)
            return encodeBase58_64(reinterpret_cast<const UInt8 *>(src.data()), dst);
        else
            return encodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};

struct Base58DecodeTraits
{
    template <typename Col>
    static size_t getBufferSize(Col const & src_column)
    {
        /// According to the RFC https://datatracker.ietf.org/doc/html/draft-msporny-base58-03
        /// base58 doesn't have a clean bitsequence-to-character mapping like base32 or base64.
        /// Instead, it uses division by 58 and modulo operations on big integers.
        /// In addition all the leading zeros are converted to "1"s as is.
        /// Thus, if we decode the can have at most same amount of bytes as a result.
        /// Example:
        /// "11111" (5 chars) -> b'\x00\x00\x00\x00\x00' (5 bytes)
        return src_column.getChars().size();
    }

    static std::optional<size_t> perform(std::string_view src, UInt8 * dst)
    {
        /// Try the fixed-size decoders first for inputs whose encoded length is
        /// compatible, but fall back to the generic decoder if they reject the
        /// value (e.g. a 33-char string that decodes to 24 bytes, not 32).
        if (src.size() >= 32 && src.size() <= BASE58_ENCODED_32_LEN)
        {
            if (auto res = decodeBase58_32(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst))
                return res;
        }
        else if (src.size() >= 64 && src.size() <= BASE58_ENCODED_64_LEN)
            if (auto res = decodeBase58_64(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst))
                return res;
        return decodeBase58(reinterpret_cast<const UInt8 *>(src.data()), src.size(), dst);
    }
};
}
