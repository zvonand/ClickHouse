#include <libdivide-config.h>
#include <libdivide.h>

#include <base/types.h>

namespace DB
{

template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
#if defined(LIBDIVIDE_AVX2)
    if constexpr (sizeof(A) >= 8)
    {
        /// For 64-bit types on AVX2, auto-vectorization degrades to scalar
        /// extract/insert (no vpmuludq for 64-bit).  Use BRANCHFREE with
        /// libdivide's hand-tuned divide(__m256i).
        libdivide::divider<A, libdivide::BRANCHFREE> divider(static_cast<A>(b));
        const A * a_end = a_pos + size;

        static constexpr size_t values_per_register = 32 / sizeof(A);
        const A * a_end_simd = a_pos + size / values_per_register * values_per_register;

        while (a_pos < a_end_simd)
        {
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(c_pos),
                _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a_pos)) / divider);
            a_pos += values_per_register;
            c_pos += values_per_register;
        }

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
    else
#endif
    {
        /// BRANCHFULL: the per-iteration algorithm-type branch is perfectly
        /// predicted (loop-invariant) and lets most divisors take the fast
        /// `mullhi >> shift` path (2 ops vs BRANCHFREE's 5 ops).  The
        /// compiler auto-vectorizes each path independently (vpmuludq on
        /// x86 for 32-bit, NEON on ARM).
        libdivide::divider<A, libdivide::BRANCHFULL> divider(static_cast<A>(b));
        for (size_t i = 0; i < size; ++i)
            c_pos[i] = a_pos[i] / divider;
    }
}

template void divideImpl<UInt64, UInt64, UInt64>(const UInt64 * __restrict, UInt64, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt32, UInt64>(const UInt64 * __restrict, UInt32, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt16, UInt64>(const UInt64 * __restrict, UInt16, UInt64 * __restrict, size_t);
template void divideImpl<UInt64, UInt8, UInt64>(const UInt64 * __restrict, UInt8, UInt64 * __restrict, size_t);

template void divideImpl<UInt32, UInt64, UInt32>(const UInt32 * __restrict, UInt64, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt32, UInt32>(const UInt32 * __restrict, UInt32, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt16, UInt32>(const UInt32 * __restrict, UInt16, UInt32 * __restrict, size_t);
template void divideImpl<UInt32, UInt8, UInt32>(const UInt32 * __restrict, UInt8, UInt32 * __restrict, size_t);

template void divideImpl<Int64, Int64, Int64>(const Int64 * __restrict, Int64, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int32, Int64>(const Int64 * __restrict, Int32, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int16, Int64>(const Int64 * __restrict, Int16, Int64 * __restrict, size_t);
template void divideImpl<Int64, Int8, Int64>(const Int64 * __restrict, Int8, Int64 * __restrict, size_t);

template void divideImpl<Int32, Int64, Int32>(const Int32 * __restrict, Int64, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int32, Int32>(const Int32 * __restrict, Int32, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int16, Int32>(const Int32 * __restrict, Int16, Int32 * __restrict, size_t);
template void divideImpl<Int32, Int8, Int32>(const Int32 * __restrict, Int8, Int32 * __restrict, size_t);

}
