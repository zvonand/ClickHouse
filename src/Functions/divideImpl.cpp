#include <libdivide-config.h>
#include <libdivide.h>

#include <base/types.h>

namespace DB
{

template <typename A, typename B, typename ResultType>
void divideImpl(const A * __restrict a_pos, B b, ResultType * __restrict c_pos, size_t size)
{
    /// BRANCHFREE: the divisor is loop-invariant, so the per-iteration
    /// algorithm-type branch in BRANCHFULL is pure overhead.  BRANCHFREE
    /// produces a straight-line multiply-shift sequence that the compiler
    /// auto-vectorizes to the widest available SIMD.
    libdivide::divider<A, libdivide::BRANCHFREE> divider(static_cast<A>(b));

    /// For 64-bit types, auto-vectorization degrades to scalar
    /// extract/insert sequences.  Use libdivide's explicit AVX2 path
    /// which has hand-tuned 64-bit vector implementations.
    if constexpr (sizeof(A) == 8)
    {
        const A * a_end = a_pos + size;

#if defined(LIBDIVIDE_AVX2)
        static constexpr size_t values_per_register = 32 / sizeof(A);
        const A * a_end_simd = a_pos + size / values_per_register * values_per_register;

        while (a_pos < a_end_simd)
        {
            _mm256_storeu_si256(reinterpret_cast<__m256i *>(c_pos),
                _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a_pos)) / divider);
            a_pos += values_per_register;
            c_pos += values_per_register;
        }
#endif

        while (a_pos < a_end)
        {
            *c_pos = *a_pos / divider;
            ++a_pos;
            ++c_pos;
        }
    }
    else
    {
        /// For 32-bit (and narrower) types, the compiler auto-vectorizes
        /// the branchfree multiply-shift sequence efficiently via vpmuludq.
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
