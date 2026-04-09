/// Test for dragonbox struct padding MSan issue.
///
/// `dragonbox::compute_nearest` used to declare `ReturnType ret_value;` (default-init)
/// where ReturnType = `fp_t<double,false,false>` = `{ uint64_t significand; int exponent; }`
/// This struct has 4 bytes of tail padding that are never written.
///
/// The fix is to value-initialize: `ReturnType ret_value{};`

#include <base/defines.h>
#include <base/MemorySanitizer.h>

#include <IO/WriteHelpers.h>
#include <Common/FiberStack.h>
#include <Common/Fiber.h>

#include <dragonbox/dragonbox.h>

#include <gtest/gtest.h>


/// Verify the fixed dragonbox returns a fully initialized struct.
static void __attribute__((noinline)) checkDragonboxPaddingFixed()
{
    auto result = jkj::dragonbox::to_decimal(0.1);

    /// fp_t<double, false, false> = { uint64_t significand; int exponent; /* 4 bytes padding */ }
    static_assert(sizeof(result) == 16);

#if defined(MEMORY_SANITIZER)
    /// __msan_test_shadow returns the offset of the first uninitialized byte, or -1 if all clean.
    intptr_t first_uninit = __msan_test_shadow(&result, sizeof(result));
    EXPECT_EQ(first_uninit, static_cast<intptr_t>(-1))
        << "dragonbox fp_t has uninitialized bytes starting at offset " << first_uninit;
#endif
}


/// Demonstrate the BUG: default-init of the same struct leaves padding uninitialized.
/// This mimics the original unfixed dragonbox code: `ReturnType ret_value;`
static void __attribute__((noinline)) checkDefaultInitPadding()
{
    using FP = jkj::dragonbox::unsigned_fp_t<double>;
    static_assert(sizeof(FP) == 16);

    FP result;
    result.significand = 1;
    result.exponent = -1;
    /// Padding bytes at offset 12..15 are never written — this is the bug.

#if defined(MEMORY_SANITIZER)
    intptr_t first_uninit = __msan_test_shadow(&result, sizeof(result));
    /// With default-init, expect uninitialized bytes starting at offset 12 (the padding).
    EXPECT_EQ(first_uninit, static_cast<intptr_t>(12))
        << "Expected uninitialized padding at offset 12, got " << first_uninit;
#endif
}


/// Run the checks on a fiber stack to match production conditions.
TEST(DragonboxMSan, FixedRetvalOnFiberStack)
{
    bool fiber_ran = false;

    FiberStack stack;
    Fiber fiber(stack, [&](auto & /* suspend */)
    {
        checkDragonboxPaddingFixed();
        fiber_ran = true;
    });

    fiber.resume();
    EXPECT_TRUE(fiber_ran);
}


TEST(DragonboxMSan, DefaultInitPaddingOnFiberStack)
{
    bool fiber_ran = false;

    FiberStack stack;
    Fiber fiber(stack, [&](auto & /* suspend */)
    {
        checkDefaultInitPadding();
        fiber_ran = true;
    });

    fiber.resume();
    EXPECT_TRUE(fiber_ran);
}


/// Same checks on the regular stack for comparison.
TEST(DragonboxMSan, FixedRetvalOnRegularStack)
{
    checkDragonboxPaddingFixed();
}

TEST(DragonboxMSan, DefaultInitPaddingOnRegularStack)
{
    checkDefaultInitPadding();
}
