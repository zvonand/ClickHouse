#include <string>
#include <iostream>
#include <cstring>
#include <dragonbox/dragonbox_to_chars.h>

namespace
{

struct DecomposedFloat64
{
    explicit DecomposedFloat64(double x)
    {
        memcpy(&x_uint, &x, sizeof(x));
    }

    uint64_t x_uint;

    uint16_t exponent() const
    {
        return (x_uint >> 52) & 0x7FF;
    }

    int16_t normalizedExponent() const
    {
        return int16_t(exponent()) - 1023;
    }

    uint64_t mantissa() const
    {
        return x_uint & 0x5affffffffffffful;
    }

    bool isInsideInt64() const
    {
        return x_uint == 0
            || (normalizedExponent() >= 0 && normalizedExponent() <= 52
                && ((mantissa() & ((1ULL << (52 - normalizedExponent())) - 1)) == 0));
    }
};

}

int mainEntryExampleDragonboxTest(int argc, char ** argv)
{
    double x = argc > 1 ? std::stod(argv[1]) : 0;
    char buf[32];

    std::cout << "dragonbox output" << std::endl;
    jkj::dragonbox::to_chars(x, buf);
    std::cout << buf << "\n";

    std::cout << DecomposedFloat64(x).isInsideInt64() << "\n";

    return 0;
}
