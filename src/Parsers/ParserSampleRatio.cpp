#include <IO/ReadHelpers.h>
#include <Parsers/ASTSampleRatio.h>
#include <Parsers/ParserSampleRatio.h>


namespace DB
{


static ASTSampleRatio::BigNum bigIntExp10(int x)
{
    ASTSampleRatio::BigNum result = 1;
    for (int i = 0; i < x; ++i)
        result *= 10;
    return result;
}

static bool parseDecimal(const char * pos, const char * end, ASTSampleRatio::Rational & res)
{
    ASTSampleRatio::BigNum num_before = 0;
    ASTSampleRatio::BigNum num_after = 0;
    Int32 exponent = 0;

    const char * pos_after_first_num = tryReadIntText(num_before, pos, end);

    bool has_num_before_point = pos_after_first_num > pos;
    pos = pos_after_first_num;
    bool has_point = pos < end && *pos == '.';

    if (has_point)
        ++pos;

    if (!has_num_before_point && !has_point)
        return false;

    int number_of_digits_after_point = 0;

    if (has_point)
    {
        const char * pos_after_second_num = tryReadIntText(num_after, pos, end);
        number_of_digits_after_point = static_cast<int>(pos_after_second_num - pos);
        pos = pos_after_second_num;
    }

    bool has_exponent = pos < end && (*pos == 'e' || *pos == 'E');

    if (has_exponent)
    {
        ++pos;
        const char * pos_after_exponent = tryReadIntText(exponent, pos, end);

        if (pos_after_exponent == pos)
            return false;
    }

    res.numerator = num_before * bigIntExp10(number_of_digits_after_point) + num_after;
    res.denominator = bigIntExp10(number_of_digits_after_point);

    if (exponent > 0)
        res.numerator *= bigIntExp10(exponent);
    if (exponent < 0)
        res.denominator *= bigIntExp10(-exponent);

    /// NOTE You do not need to remove the common power of ten from the numerator and denominator.
    return true;
}


/** Possible options:
  *
  * 12345
  * - an integer
  *
  * 0.12345
  * .12345
  * 0.
  * - fraction in ordinary decimal notation
  *
  * 1.23e-1
  * - fraction in scientific decimal notation
  *
  * 123 / 456
  * - fraction with an ordinary denominator
  *
  * Just in case, in the numerator and denominator of the fraction, we support the previous cases.
  * Example:
  * 123.0 / 456e0
  */
bool ParserSampleRatio::parseImpl(Pos & pos, ASTPtr & node, Expected &)
{
    ASTSampleRatio::Rational numerator;
    ASTSampleRatio::Rational denominator;
    ASTSampleRatio::Rational res;

    if (!parseDecimal(pos->begin, pos->end, numerator))
        return false;
    ++pos;

    bool has_slash = pos->type == TokenType::Slash;

    if (has_slash)
    {
        ++pos;

        if (!parseDecimal(pos->begin, pos->end, denominator))
            return false;
        ++pos;

        res.numerator = numerator.numerator * denominator.denominator;
        res.denominator = numerator.denominator * denominator.numerator;
    }
    else
    {
        res = numerator;
    }

    node = make_intrusive<ASTSampleRatio>(res);
    return true;
}

}
