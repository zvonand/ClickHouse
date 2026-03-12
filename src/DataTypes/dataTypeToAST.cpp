#include <DataTypes/dataTypeToAST.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
#include <Common/DateLUTImpl.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

boost::intrusive_ptr<ASTDataType> dataTypeToAST(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTDataType("Nullable", dataTypeToAST(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTDataType("Array", dataTypeToAST(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    if (which.isDateTime64())
    {
        const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(data_type.get());
        auto scale = make_intrusive<ASTLiteral>(dt64->getScale());
        if (dt64->hasExplicitTimeZone())
            return makeASTDataType("DateTime64", scale, make_intrusive<ASTLiteral>(dt64->getTimeZone().getTimeZone()));
        return makeASTDataType("DateTime64", scale);
    }

    if (which.isDecimal())
        return makeASTDataType("Decimal", make_intrusive<ASTLiteral>(getDecimalPrecision(*data_type)), make_intrusive<ASTLiteral>(getDecimalScale(*data_type)));

    return makeASTDataType(data_type->getName());
}

}
