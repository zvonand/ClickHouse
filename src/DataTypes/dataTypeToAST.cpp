#include <DataTypes/dataTypeToAST.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/IDataType.h>
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
        return makeASTDataType("DateTime64", make_intrusive<ASTLiteral>(static_cast<UInt32>(6)));

    if (which.isDecimal())
        return makeASTDataType("Decimal", make_intrusive<ASTLiteral>(getDecimalPrecision(*data_type)), make_intrusive<ASTLiteral>(getDecimalScale(*data_type)));

    return makeASTDataType(data_type->getName());
}

}
