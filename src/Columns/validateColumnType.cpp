#include <Columns/validateColumnType.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

bool columnMatchesType(const IColumn & column, const IDataType & type)
{
    const IColumn * col = &column;

    /// Strip wrappers that don't change the logical type.
    if (const auto * col_const = typeid_cast<const ColumnConst *>(col))
        col = &col_const->getDataColumn();
    if (const auto * col_sparse = typeid_cast<const ColumnSparse *>(col))
        col = &col_sparse->getValuesColumn();

    if (col->getDataType() != type.getColumnType())
        return false;

    if (const auto * col_array = typeid_cast<const ColumnArray *>(col))
    {
        if (const auto * type_array = typeid_cast<const DataTypeArray *>(&type))
            return columnMatchesType(col_array->getData(), *type_array->getNestedType());
        return false;
    }

    if (const auto * col_nullable = typeid_cast<const ColumnNullable *>(col))
    {
        if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(&type))
            return columnMatchesType(col_nullable->getNestedColumn(), *type_nullable->getNestedType());
        return false;
    }

    if (const auto * col_tuple = typeid_cast<const ColumnTuple *>(col))
    {
        if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(&type))
        {
            const auto & type_elements = type_tuple->getElements();
            if (col_tuple->tupleSize() != type_elements.size())
                return false;
            for (size_t i = 0; i < col_tuple->tupleSize(); ++i)
                if (!columnMatchesType(col_tuple->getColumn(i), *type_elements[i]))
                    return false;
            return true;
        }
        return false;
    }

    if (const auto * col_map = typeid_cast<const ColumnMap *>(col))
    {
        if (const auto * type_map = typeid_cast<const DataTypeMap *>(&type))
            return columnMatchesType(col_map->getNestedColumn(), *type_map->getNestedType());
        return false;
    }

    return true;
}

}
