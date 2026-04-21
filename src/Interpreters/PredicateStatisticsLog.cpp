#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PredicateStatisticsLog.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/getFQDNOrHostName.h>


namespace DB
{

ColumnsDescription PredicateStatisticsLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;

    auto lc_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto array_lc_string = std::make_shared<DataTypeArray>(lc_string);
    auto array_uint64 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    auto array_float64 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());

    return ColumnsDescription
    {
        {
            "hostname",
            lc_string,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Timestamp when this log entry was written."
        },
        {
            "database",
            lc_string,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Database name of the target table."
        },
        {
            "table",
            lc_string,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Table name of the target table."
        },
        {
            "query_id",
            std::make_shared<DataTypeString>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Query ID for linking back to query_log."
        },
        {
            "filter_expression",
            std::make_shared<DataTypeString>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Full filter expression pushed to the source step."
        },
        {
            "predicate_expression",
            std::make_shared<DataTypeString>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Whole filter expression handled by this prewhere/filter step (ActionsDAG dump)."
        },
        {
            "input_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Rows entering this prewhere/filter step."
        },
        {
            "passed_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Rows surviving this prewhere/filter step."
        },
        {
            "filter_selectivity",
            std::make_shared<DataTypeFloat64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Selectivity of this step: passed_rows / input_rows."
        },

        {
            "total_input_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Rows entering the first prewhere step (total rows read from granules)."
        },
        {
            "total_passed_rows",
            std::make_shared<DataTypeUInt64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Rows surviving all prewhere steps (rows delivered to the query)."
        },
        {
            "total_selectivity",
            std::make_shared<DataTypeFloat64>(),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Selectivity of the whole predicate: total_passed_rows / total_input_rows."
        },

        {
            "index_names",
            array_lc_string,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Names of indexes applied, e.g. ['PrimaryKey', 'idx_bf_status'] (index rows only)."
        },
        {
            "index_types",
            array_lc_string,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Types of indexes applied: PrimaryKey, Skip, MinMax, Partition (index rows only)."
        },
        {
            "total_granules",
            array_uint64,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Granules entering each index stage (index rows only)."
        },
        {
            "granules_after",
            array_uint64,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Granules remaining after each index stage (index rows only)."
        },
        {
            "index_selectivities",
            array_float64,
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Per-index selectivity: granules_after / total_granules (index rows only)."
        }
    };
}

void PredicateStatisticsLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(event_date);
    columns[i++]->insert(event_time);
    columns[i++]->insert(database);
    columns[i++]->insert(table);
    columns[i++]->insert(query_id);
    columns[i++]->insert(filter_expression);
    columns[i++]->insert(predicate_expression);

    columns[i++]->insert(input_rows);
    columns[i++]->insert(passed_rows);
    columns[i++]->insert(filter_selectivity);

    columns[i++]->insert(total_input_rows);
    columns[i++]->insert(total_passed_rows);
    columns[i++]->insert(total_selectivity);

    /// index-level arrays
    auto fill_string_array = [](const std::vector<String> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & lc_data = typeid_cast<ColumnLowCardinality &>(arr_col.getData());
        for (const auto & val : data)
            lc_data.insertData(val.data(), val.size());
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    auto fill_uint64_array = [](const std::vector<UInt64> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & num_data = typeid_cast<ColumnUInt64 &>(arr_col.getData()).getData();
        for (auto val : data)
            num_data.push_back(val);
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    auto fill_float64_array = [](const std::vector<Float64> & data, IColumn & column)
    {
        auto & arr_col = typeid_cast<ColumnArray &>(column);
        auto & num_data = typeid_cast<ColumnFloat64 &>(arr_col.getData()).getData();
        for (auto val : data)
            num_data.push_back(val);
        arr_col.getOffsets().push_back(arr_col.getOffsets().back() + data.size());
    };

    fill_string_array(index_names, *columns[i++]);
    fill_string_array(index_types, *columns[i++]);
    fill_uint64_array(total_granules, *columns[i++]);
    fill_uint64_array(granules_after, *columns[i++]);
    fill_float64_array(index_selectivities, *columns[i++]);
}

}
