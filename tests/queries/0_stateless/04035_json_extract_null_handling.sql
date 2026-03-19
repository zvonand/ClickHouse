-- Tests for JSONExtract null handling paths that were missing coverage.
-- Covers: NumericNode, LowCardinalityNumericNode, StringNode, LowCardinalityStringNode,
-- FixedStringNode, LowCardinalityFixedStringNode, UUIDNode, LowCardinalityUUIDNode,
-- DateNode, DateTimeNode, DateTime64Node, TimeNode, Time64Node, DecimalNode,
-- IPv4Node, IPv6Node, EnumNode, ArrayNode, TupleNode null handling paths,
-- and jsonElementToString null case (L127-131).

-- NumericNode null handling (L244-253 in JSONExtractTree.cpp)
SELECT JSONExtract('null', 'Int32');                                             -- null_as_default=1: inserts 0
SELECT JSONExtract('null', 'Int32') SETTINGS input_format_null_as_default=0;    -- error path: outer insertDefault → 0

-- LowCardinalityNumericNode null handling (L299-308)
SELECT JSONExtract('null', 'LowCardinality(Int32)') SETTINGS allow_suspicious_low_cardinality_types=1;
SELECT JSONExtract('null', 'LowCardinality(Float32)') SETTINGS allow_suspicious_low_cardinality_types=1;
SELECT JSONExtract('null', 'LowCardinality(Int32)') SETTINGS allow_suspicious_low_cardinality_types=1, input_format_null_as_default=0;

-- LowCardinalityNumericNode with is_nullable=true (L301): inserts NULL
SELECT JSONExtract('null', 'LowCardinality(Nullable(Int32))');

-- StringNode null handling (L354-359)
SELECT JSONExtract('null', 'String');
SELECT JSONExtract('null', 'String') SETTINGS input_format_null_as_default=0;

-- LowCardinalityStringNode null handling (L400-409)
SELECT JSONExtract('null', 'LowCardinality(String)');
SELECT JSONExtract('null', 'LowCardinality(String)') SETTINGS input_format_null_as_default=0;

-- FixedStringNode null handling (L443-453)
SELECT hex(JSONExtract('null', 'FixedString(3)'));
SELECT hex(JSONExtract('null', 'FixedString(3)')) SETTINGS input_format_null_as_default=0;

-- LowCardinalityFixedStringNode null handling (L495-503)
SELECT hex(JSONExtract('null', 'LowCardinality(FixedString(3))')) SETTINGS allow_suspicious_low_cardinality_types=1;

-- LowCardinalityFixedStringNode value padding (L533-537): size < fixed_length
SELECT hex(JSONExtract('{"a": "H"}', 'a', 'LowCardinality(FixedString(3))')) SETTINGS allow_suspicious_low_cardinality_types=1;

-- UUIDNode null handling (L557-560)
SELECT JSONExtract('null', 'UUID');
SELECT JSONExtract('null', 'UUID') SETTINGS input_format_null_as_default=0;

-- LowCardinalityUUIDNode null handling (L601-605)
SELECT JSONExtract('null', 'LowCardinality(UUID)') SETTINGS allow_suspicious_low_cardinality_types=1;

-- DateNode null handling (L641-643) + jsonElementToString null case (L127-131)
SELECT JSONExtract('null', 'Date');
-- null_as_default=0: DateNode calls jsonElementToString(null) for error message, triggers L127-131
SELECT JSONExtract('null', 'Date') SETTINGS input_format_null_as_default=0;

-- DateTimeNode null handling (L680-683): use UTC to avoid timezone dependency
SELECT JSONExtract('null', 'DateTime(''UTC'')');

-- DateTime64Node null handling (L876-879)
SELECT JSONExtract('null', 'DateTime64(3, ''UTC'')');

-- TimeNode: completely uncovered (L733-782)
SELECT JSONExtract('{"a": "12:30:00"}', 'a', 'Time');
SELECT JSONExtract('null', 'Time');
SELECT JSONExtract('null', 'Time') SETTINGS input_format_null_as_default=0;
-- TimeNode error path for non-string non-uint64 element (L763-766)
SELECT JSONExtract('{"a": true}', 'a', 'Time');
-- TimeNode error path for unparseable string (L754-757)
SELECT JSONExtract('{"a": "not_a_time"}', 'a', 'Time');

-- Time64Node: completely uncovered (L908-1007)
SELECT JSONExtract('{"a": "12:30:00.123"}', 'a', 'Time64(3)');
SELECT JSONExtract('null', 'Time64(3)');
SELECT JSONExtract('null', 'Time64(3)') SETTINGS input_format_null_as_default=0;

-- DecimalNode: DOUBLE path (L802-817), UINT64 path (L819-820), NULL_VALUE path (L835-843)
SELECT JSONExtract('{"a": 1.5}', 'a', 'Decimal32(2)');                          -- DOUBLE path
SELECT JSONExtract('{"a": 42}', 'a', 'Decimal32(2)');                           -- INT64 path
SELECT JSONExtract('{"a": 9223372036854775808}', 'a', 'Decimal128(0)');          -- UINT64 path (L819-820)
SELECT JSONExtract('null', 'Decimal32(3)');                                      -- NULL_VALUE null_as_default=1
SELECT JSONExtract('null', 'Decimal32(3)') SETTINGS input_format_null_as_default=0; -- NULL_VALUE error path

-- IPv4Node null handling (L1106-1109) + non-string error (L1112-1115, calls jsonElementToString for error msg)
SELECT JSONExtract('null', 'IPv4');
SELECT JSONExtract('null', 'IPv4') SETTINGS input_format_null_as_default=0;
-- IPv4Node non-string: triggers L1112-1115 (calls jsonElementToString(number))
SELECT JSONExtract('{"a": 123}', 'a', 'IPv4');

-- IPv6Node null handling (L1148-1151) + non-string error (L1154-1157)
SELECT JSONExtract('null', 'IPv6');
SELECT JSONExtract('null', 'IPv6') SETTINGS input_format_null_as_default=0;

-- EnumNode null handling (L1036-1040 with null_as_default=1, L1043-1044 error path)
-- Use Enum8('a'=0, 'b'=1) so insertDefault(0) maps to valid 'a'.
SELECT JSONExtract('null', 'Enum8(''a''=0, ''b''=1)');                                           -- null_as_default=1: returns first enum value 'a'
SELECT JSONExtract('null', 'Enum8(''a''=0, ''b''=1)') SETTINGS input_format_null_as_default=0;  -- error → outer insertDefault(0) → 'a'
-- EnumNode int64 not in enum (L1051-1054)
SELECT JSONExtract('{"a": 99}', 'a', 'Enum8(''a''=0, ''b''=1)');
-- EnumNode string not in enum (L1075-1078)
SELECT JSONExtract('{"a": "z"}', 'a', 'Enum8(''a''=0, ''b''=1)');
-- EnumNode other type error (L1083-1084): boolean maps to neither string/int64/uint64
SELECT JSONExtract('{"a": [1,2]}', 'a', 'Enum8(''a''=0, ''b''=1)');

-- ArrayNode null handling (L1258-1262)
SELECT JSONExtract('null', 'Array(Int32)');
SELECT JSONExtract('null', 'Array(Int32)') SETTINGS input_format_null_as_default=0;

-- TupleNode null handling (L1326-1329)
SELECT JSONExtract('null', 'Tuple(Int32, String)');

-- LowCardinalityNode (generic) null handling (L1225-1229) - Date in LowCardinality with null
SELECT JSONExtract('null', 'LowCardinality(Nullable(Date))');
