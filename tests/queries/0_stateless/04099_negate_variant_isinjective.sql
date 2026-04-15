-- Tags: no-fasttest
-- Verify that negate works correctly with Variant arguments and that isInjective
-- is consistent (the function is injective regardless of Variant wrapping).
-- https://github.com/ClickHouse/ClickHouse/issues/102542

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

SELECT negate(a) FROM system.one ARRAY JOIN CAST([3, -11, 0, -3, 5] AS Array(Variant(Int8, UUID))) AS a;
