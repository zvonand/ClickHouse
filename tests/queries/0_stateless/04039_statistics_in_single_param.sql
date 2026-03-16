-- Regression test: `IN` with a single query parameter bound as a scalar must not throw
-- "Bad get: has Tuple, actual type String" in ConditionSelectivityEstimator when
-- statistics are present and use_statistics is enabled.
-- The old analyzer (enable_analyzer=0) gives the IN argument an ASTNode, so
-- tryGetConstant returns a plain String Field instead of a Tuple, and the
-- atom handler for "in" used to call value.safeGet<Tuple>() unconditionally.

SET allow_experimental_statistics = 1;
SET use_statistics = 1;
SET enable_analyzer = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    category String STATISTICS(uniq),
    val      UInt64
)
ENGINE = MergeTree
ORDER BY val;

INSERT INTO tab SELECT if(number % 2 = 0, 'alice', 'bob'), number FROM numbers(100);
OPTIMIZE TABLE tab FINAL;

-- Single scalar parameter: before the fix this threw LOGICAL_ERROR.
SET param_cat = 'alice';
SELECT count() FROM tab WHERE category IN ({cat:String});

SET param_cat = 'carol';
SELECT count() FROM tab WHERE category IN ({cat:String});

DROP TABLE tab;
