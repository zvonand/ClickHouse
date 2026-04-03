-- Regression test: arrayDotProduct must handle empty arrays without UB.
-- Previously, &data[offset] with offset == size() was undefined behavior.

-- Empty arrays of various types
SELECT arrayDotProduct([]::Array(Float32), []::Array(Float32));
SELECT arrayDotProduct([]::Array(Float64), []::Array(Float64));
SELECT arrayDotProduct([]::Array(UInt8), []::Array(UInt8));

-- Mixed empty/non-empty via table (exercises per-row offset logic)
SELECT arrayDotProduct(x, y) FROM VALUES('x Array(Float32), y Array(Float32)',
    ([], []),
    ([1, 2, 3], [4, 5, 6]),
    ([], []),
    ([10], [20]));
