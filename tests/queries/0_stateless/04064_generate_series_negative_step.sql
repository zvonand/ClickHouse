-- Descending series with negative step
SELECT * FROM generate_series(9, 0, -1);
SELECT * FROM generate_series(10, 0, -3);
SELECT * FROM generate_series(99, 0, -1) LIMIT 5;

-- Empty result: negative step but start < stop
SELECT count() FROM generate_series(0, 10, -1);

-- Empty result: positive step but start > stop (existing behavior)
SELECT count() FROM generate_series(10, 0, 1);

-- Step of -1 with equal start and stop
SELECT * FROM generate_series(5, 5, -1);

-- Larger negative step
SELECT * FROM generate_series(100, 0, -25);

-- Count with negative step
SELECT count() FROM generate_series(99, 0, -1);
SELECT count() FROM generate_series(1000, 0, -3);

-- Sum with negative step
SELECT sum(generate_series) FROM generate_series(10, 0, -1);

-- Zero step should error
SELECT * FROM generate_series(0, 10, 0); -- { serverError INVALID_SETTING_VALUE }
