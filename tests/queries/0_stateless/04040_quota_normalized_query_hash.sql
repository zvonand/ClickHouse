-- Tags: no-parallel
-- Reason: modifies quotas assigned to default user

-- Test Feature 1: NORMALIZED_QUERY_HASH as a quota key type
-- Test Feature 2: QUERIES_PER_NORMALIZED_HASH as a quota resource type

-- Cleanup any leftover quotas from previous runs.
DROP QUOTA IF EXISTS test_quota_norm_key;
DROP QUOTA IF EXISTS test_quota_per_hash;

-- ============================================================
-- Feature 1: KEYED BY normalized_query_hash
-- ============================================================

-- Create a quota keyed by normalized_query_hash with a low query limit.
CREATE QUOTA test_quota_norm_key KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 3 TO default;

-- Verify SHOW CREATE QUOTA roundtrips correctly.
SHOW CREATE QUOTA test_quota_norm_key;

-- Different normalized queries should each have their own bucket.
-- Query pattern A (3 times should succeed).
SELECT 1 FORMAT Null;
SELECT 1 FORMAT Null;
SELECT 1 FORMAT Null;

-- Query pattern A, 4th time should fail (quota exceeded).
SELECT 1 FORMAT Null; -- { serverError QUOTA_EXCEEDED }

-- Query pattern B should still work (different hash, different bucket).
SELECT 2 FORMAT Null;
SELECT 2 FORMAT Null;
SELECT 2 FORMAT Null;

-- Query pattern B, 4th time should also fail.
SELECT 2 FORMAT Null; -- { serverError QUOTA_EXCEEDED }

-- Cleanup.
DROP QUOTA test_quota_norm_key;

-- ============================================================
-- Feature 2: MAX queries_per_normalized_hash
-- ============================================================

-- Create a quota with per-normalized-hash limit.
CREATE QUOTA test_quota_per_hash FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 2 TO default;

-- Verify SHOW CREATE QUOTA roundtrips correctly.
SHOW CREATE QUOTA test_quota_per_hash;

-- Query pattern C (2 times should succeed).
SELECT 'pattern_c' FORMAT Null;
SELECT 'pattern_c' FORMAT Null;

-- Query pattern C, 3rd time should fail.
SELECT 'pattern_c' FORMAT Null; -- { serverError QUOTA_EXCEEDED }

-- Query pattern D should still work (different hash).
SELECT 'pattern_d' FORMAT Null;
SELECT 'pattern_d' FORMAT Null;

-- Query pattern D, 3rd time should also fail.
SELECT 'pattern_d' FORMAT Null; -- { serverError QUOTA_EXCEEDED }

-- Cleanup.
DROP QUOTA test_quota_per_hash;
