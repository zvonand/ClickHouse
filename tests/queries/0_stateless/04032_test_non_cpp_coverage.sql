-- This test exists to verify that the CI LLVM differential coverage job does not
-- fail when a PR contains only non-C++ file changes (cmake, scripts, sql tests, etc.).
-- The coverage script must skip such PRs gracefully instead of crashing.
SELECT 1;
