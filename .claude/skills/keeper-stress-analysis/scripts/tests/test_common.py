"""Tests for the small pure functions in `_common`. Stdlib `unittest` only,
no extra deps. Run with:

    cd .claude/skills/keeper-stress-analysis/scripts
    python3 -m unittest tests.test_common -v

These two functions are the riskiest pieces of the rubric:

  - `classify` encodes the per-metric significance bands documented in
    `references/methodology.md`. A regression here changes every per-PR
    verdict.
  - `iso_week` underpins the PR-branch pool widening across year/W01/W52-53
    boundaries. A regression here silently shrinks the pool for any PR
    landing in the first or last ISO week of a year.
"""
import datetime
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from _common import classify, iso_week  # noqa: E402


class ClassifyBandsTest(unittest.TestCase):
    """Mirror the rubric in `references/methodology.md`. If these
    expectations and the doc drift apart, change one to match the other —
    the rubric is the binding contract.
    """

    # ---- rps (higher_better, 5/15) ----

    def test_rps_clean_within_band(self):
        # 5 % drop sits exactly on the clean/watch boundary; clean wins.
        self.assertEqual(classify("rps", 100.0, 95.0), "clean")

    def test_rps_watch_band(self):
        # 10 % drop is in watch (-5 %..-15 %).
        self.assertEqual(classify("rps", 100.0, 90.0), "watch")

    def test_rps_regression_below_watch(self):
        # 16 % drop crosses regression.
        self.assertEqual(classify("rps", 100.0, 84.0), "regression")

    def test_rps_clean_when_improved(self):
        # Higher is better — improvements stay clean.
        self.assertEqual(classify("rps", 100.0, 120.0), "clean")

    # ---- read_p99_ms (lower_better, 10/30 — wider band than rps) ----

    def test_read_p99_clean_within_wider_band(self):
        # +9 % rise on p99 is still clean (band is 10 %, not 5 %).
        self.assertEqual(classify("read_p99_ms", 10.0, 10.9), "clean")

    def test_read_p99_watch_band(self):
        # +20 % rise on p99 is watch (10 %..30 %).
        self.assertEqual(classify("read_p99_ms", 10.0, 12.0), "watch")

    def test_read_p99_regression_above_watch(self):
        # +35 % rise on p99 crosses regression.
        self.assertEqual(classify("read_p99_ms", 10.0, 13.5), "regression")

    # ---- peak_mem_gb (lower_better, 10/30) ----

    def test_peak_mem_watch_band(self):
        # +25 % memory rise is watch.
        self.assertEqual(classify("peak_mem_gb", 1.0, 1.25), "watch")

    # ---- error_pct (absolute PP, not relative %) ----

    def test_error_pct_small_rise_is_clean(self):
        # +0.04 PP rise is below the 0.05 PP noise threshold.
        self.assertEqual(classify("error_pct", 0.0, 0.04), "clean")

    def test_error_pct_mid_rise_is_watch(self):
        # +0.1 PP is in (0.05, 0.5) → watch.
        self.assertEqual(classify("error_pct", 0.0, 0.1), "watch")

    def test_error_pct_large_rise_is_regression(self):
        # +0.6 PP crosses regression.
        self.assertEqual(classify("error_pct", 0.0, 0.6), "regression")

    # ---- edge cases ----

    def test_no_data_when_pre_or_post_missing(self):
        self.assertEqual(classify("rps", None, 100.0), "no-data")
        self.assertEqual(classify("rps", 100.0, None), "no-data")

    def test_pre_zero_clean_when_post_also_zero(self):
        self.assertEqual(classify("rps", 0.0, 0.0), "clean")

    def test_pre_zero_watch_when_post_nonzero(self):
        self.assertEqual(classify("rps", 0.0, 5.0), "watch")


class IsoWeekTest(unittest.TestCase):
    """Verify the year-boundary cases that the post-`4692e7` widening
    fix relies on. A run on Mon 2026-01-05 (`2026-W02`) should look
    back to Mon 2025-12-29 (`2026-W01` per ISO calendar quirks) and
    forward to Mon 2026-01-12 (`2026-W03`); a run in `2025-W52` should
    cleanly cross into `2026-W01` etc.
    """

    @staticmethod
    def _day(y, m, d):
        return datetime.datetime(y, m, d, tzinfo=datetime.timezone.utc)

    def test_format_is_yyyy_dash_w_two_digits(self):
        self.assertEqual(iso_week(self._day(2026, 4, 1)), "2026-W14")

    def test_year_rollover_backward(self):
        # 2026-01-05 is 2026-W02 by ISO. -7 days lands 2025-12-29 which
        # ISO also calls 2026-W01 (Mon-of-week-that-contains-Jan-1).
        d = self._day(2026, 1, 5)
        self.assertEqual(iso_week(d), "2026-W02")
        self.assertEqual(iso_week(d - datetime.timedelta(days=7)), "2026-W01")
        self.assertEqual(iso_week(d - datetime.timedelta(days=14)), "2025-W52")

    def test_year_rollover_forward(self):
        # 2025-12-29 is the start of 2026-W01 by ISO.
        d = self._day(2025, 12, 29)
        self.assertEqual(iso_week(d), "2026-W01")
        self.assertEqual(iso_week(d + datetime.timedelta(days=7)), "2026-W02")

    def test_w53_year(self):
        # 2026 has 53 ISO weeks. 2026-12-31 is in W53.
        self.assertEqual(iso_week(self._day(2026, 12, 31)), "2026-W53")


if __name__ == "__main__":
    unittest.main(verbosity=2)
