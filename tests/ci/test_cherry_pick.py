#!/usr/bin/env python3
"""Unit tests for the bot-label filtering logic in `cherry_pick.py`.

Run with:
    cd tests/ci && python3 -m unittest test_cherry_pick.py
"""

import unittest

from cherry_pick import _bot_added_labels, _is_bot_actor


class _Actor:
    """Minimal stand-in for `github.NamedUser.NamedUser`."""

    def __init__(self, login, type_="User"):
        self.login = login
        self.type = type_


class _Label:
    def __init__(self, name):
        self.name = name


class _Event:
    def __init__(self, event, label=None, actor=None):
        self.event = event
        self.label = label
        self.actor = actor


class _Issue:
    def __init__(self, events):
        self._events = events

    def get_events(self):
        return iter(self._events)


class _RaisingIssue:
    """Simulates an events API call that fails — `get_events` raises the same
    way `pr.as_issue().get_events()` would surface a transport error from
    PyGithub."""

    def get_events(self):
        raise RuntimeError("simulated events API failure")


class _PR:
    def __init__(self, issue):
        self._issue = issue

    def as_issue(self):
        return self._issue


class IsBotActorTest(unittest.TestCase):
    def test_none_actor(self):
        self.assertFalse(_is_bot_actor(None))

    def test_github_app_bot(self):
        self.assertTrue(_is_bot_actor(_Actor("dependabot[bot]", type_="Bot")))

    def test_login_with_bot_suffix(self):
        self.assertTrue(_is_bot_actor(_Actor("renovate[bot]")))

    def test_robot_prefix(self):
        self.assertTrue(_is_bot_actor(_Actor("robot-clickhouse-ci-1")))

    def test_clickhouse_gh_prefix(self):
        self.assertTrue(_is_bot_actor(_Actor("clickhouse-gh")))

    def test_explicit_ai_allowlist(self):
        self.assertTrue(_is_bot_actor(_Actor("groeneai")))
        self.assertTrue(_is_bot_actor(_Actor("clickgapai")))

    def test_human_login_ending_in_ai_is_not_a_bot(self):
        # Regression guard: a human username such as `kai` must not be treated
        # as a bot, otherwise their manually-added must-backport labels would
        # be silently dropped.
        self.assertFalse(_is_bot_actor(_Actor("kai")))
        self.assertFalse(_is_bot_actor(_Actor("mikai")))

    def test_human_login(self):
        self.assertFalse(_is_bot_actor(_Actor("alexey-milovidov")))


class BotAddedLabelsTest(unittest.TestCase):
    LABEL = "pr-must-backport"
    OTHER_LABEL = "v25.10-must-backport"

    def _pr(self, *events):
        return _PR(_Issue(list(events)))

    def test_empty_labels_of_interest_short_circuits(self):
        # Even if events would say otherwise, an empty input means an empty
        # output — and we should not iterate events at all.
        pr = _PR(_RaisingIssue())
        self.assertEqual(_bot_added_labels(pr, []), set())

    def test_human_added_label_is_not_flagged(self):
        pr = self._pr(
            _Event("labeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
        )
        self.assertEqual(_bot_added_labels(pr, [self.LABEL]), set())

    def test_bot_added_label_is_flagged(self):
        pr = self._pr(
            _Event("labeled", _Label(self.LABEL), _Actor("clickhouse-gh")),
        )
        self.assertEqual(_bot_added_labels(pr, [self.LABEL]), {self.LABEL})

    def test_bot_then_human_re_add_is_not_flagged(self):
        # Only the most recent `labeled` event for a given label name decides
        # attribution: if a human re-applies the label after a bot, treat it
        # as human-added.
        pr = self._pr(
            _Event("labeled", _Label(self.LABEL), _Actor("clickhouse-gh")),
            _Event("unlabeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
            _Event("labeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
        )
        self.assertEqual(_bot_added_labels(pr, [self.LABEL]), set())

    def test_human_then_bot_re_add_is_flagged(self):
        pr = self._pr(
            _Event("labeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
            _Event("unlabeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
            _Event("labeled", _Label(self.LABEL), _Actor("clickhouse-gh")),
        )
        self.assertEqual(_bot_added_labels(pr, [self.LABEL]), {self.LABEL})

    def test_unrelated_labels_are_ignored(self):
        pr = self._pr(
            _Event("labeled", _Label("documentation"), _Actor("clickhouse-gh")),
            _Event("labeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
        )
        self.assertEqual(_bot_added_labels(pr, [self.LABEL]), set())

    def test_multiple_labels_attributed_independently(self):
        pr = self._pr(
            _Event("labeled", _Label(self.LABEL), _Actor("alexey-milovidov")),
            _Event("labeled", _Label(self.OTHER_LABEL), _Actor("clickhouse-gh")),
        )
        self.assertEqual(
            _bot_added_labels(pr, [self.LABEL, self.OTHER_LABEL]),
            {self.OTHER_LABEL},
        )

    def test_events_api_failure_propagates(self):
        # We must fail closed: if the events API errors out we cannot safely
        # decide who applied the label, so the exception must reach the
        # caller (which handles per-PR errors) rather than silently treating
        # the label as human-added.
        pr = _PR(_RaisingIssue())
        with self.assertRaises(RuntimeError):
            _bot_added_labels(pr, [self.LABEL])


if __name__ == "__main__":
    unittest.main()
