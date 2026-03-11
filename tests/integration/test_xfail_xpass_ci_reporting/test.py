# TEMPORARY: used to verify XFAIL/XPASS status handling in Praktika CI reporting.
# Remove once the reporting behaviour is confirmed in CI.
import pytest


@pytest.mark.xfail(reason="expected to fail")
def test_xfail():
    """This test is expected to fail and does fail → should appear as XFAIL (green)."""
    assert False


@pytest.mark.xfail(reason="expected to fail but actually passes")
def test_xpass():
    """This test is expected to fail but passes → should appear as XPASS (red) and fail the job."""
    assert True
