"""Tests for constants.py."""

from constants import Period, DOWNLOAD_PERIOD, DOWNLOAD_RESET


def test_period_members():
    """Period enum defines exactly the four expected members."""
    assert {p.name for p in Period} == {"DAY", "WEEK", "FULL", "CUSTOM"}


def test_period_value_equals_name():
    """Each Period member's value is the same string as its name."""
    for p in Period:
        assert p.value == p.name


def test_period_lookup_by_value():
    """Period can be looked up by passing its string value to the constructor."""
    assert Period("DAY") is Period.DAY
    assert Period("FULL") is Period.FULL


def test_defaults():
    """Module-level defaults are FULL period and reset disabled."""
    assert DOWNLOAD_PERIOD is Period.FULL
    assert DOWNLOAD_RESET is False
