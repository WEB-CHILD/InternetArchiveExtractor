"""Tests for the WaybackDateObject date helper."""

from datetime import datetime

import pytest

from wayback_date_object import WaybackDateObject


def test_parses_components_from_string():
    """Constructor splits a 14-digit Wayback string into year/month/day/hour/minute/second."""
    obj = WaybackDateObject("20030409193011")
    assert obj.year == "2003"
    assert obj.month == "04"
    assert obj.day == "09"
    assert obj.hour == "19"
    assert obj.minute == "30"
    assert obj.second == "11"


def test_pretty_print():
    """pretty_print returns a human-readable 'YYYY-MM-DD HH:MM:SS' string."""
    obj = WaybackDateObject("20030409193011")
    assert obj.pretty_print() == "2003-04-09 19:30:11"


def test_wayback_format_roundtrips_input():
    """wayback_format reconstructs the original 14-digit string."""
    raw = "20030409193011"
    assert WaybackDateObject(raw).wayback_format() == raw


def test_to_datetime():
    """to_datetime converts the stored values to an equivalent Python datetime."""
    obj = WaybackDateObject("20030409193011")
    assert obj.to_datetime() == datetime(2003, 4, 9, 19, 30, 11)


def test_from_datetime_zero_pads():
    """from_datetime zero-pads each field so wayback_format stays 14 digits wide."""
    obj = WaybackDateObject("20030409193011")
    obj.from_datetime(datetime(5, 1, 2, 3, 4, 5))
    assert obj.year == "0005"
    assert obj.month == "01"
    assert obj.wayback_format() == "00050102030405"


def test_increment_day_simple():
    """increment_day advances the date by one day within the same month."""
    obj = WaybackDateObject("20030409193011")
    obj.increment_day()
    assert obj.wayback_format() == "20030410193011"


def test_decrement_day_simple():
    """decrement_day moves the date back by one day within the same month."""
    obj = WaybackDateObject("20030409193011")
    obj.decrement_day()
    assert obj.wayback_format() == "20030408193011"


def test_increment_day_crosses_month_boundary():
    """increment_day rolls over to the next month correctly."""
    obj = WaybackDateObject("20030430120000")
    obj.increment_day()
    assert obj.wayback_format() == "20030501120000"


def test_decrement_day_crosses_month_boundary():
    """decrement_day rolls back into the previous month with the correct number of days."""
    obj = WaybackDateObject("20030301120000")
    obj.decrement_day()
    # 2003 is not a leap year, so February has 28 days.
    assert obj.wayback_format() == "20030228120000"


def test_increment_day_leap_year():
    """increment_day produces Feb 29 when the year is a leap year."""
    obj = WaybackDateObject("20000228120000")
    obj.increment_day()
    # 2000 is a leap year, so Feb 29 exists.
    assert obj.wayback_format() == "20000229120000"


def test_increment_day_crosses_year_boundary():
    """increment_day rolls Dec 31 over to Jan 1 of the next year."""
    obj = WaybackDateObject("20031231235900")
    obj.increment_day()
    assert obj.wayback_format() == "20040101235900"


def test_increment_week():
    """increment_week advances the date by exactly 7 days."""
    obj = WaybackDateObject("20030409193011")
    obj.increment_week()
    assert obj.wayback_format() == "20030416193011"


def test_decrement_week_crosses_month():
    """decrement_week moves the date back 7 days, crossing a month boundary."""
    obj = WaybackDateObject("20030405120000")
    obj.decrement_week()
    assert obj.wayback_format() == "20030329120000"


def test_increment_then_decrement_day_is_identity():
    """Incrementing then decrementing by one day returns to the original date."""
    obj = WaybackDateObject("20030409193011")
    obj.increment_day()
    obj.decrement_day()
    assert obj.wayback_format() == "20030409193011"


def test_invalid_month_raises_on_to_datetime():
    """to_datetime raises ValueError when the stored values form an impossible date."""
    obj = WaybackDateObject("20031340120000")  # month 13, day 40
    with pytest.raises(ValueError):
        obj.to_datetime()
