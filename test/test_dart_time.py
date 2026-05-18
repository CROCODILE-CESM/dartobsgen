"""Tests for _to_dart_time."""
import pandas as pd
import pytest
from dartobsgen.sources.nnja import _to_dart_time


def test_to_dart_time_midnight_utc():
    ts = pd.Series(pd.to_datetime(["2021-01-01 00:00:00"]).tz_localize("UTC"))
    days, secs = _to_dart_time(ts)
    assert days.iloc[0] == 153402
    assert secs.iloc[0] == 0


def test_to_dart_time_six_hours_utc():
    ts = pd.Series(pd.to_datetime(["2021-01-01 06:00:00"]).tz_localize("UTC"))
    days, secs = _to_dart_time(ts)
    assert days.iloc[0] == 153402
    assert secs.iloc[0] == 21600


def test_to_dart_time_naive():
    ts = pd.Series(pd.to_datetime(["2021-01-01 12:00:00"]))
    days, secs = _to_dart_time(ts)
    assert days.iloc[0] == 153402
    assert secs.iloc[0] == 43200
