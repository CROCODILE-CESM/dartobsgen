"""Tests for dartobsgen.model_state using tiny synthetic netCDF fixtures.

No real MOM6 files are needed: fixtures build restart-shaped files
(Temp, Salt, u, v, h with a Time dimension in days since 0001-01-01)
on the fly in tmp_path.
"""
from __future__ import annotations

import os
from datetime import datetime

import numpy as np
import pytest

xr = pytest.importorskip("xarray")

from dartobsgen.model_state import (
    _MOM6_DAYS_TO_DART_EPOCH,
    MOM6StateProvider,
    mom6_time_to_datetime,
)

_EPOCH_YEAR1 = datetime(1, 1, 1)

RESTART_VARS = ("Temp", "Salt", "u", "v", "h")


def raw_days(dt: datetime) -> float:
    """MOM6 ``Time`` value (days since 0001-01-01) for *dt*."""
    return (dt - _EPOCH_YEAR1).total_seconds() / 86400.0


def make_mom6_file(
    path,
    raw_times,
    *,
    calendar="gregorian",
    units="days since 0001-01-01 00:00:00",
    var_names=RESTART_VARS,
):
    """Write a tiny restart-shaped netCDF file.

    Every variable's values equal the slice's time index, so tests can
    verify the correct slice was extracted.
    """
    nz, ny, nx = 3, 4, 5
    nt = len(raw_times)
    data_vars = {
        name: (
            ("Time", "Layer", "lath", "lonh"),
            np.array([np.full((nz, ny, nx), float(t)) for t in range(nt)]),
        )
        for name in var_names
    }
    ds = xr.Dataset(
        data_vars,
        coords={
            "Time": (
                "Time",
                np.asarray(raw_times, dtype="f8"),
                {"units": units, "calendar": calendar},
            )
        },
    )
    ds.to_netcdf(path)


class TestMom6TimeToDatetime:
    def test_dart_epoch(self):
        assert mom6_time_to_datetime(float(_MOM6_DAYS_TO_DART_EPOCH)) == datetime(1601, 1, 1)

    def test_round_trip_modern_date(self):
        dt = datetime(2010, 1, 5)
        assert mom6_time_to_datetime(raw_days(dt)) == dt

    def test_truncates_to_whole_days(self):
        # read_model_time in DART's MOM6 model_mod does int(days): a slice
        # stamped at noon is seen by DART as 00:00 of that day.
        noon = datetime(2010, 1, 5, 12, 0, 0)
        assert mom6_time_to_datetime(raw_days(noon)) == datetime(2010, 1, 5)


@pytest.fixture
def daily_file(tmp_path):
    """One file with five daily slices, 2010-01-01 .. 2010-01-05 00Z."""
    path = tmp_path / "mom6_output.nc"
    make_mom6_file(path, [raw_days(datetime(2010, 1, d)) for d in range(1, 6)])
    return str(path)


@pytest.fixture
def cache_dir(tmp_path):
    return str(tmp_path / "cache")


class TestSliceSelection:
    def test_selects_slice_within_window(self, daily_file, cache_dir):
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 3), datetime(2010, 1, 4))
        assert state is not None
        assert state.valid_time == datetime(2010, 1, 3)
        with xr.open_dataset(state.path, decode_times=False) as ds:
            assert ds.sizes["Time"] == 1  # Time kept as a size-1 dimension
            for name in RESTART_VARS:
                assert name in ds.variables
            # values equal the source time index: Jan 3 is index 2
            assert float(ds["Temp"][0, 0, 0, 0]) == 2.0

    def test_no_slice_in_window_returns_none(self, daily_file, cache_dir):
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        assert provider.state_for_window(datetime(2010, 2, 1), datetime(2010, 2, 2)) is None

    def test_window_is_half_open(self, daily_file, cache_dir):
        # date1 is exclusive: a slice exactly at date1 must not be selected
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        assert provider.state_for_window(datetime(2009, 12, 31), datetime(2010, 1, 1)) is None

    def test_picks_earliest_of_multiple_slices(self, daily_file, cache_dir):
        # one slice per window: a 3-day window takes the earliest slice
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 2), datetime(2010, 1, 5))
        assert state.valid_time == datetime(2010, 1, 2)

    def test_midday_stamps_map_to_truncated_time(self, tmp_path, cache_dir):
        # daily means stamped at noon: DART truncates to 00Z of the same day
        path = tmp_path / "midday.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, d, 12)) for d in range(1, 4)]
        )
        provider = MOM6StateProvider(str(path), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 2), datetime(2010, 1, 3))
        assert state is not None
        assert state.valid_time == datetime(2010, 1, 2)
        with xr.open_dataset(state.path, decode_times=False) as ds:
            assert float(ds["Temp"][0, 0, 0, 0]) == 1.0  # second slice


class TestExtractionCache:
    def test_extraction_is_cached(self, daily_file, cache_dir):
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        first = provider.state_for_window(datetime(2010, 1, 3), datetime(2010, 1, 4))
        second = provider.state_for_window(datetime(2010, 1, 3), datetime(2010, 1, 4))
        assert first.path == second.path
        assert len(os.listdir(cache_dir)) == 1

    def test_single_slice_file_used_in_place(self, tmp_path, cache_dir):
        path = tmp_path / "single.nc"
        make_mom6_file(path, [raw_days(datetime(2010, 1, 1))])
        provider = MOM6StateProvider(str(path), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 1), datetime(2010, 1, 2))
        assert os.path.samefile(state.path, path)
        assert not os.path.exists(cache_dir)  # nothing was extracted

    def test_glob_of_single_slice_files(self, tmp_path, cache_dir):
        for d in (1, 2, 3):
            make_mom6_file(
                tmp_path / f"mom6.r.2010010{d}.nc", [raw_days(datetime(2010, 1, d))]
            )
        provider = MOM6StateProvider(str(tmp_path / "mom6.r.*.nc"), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 2), datetime(2010, 1, 3))
        assert state.valid_time == datetime(2010, 1, 2)
        assert os.path.samefile(state.path, tmp_path / "mom6.r.20100102.nc")


class TestValidation:
    def test_no_matching_files_raises(self, tmp_path, cache_dir):
        with pytest.raises(FileNotFoundError):
            MOM6StateProvider(str(tmp_path / "nope*.nc"), cache_dir=cache_dir)

    def test_missing_restart_variable_raises(self, tmp_path, cache_dir):
        path = tmp_path / "partial.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], var_names=("Temp", "Salt")
        )
        with pytest.raises(ValueError, match="missing restart variable"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)

    def test_history_style_names_rejected(self, tmp_path, cache_dir):
        path = tmp_path / "history.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], var_names=("thetao", "so")
        )
        with pytest.raises(ValueError, match="history/diagnostic"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)

    def test_custom_required_vars_accepted(self, tmp_path, cache_dir):
        path = tmp_path / "temp_salt_h.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], var_names=("Temp", "Salt", "h")
        )
        provider = MOM6StateProvider(
            str(path), cache_dir=cache_dir, required_vars=("Temp", "Salt", "h")
        )
        assert provider.state_for_window(
            datetime(2010, 1, 1), datetime(2010, 1, 2)
        ) is not None

    def test_noleap_calendar_rejected(self, tmp_path, cache_dir):
        path = tmp_path / "noleap.nc"
        make_mom6_file(path, [raw_days(datetime(2010, 1, 1))], calendar="noleap")
        with pytest.raises(ValueError, match="calendar"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)

    def test_wrong_time_epoch_rejected(self, tmp_path, cache_dir):
        path = tmp_path / "wrong_epoch.nc"
        make_mom6_file(
            path,
            [raw_days(datetime(2010, 1, 1))],
            units="days since 1900-01-01 00:00:00",
        )
        with pytest.raises(ValueError, match="Time units"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)
