"""Tests for dartobsgen.model_state using tiny synthetic netCDF fixtures.

No real MOM6 files are needed: fixtures build MOM6-shaped files (restart
style with 'Time', or history style with lowercase 'time' and diagnostic
variable names, days since 0001-01-01) on the fly in tmp_path.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import numpy as np
import pytest

xr = pytest.importorskip("xarray")

from dartobsgen.model_state import (
    _MOM6_DAYS_TO_DART_EPOCH,
    CICEStateProvider,
    TimesliceStateProvider,
    MOM6StateProvider,
    cice_time_to_datetime,
    input_files_from_nml,
    mom6_time_to_datetime,
    state_vars_from_nml,
)

_EPOCH_YEAR1 = datetime(1, 1, 1)

RESTART_VARS = ("Temp", "Salt", "u", "v", "h")
HISTORY_VARS = ("thetao", "so", "uo", "vo")


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
    time_name="Time",
):
    """Write a tiny MOM6-shaped netCDF file.

    Every variable's values equal the slice's time index, so tests can
    verify the correct slice was extracted.
    """
    nz, ny, nx = 3, 4, 5
    nt = len(raw_times)
    data_vars = {
        name: (
            (time_name, "Layer", "lath", "lonh"),
            np.array([np.full((nz, ny, nx), float(t)) for t in range(nt)]),
        )
        for name in var_names
    }
    ds = xr.Dataset(
        data_vars,
        coords={
            time_name: (
                time_name,
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

    def test_preserves_time_of_day(self):
        # read_model_time keeps the fractional day as seconds: a slice
        # stamped at noon is seen by DART as 12:00 of that day.
        noon = datetime(2010, 1, 5, 12, 0, 0)
        assert mom6_time_to_datetime(raw_days(noon)) == noon

    def test_quarter_day_maps_to_six_hours(self):
        six_am = datetime(2010, 1, 5, 6, 0, 0)
        assert mom6_time_to_datetime(raw_days(six_am)) == six_am


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
        state = provider.state_for_window(datetime(2010, 1, 2), datetime(2010, 1, 3))
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
        # (date0, date1] matches the window perfect_model_obs is given, so a
        # slice exactly on date0 belongs to the previous window, not this one.
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        assert provider.state_for_window(datetime(2010, 1, 1), datetime(2010, 1, 2)) \
            .valid_time == datetime(2010, 1, 2)
        assert provider.state_for_window(datetime(2010, 1, 5), datetime(2010, 1, 6)) is None

    def test_picks_earliest_of_multiple_slices(self, daily_file, cache_dir):
        # one slice per window: a 3-day window takes the earliest slice
        provider = MOM6StateProvider(daily_file, cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 1), datetime(2010, 1, 4))
        assert state.valid_time == datetime(2010, 1, 2)

    def test_midday_stamps_keep_time_of_day(self, tmp_path, cache_dir):
        # daily means stamped at noon: DART sees 12:00 of the same day
        path = tmp_path / "midday.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, d, 12)) for d in range(1, 4)]
        )
        provider = MOM6StateProvider(str(path), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 2), datetime(2010, 1, 3))
        assert state is not None
        assert state.valid_time == datetime(2010, 1, 2, 12)
        with xr.open_dataset(state.path, decode_times=False) as ds:
            assert float(ds["Temp"][0, 0, 0, 0]) == 1.0  # second slice

    def test_history_style_file(self, tmp_path, cache_dir):
        # z-space history output: lowercase 'time', diagnostic variable names
        path = tmp_path / "mom6.h.nc"
        make_mom6_file(
            path,
            [raw_days(datetime(2015, 10, d, 12)) for d in range(1, 4)],
            var_names=HISTORY_VARS,
            time_name="time",
        )
        provider = MOM6StateProvider(str(path), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2015, 10, 2), datetime(2015, 10, 3))
        assert state is not None
        assert state.valid_time == datetime(2015, 10, 2, 12)
        with xr.open_dataset(state.path, decode_times=False) as ds:
            assert ds.sizes["time"] == 1
            assert float(ds["thetao"][0, 0, 0, 0]) == 1.0  # second slice


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
        state = provider.state_for_window(datetime(2009, 12, 31), datetime(2010, 1, 1))
        assert os.path.samefile(state.path, path)
        assert not os.path.exists(cache_dir)  # nothing was extracted

    def test_glob_of_single_slice_files(self, tmp_path, cache_dir):
        for d in (1, 2, 3):
            make_mom6_file(
                tmp_path / f"mom6.r.2010010{d}.nc", [raw_days(datetime(2010, 1, d))]
            )
        provider = MOM6StateProvider(str(tmp_path / "mom6.r.*.nc"), cache_dir=cache_dir)
        state = provider.state_for_window(datetime(2010, 1, 1), datetime(2010, 1, 2))
        assert state.valid_time == datetime(2010, 1, 2)
        assert os.path.samefile(state.path, tmp_path / "mom6.r.20100102.nc")


class TestValidation:
    def test_no_matching_files_raises(self, tmp_path, cache_dir):
        with pytest.raises(FileNotFoundError):
            MOM6StateProvider(str(tmp_path / "nope*.nc"), cache_dir=cache_dir)

    def test_missing_required_variable_raises(self, tmp_path, cache_dir):
        path = tmp_path / "partial.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], var_names=("Temp", "Salt")
        )
        with pytest.raises(ValueError, match="missing state variable"):
            MOM6StateProvider(str(path), cache_dir=cache_dir, required_vars=RESTART_VARS)

    def test_required_vars_satisfied(self, tmp_path, cache_dir):
        path = tmp_path / "temp_salt_h.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], var_names=("Temp", "Salt", "h")
        )
        provider = MOM6StateProvider(
            str(path), cache_dir=cache_dir, required_vars=("Temp", "Salt", "h")
        )
        assert provider.state_for_window(
            datetime(2009, 12, 31), datetime(2010, 1, 1)
        ) is not None

    def test_no_time_variable_raises(self, tmp_path, cache_dir):
        path = tmp_path / "no_time.nc"
        xr.Dataset({"Temp": (("z",), np.zeros(3))}).to_netcdf(path)
        with pytest.raises(ValueError, match="variable"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)

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

    def test_bare_days_units_accepted(self, tmp_path, cache_dir):
        # CESM-written MOM6 restarts have Time units "days" with no epoch;
        # DART ignores units and counts from 0001-01-01, so this is valid.
        path = tmp_path / "bare_days.nc"
        make_mom6_file(path, [raw_days(datetime(2010, 1, 1))], units="days")
        provider = MOM6StateProvider(str(path), cache_dir=cache_dir)
        assert provider.available_times() == [datetime(2010, 1, 1)]

    def test_bare_days_with_noleap_still_rejected(self, tmp_path, cache_dir):
        path = tmp_path / "bare_days_noleap.nc"
        make_mom6_file(
            path, [raw_days(datetime(2010, 1, 1))], units="days", calendar="noleap"
        )
        with pytest.raises(ValueError, match="calendar"):
            MOM6StateProvider(str(path), cache_dir=cache_dir)


def test_state_vars_from_nml(tmp_path):
    pytest.importorskip("f90nml")
    nml = tmp_path / "input.nml"
    nml.write_text(
        "&model_nml\n"
        "    model_state_variables = 'so ', 'QTY_SALINITY             ', 'NA', 'NA', 'UPDATE',\n"
        "                            'thetao ', 'QTY_POTENTIAL_TEMPERATURE', 'NA', 'NA', 'UPDATE',\n"
        "                            'uo    ', 'QTY_U_CURRENT_COMPONENT  ', 'NA', 'NA', 'UPDATE',\n"
        "                            'vo    ', 'QTY_V_CURRENT_COMPONENT  ', 'NA', 'NA', 'UPDATE',\n"
        "/\n"
    )
    assert state_vars_from_nml(str(nml)) == ("so", "thetao", "uo", "vo")


class TestInputFilesFromNml:
    """input_files_from_nml maps each named input file to the entry naming it."""

    def _write(self, tmp_path, text):
        pytest.importorskip("f90nml")
        nml = tmp_path / "input.nml"
        nml.write_text(text)
        return str(nml)

    def test_collects_model_nml_and_state_files(self, tmp_path):
        path = self._write(
            tmp_path,
            "&perfect_model_obs_nml\n"
            "    input_state_files = 'perfect_input.nc'\n"
            "/\n"
            "&model_nml\n"
            "    ocean_geometry = 'ocean_geometry.nc'\n"
            "    static_file = 'mom6.static.nc'\n"
            "    template_file = 'mom6.r.nc'\n"
            "/\n",
        )
        assert input_files_from_nml(path) == {
            "mom6.r.nc": "model_nml:template_file",
            "mom6.static.nc": "model_nml:static_file",
            "ocean_geometry.nc": "model_nml:ocean_geometry",
            "perfect_input.nc": "perfect_model_obs_nml:input_state_files",
        }

    def test_ignores_output_and_list_files(self, tmp_path):
        # output_state_files is written, not read; filter_nml's list files
        # belong to a different program — neither should be demanded
        path = self._write(
            tmp_path,
            "&perfect_model_obs_nml\n"
            "    input_state_files = 'mom6.r.nc'\n"
            "    output_state_files = 'out_mom6.r.nc'\n"
            "    obs_seq_in_file_name = 'obs_seq.in'\n"
            "/\n"
            "&filter_nml\n"
            "    input_state_file_list = 'filter_input_list.txt'\n"
            "/\n"
            "&utilities_nml\n"
            "    logfilename = 'dart_log.out'\n"
            "/\n",
        )
        assert input_files_from_nml(path) == {
            "mom6.r.nc": "perfect_model_obs_nml:input_state_files"
        }

    def test_file_named_twice_reported_against_first_entry(self, tmp_path):
        path = self._write(
            tmp_path,
            "&perfect_model_obs_nml\n"
            "    input_state_files = 'mom6.r.nc'\n"
            "/\n"
            "&model_nml\n"
            "    template_file = 'mom6.r.nc'\n"
            "/\n",
        )
        # model_nml comes first in the key list, so it is the entry reported
        assert input_files_from_nml(path) == {"mom6.r.nc": "model_nml:template_file"}

    def test_multiple_state_files(self, tmp_path):
        path = self._write(
            tmp_path,
            "&perfect_model_obs_nml\n"
            "    input_state_files = 'a.nc', 'b.nc'\n"
            "/\n",
        )
        assert input_files_from_nml(path) == {
            "a.nc": "perfect_model_obs_nml:input_state_files",
            "b.nc": "perfect_model_obs_nml:input_state_files",
        }

    def test_blank_and_absent_entries_skipped(self, tmp_path):
        path = self._write(
            tmp_path,
            "&perfect_model_obs_nml\n"
            "    input_state_files = ''\n"
            "/\n"
            "&model_nml\n"
            "    template_file =\n"
            "/\n",
        )
        assert input_files_from_nml(path) == {}

    def test_namelist_without_these_entries(self, tmp_path):
        # e.g. Lorenz 96: nothing to check, not an error
        path = self._write(tmp_path, "&model_nml\n    model_size = 40\n/\n")
        assert input_files_from_nml(path) == {}


def test_state_vars_from_nml_three_field_records(tmp_path):
    pytest.importorskip("f90nml")
    nml = tmp_path / "input.nml"
    nml.write_text(
        "&model_nml\n"
        "    model_state_variables = 'aicen', 'QTY_SEAICE_CONCENTR', 'UPDATE',\n"
        "                            'vicen', 'QTY_SEAICE_VOLUME', 'UPDATE',\n"
        "/\n"
    )
    assert state_vars_from_nml(str(nml), fields_per_entry=3) == ("aicen", "vicen")


def test_input_files_from_nml_custom_keys(tmp_path):
    pytest.importorskip("f90nml")
    nml = tmp_path / "input.nml"
    nml.write_text(
        "&perfect_model_obs_nml\n    input_state_files = 'a.nc'\n/\n"
        "&model_nml\n    template_file = 'ignored.nc'\n/\n"
    )
    keys = (("perfect_model_obs_nml", "input_state_files"),)
    assert input_files_from_nml(str(nml), keys) == {
        "a.nc": "perfect_model_obs_nml:input_state_files"
    }


class _FakeProvider(TimesliceStateProvider):
    """Minimal subclass: valid time is a global attribute ``t`` in hours."""

    def _validate(self, ds, path):
        if "t" not in ds.attrs:
            raise ValueError(f"{path}: no t")

    def _scan_file(self, ds, path):
        return [(0, datetime(2000, 1, 1) + timedelta(hours=int(ds.attrs["t"])))]


class TestTimesliceStateProvider:
    def _files(self, tmp_path, hours):
        for h in hours:
            xr.Dataset({"x": ("n", [1.0])}, attrs={"t": h}).to_netcdf(tmp_path / f"f{h:03d}.nc")
        return str(tmp_path / "f*.nc")

    def test_default_materialise_uses_file_in_place(self, tmp_path):
        provider = _FakeProvider(self._files(tmp_path, [0, 24]), cache_dir=str(tmp_path / "c"))
        state = provider.state_for_window(datetime(2000, 1, 1), datetime(2000, 1, 2))
        assert state.valid_time == datetime(2000, 1, 2)
        assert os.path.samefile(state.path, tmp_path / "f024.nc")
        assert not os.path.exists(tmp_path / "c")

    def test_available_times_sorted_and_window_edges(self, tmp_path):
        provider = _FakeProvider(self._files(tmp_path, [48, 0, 24]), cache_dir=str(tmp_path / "c"))
        assert provider.available_times() == [
            datetime(2000, 1, 1), datetime(2000, 1, 2), datetime(2000, 1, 3)
        ]
        # open below, closed above
        assert provider.state_for_window(datetime(2000, 1, 1), datetime(2000, 1, 2)).valid_time == datetime(2000, 1, 2)
        assert provider.state_for_window(datetime(2000, 1, 2), datetime(2000, 1, 2, 12)) is None

    def test_required_vars_checked_by_base(self, tmp_path):
        with pytest.raises(ValueError, match="missing state variable"):
            _FakeProvider(self._files(tmp_path, [0]), cache_dir=str(tmp_path / "c"),
                          required_vars=("nope",))

    def test_validate_hook_runs(self, tmp_path):
        xr.Dataset({"x": ("n", [1.0])}).to_netcdf(tmp_path / "bad.nc")
        with pytest.raises(ValueError, match="no t"):
            _FakeProvider(str(tmp_path / "bad.nc"), cache_dir=str(tmp_path / "c"))

    def test_default_input_file_keys_are_inherited(self):
        assert ("model_nml", "template_file") in _FakeProvider.input_file_keys


class TestCiceTime:
    def test_basic(self):
        assert cice_time_to_datetime(2010, 3, 4, 3600) == datetime(2010, 3, 4, 1)

    def test_year_zero_is_year_one(self):
        assert cice_time_to_datetime(0, 1, 1, 0) == datetime(1, 1, 1)


def make_cice_file(path, nyr, month, mday, sec, variables=("aicen", "vicen")):
    ds = xr.Dataset(
        {v: (("nj", "ni"), np.zeros((2, 2))) for v in variables},
        attrs={"nyr": nyr, "month": month, "mday": mday, "sec": sec},
    )
    ds.to_netcdf(path)


class TestCICEStateProvider:
    @pytest.fixture
    def restarts(self, tmp_path):
        for d in (1, 2, 3):
            make_cice_file(tmp_path / f"iced.2010-01-0{d}.nc", 2010, 1, d, 0)
        return str(tmp_path / "iced.*.nc")

    def test_window_selection_uses_file_in_place(self, tmp_path, restarts):
        provider = CICEStateProvider(restarts, cache_dir=str(tmp_path / "c"))
        state = provider.state_for_window(datetime(2010, 1, 1), datetime(2010, 1, 2))
        assert state.valid_time == datetime(2010, 1, 2)
        assert os.path.samefile(state.path, tmp_path / "iced.2010-01-02.nc")
        assert not os.path.exists(tmp_path / "c")

    def test_lower_edge_belongs_to_previous_window(self, tmp_path, restarts):
        provider = CICEStateProvider(restarts, cache_dir=str(tmp_path / "c"))
        assert provider.state_for_window(datetime(2010, 1, 3), datetime(2010, 1, 4)) is None

    def test_available_times(self, tmp_path, restarts):
        provider = CICEStateProvider(restarts, cache_dir=str(tmp_path / "c"))
        assert provider.available_times() == [
            datetime(2010, 1, 1), datetime(2010, 1, 2), datetime(2010, 1, 3)
        ]

    def test_missing_time_attribute(self, tmp_path):
        xr.Dataset({"aicen": ("n", [0.0])}, attrs={"nyr": 2010}).to_netcdf(tmp_path / "x.nc")
        with pytest.raises(ValueError, match="missing global attribute"):
            CICEStateProvider(str(tmp_path / "x.nc"), cache_dir=str(tmp_path / "c"))

    def test_missing_state_variable(self, tmp_path):
        make_cice_file(tmp_path / "x.nc", 2010, 1, 1, 0)
        with pytest.raises(ValueError, match="missing state variable"):
            CICEStateProvider(str(tmp_path / "x.nc"), cache_dir=str(tmp_path / "c"),
                              required_vars=("vsnon",))

    def test_input_file_keys_only_state_files(self):
        assert CICEStateProvider.input_file_keys == (
            ("perfect_model_obs_nml", "input_state_files"),
        )
