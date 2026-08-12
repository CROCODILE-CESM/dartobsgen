"""Tests for perfect_model namelist patching and state-provider wiring.

These run without a compiled perfect_model_obs executable.
"""
from __future__ import annotations

from datetime import datetime

import pytest

f90nml = pytest.importorskip("f90nml")

from dartobsgen.model_state import ModelStateProvider
from dartobsgen.sources.perfect_model import (
    ObsNetworkEntry,
    PerfectModelSource,
    _patch_input_nml,
)

BASE_NML = """\
&perfect_model_obs_nml
   read_input_state_from_file = .false.
   input_state_files          = 'perfect_input.nc'
   obs_seq_in_file_name       = 'obs_seq.in'
   obs_seq_out_file_name      = 'obs_seq.out'
/
&model_nml
   template_file = 'mom6.r.nc'
/
"""


@pytest.fixture
def src_nml(tmp_path):
    path = tmp_path / "input.nml"
    path.write_text(BASE_NML)
    return str(path)


def _patch(src_nml, tmp_path, **kwargs):
    dest = str(tmp_path / "patched.nml")
    _patch_input_nml(
        src_nml,
        dest,
        "obs_seq.in",
        "obs_seq.out",
        datetime(2010, 1, 2),
        datetime(2010, 1, 3),
        **kwargs,
    )
    return f90nml.read(dest)


def test_patch_sets_input_state_files(src_nml, tmp_path):
    nml = _patch(src_nml, tmp_path, input_state_files="/cache/mom6_state_20100102.nc")
    block = nml["perfect_model_obs_nml"]
    assert block["input_state_files"] == "/cache/mom6_state_20100102.nc"
    assert block["read_input_state_from_file"] is True


def test_patch_without_state_leaves_state_settings_alone(src_nml, tmp_path):
    nml = _patch(src_nml, tmp_path)
    block = nml["perfect_model_obs_nml"]
    assert block["input_state_files"] == "perfect_input.nc"
    assert block["read_input_state_from_file"] is False


def test_patch_preserves_other_groups(src_nml, tmp_path):
    nml = _patch(src_nml, tmp_path, input_state_files="/cache/state.nc")
    assert nml["model_nml"]["template_file"] == "mom6.r.nc"


def test_patch_sets_window_bounds(src_nml, tmp_path):
    nml = _patch(src_nml, tmp_path)
    block = nml["perfect_model_obs_nml"]
    days_2010_01_02 = (datetime(2010, 1, 2) - datetime(1601, 1, 1)).days
    days_2010_01_03 = (datetime(2010, 1, 3) - datetime(1601, 1, 1)).days
    # first obs time is one second after the (exclusive) window start
    assert block["first_obs_days"] == days_2010_01_02
    assert block["first_obs_seconds"] == 1
    # last obs time is exactly the (inclusive) window end
    assert block["last_obs_days"] == days_2010_01_03
    assert block["last_obs_seconds"] == 0


class _NoStateProvider(ModelStateProvider):
    """Provider stub for a run with no model output in any window."""

    def state_for_window(self, date0, date1):
        return None


def test_write_obs_seq_skips_window_without_state(tmp_path):
    source = PerfectModelSource(
        dart_work_dir=str(tmp_path),
        obs_network=[
            ObsNetworkEntry(
                obs_type="OCEAN_TEMPERATURE",
                lat=0.0,
                lon=0.0,
                vertical=10.0,
                vert_unit="height (m)",
                obs_err_var=0.04,
            )
        ],
        state_provider=_NoStateProvider(),
    )
    written = source.write_obs_seq(
        output_file=str(tmp_path / "obs_seq.out"),
        analysis_time=datetime(2010, 1, 1, 12),
        date0=datetime(2010, 1, 1),
        date1=datetime(2010, 1, 2),
        lat_min=-90.0,
        lat_max=90.0,
        lon_min=-180.0,
        lon_max=180.0,
        obs_types=["OCEAN_TEMPERATURE"],
        obs_type_map=None,
    )
    assert written is False


class _FixedTimesProvider(ModelStateProvider):
    """Provider stub serving states at a fixed list of valid times."""

    def __init__(self, times):
        self._times = sorted(times)

    def available_times(self):
        return list(self._times)

    def state_for_window(self, date0, date1):
        return None  # coverage checks never reach this


def _source(provider):
    return PerfectModelSource(
        dart_work_dir="/nonexistent", obs_network=[], state_provider=provider
    )


def _windows(first_analysis, n, freq):
    half = freq / 2
    return [
        (first_analysis + i * freq,
         first_analysis + i * freq - half,
         first_analysis + i * freq + half)
        for i in range(n)
    ]


DAY = datetime(2015, 10, 4, 12) - datetime(2015, 10, 3, 12)


class TestCheckCoverage:
    def test_aligned_states_pass(self, capsys):
        times = [datetime(2015, 10, d, 12) for d in (4, 5, 6)]
        _source(_FixedTimesProvider(times)).check_coverage(
            _windows(datetime(2015, 10, 4, 12), 3, DAY)
        )
        out = capsys.readouterr().out
        assert "3 of 3 window(s) have a state" in out
        assert "outside all windows" in out  # summary line reports 0 outside

    def test_no_state_in_any_window_raises(self):
        # the classic misconfiguration: analysis times a half-day off the
        # model output, so every state lands outside every window
        times = [datetime(2015, 10, 4, 12)]
        with pytest.raises(ValueError, match="No model state falls in any"):
            _source(_FixedTimesProvider(times)).check_coverage(
                _windows(datetime(2015, 10, 6, 0), 3, DAY)
            )

    def test_error_names_the_fix(self):
        times = [datetime(2015, 10, 4, 12)]
        with pytest.raises(ValueError) as exc:
            _source(_FixedTimesProvider(times)).check_coverage(
                _windows(datetime(2015, 10, 6, 0), 3, DAY)
            )
        assert "first_analysis=2015-10-04T12:00:00" in str(exc.value)

    def test_reports_states_outside_all_windows(self, capsys):
        times = [datetime(2015, 10, d, 12) for d in (4, 5, 9)]
        _source(_FixedTimesProvider(times)).check_coverage(
            _windows(datetime(2015, 10, 4, 12), 3, DAY)
        )
        out = capsys.readouterr().out
        assert "Outside all windows: 2015-10-09T12:00:00" in out

    def test_reports_shadowed_states(self, capsys):
        # two states inside one daily window: only the earliest is used
        times = [datetime(2015, 10, 4, 6), datetime(2015, 10, 4, 12)]
        _source(_FixedTimesProvider(times)).check_coverage(
            _windows(datetime(2015, 10, 4, 12), 1, DAY)
        )
        out = capsys.readouterr().out
        assert "Shadowed" in out
        assert "2015-10-04T12:00:00" in out

    def test_reports_off_centre_states(self, capsys):
        # state inside its window but not at the analysis time
        times = [datetime(2015, 10, 4, 12)]
        _source(_FixedTimesProvider(times)).check_coverage(
            _windows(datetime(2015, 10, 4, 18), 1, DAY)
        )
        out = capsys.readouterr().out
        assert "Off-centre" in out

    def test_no_provider_is_a_no_op(self):
        source = PerfectModelSource(dart_work_dir="/nonexistent", obs_network=[])
        assert source.check_coverage(_windows(datetime(2015, 10, 4, 12), 3, DAY)) is None

    def test_unenumerable_provider_is_a_no_op(self):
        # _NoStateProvider inherits available_times() -> None
        assert _source(_NoStateProvider()).check_coverage(
            _windows(datetime(2015, 10, 4, 12), 3, DAY)
        ) is None
