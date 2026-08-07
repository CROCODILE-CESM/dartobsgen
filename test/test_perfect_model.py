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
