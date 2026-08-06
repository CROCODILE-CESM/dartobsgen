from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pandas as pd

from .base import DataSource

_DART_EPOCH_PY = datetime(1601, 1, 1)

# Files that must not be symlinked from dart_work_dir into each window dir —
# either because the window writes them or because the window patches them.
_SKIP_LINKS = frozenset({"input.nml", "obs_seq.in", "obs_seq.out", "windows"})


@dataclass
class ObsNetworkEntry:
    """One synthetic observation location and type in the observing network.

    Parameters
    ----------
    obs_type : str
        DART obs type name, e.g. ``"TEMPERATURE"``, ``"U_WIND_COMPONENT"``.
    lat : float
        Latitude in degrees.
    lon : float
        Longitude in degrees (-180 to 180).
    vertical : float
        Vertical coordinate value (units defined by ``vert_unit``).
    vert_unit : str
        Vertical coordinate unit string understood by DART, e.g.
        ``"pressure (Pa)"``, ``"height (m)"``, ``"model level"``.
    obs_err_var : float
        Observation error variance (same units as the observed quantity,
        squared).
    time_offset : timedelta
        Offset from the cycle's ``analysis_time`` at which this observation is
        placed.  Default is ``timedelta(0)`` — exactly at the analysis time,
        i.e. the centre of the window.  Must stay within
        ``(-freq/2, +freq/2]`` or ``perfect_model_obs`` will reject the
        observation as outside the window.
    """

    obs_type: str
    lat: float
    lon: float
    vertical: float
    vert_unit: str
    obs_err_var: float
    time_offset: timedelta = field(default_factory=timedelta)


def _datetime_to_dart_time(dt: datetime) -> tuple[int, int]:
    """Return ``(days, seconds)`` since the DART epoch (1601-01-01 00:00:00)."""
    delta = dt - _DART_EPOCH_PY
    return delta.days, delta.seconds


def _write_obs_seq_template(
    entries: list[ObsNetworkEntry], analysis_time: datetime, output_path: str
) -> None:
    """Write a template obs_seq.in with placeholder observation values.

    ``perfect_model_obs`` replaces the placeholder values (0.0) with
    forward-operator results from the model state.  The metadata — location,
    type, time, error variance — must be correct.

    Observations are placed at ``analysis_time + entry.time_offset``, so the
    default offset of zero puts them at the centre of the window rather than
    on its exclusive lower edge (where they would be filtered out).
    """
    from pydartdiags.obs_sequence.obs_sequence import ObsSequence  # noqa: PLC0415

    n = len(entries)
    obs_times = [analysis_time + e.time_offset for e in entries]
    dart_times = [_datetime_to_dart_time(t) for t in obs_times]

    df = pd.DataFrame(
        {
            "obs_num": 0,
            "observation": 0.0,
            "DART_quality_control": 0.0,
            "linked_list": "",
            "longitude": [e.lon for e in entries],
            "latitude": [e.lat for e in entries],
            "vertical": [e.vertical for e in entries],
            "vert_unit": [e.vert_unit for e in entries],
            "type": [e.obs_type for e in entries],
            "metadata": [[] for _ in range(n)],
            "external_FO": [[] for _ in range(n)],
            "seconds": [s for _, s in dart_times],
            "days": [d for d, _ in dart_times],
            "time": pd.to_datetime(obs_times),
            "obs_err_var": [e.obs_err_var for e in entries],
        }
    )

    obs_seq = ObsSequence(file=None)
    obs_seq.qc_copie_names = ["DART_quality_control"]
    obs_seq.df = df
    obs_seq.write_obs_seq(output_path)


def _patch_input_nml(
    src_nml: str,
    dest_nml: str,
    obs_seq_in: str,
    obs_seq_out: str,
    date0: datetime,
    date1: datetime,
) -> None:
    """Write a patched ``input.nml`` for a single window.

    Only the ``perfect_model_obs_nml`` block is modified; all other namelist
    groups are preserved verbatim.

    The window ``(date0, date1]`` is expressed in DART's integer-second form
    as ``first_obs = date0 + 1s`` and ``last_obs = date1``, which is exactly
    the convention documented in ``obs_sequence_tool.rst``: start at minus
    half the window plus one second, end at exactly plus half the window.
    """
    import f90nml  # noqa: PLC0415

    nml = f90nml.read(src_nml)

    first_days, first_secs = _datetime_to_dart_time(date0 + timedelta(seconds=1))
    last_days, last_secs = _datetime_to_dart_time(date1)

    block = nml.get("perfect_model_obs_nml", {})
    block["obs_seq_in_file_name"] = obs_seq_in
    block["obs_seq_out_file_name"] = obs_seq_out
    block["first_obs_days"] = first_days
    block["first_obs_seconds"] = first_secs
    block["last_obs_days"] = last_days
    block["last_obs_seconds"] = last_secs
    nml["perfect_model_obs_nml"] = block

    nml.write(dest_nml, force=True)


class PerfectModelSource(DataSource):
    """Data source that generates synthetic observations via DART's ``perfect_model_obs``.

    For each assimilation window, this source:

    1. Filters the observing network to the requested obs types and bounding box.
    2. Writes a template ``obs_seq.in`` (placeholder observation values 0.0).
    3. Patches ``input.nml`` with the obs_seq filenames and window time bounds.
    4. Runs ``perfect_model_obs`` in an isolated per-window directory.
    5. Moves the resulting ``obs_seq.out`` to the caller-specified output path.

    Parameters
    ----------
    dart_work_dir : str
        Directory containing the compiled ``perfect_model_obs`` executable,
        a base ``input.nml``, and any initial-conditions files it references.
        Each window runs in a temporary subdirectory of
        ``{dart_work_dir}/windows/`` that symlinks back to the shared files.
    obs_network : list[ObsNetworkEntry]
        The synthetic observing network.  Each entry defines one observation
        location, type, error variance, and time offset within the window.
    perfect_model_obs_exe : str
        Name or path of the executable relative to the window directory.
        Default is ``"./perfect_model_obs"``.

    Notes
    -----
    **Parallel safety**: each call to :meth:`write_obs_seq` runs in its own
    subdirectory, so multiple ``ProcessPoolExecutor`` workers can run
    simultaneously without file conflicts.

    **Model state**: ``perfect_model_obs`` interpolates from the initial
    conditions file specified in ``input.nml``.  This class assumes a single
    fixed initial conditions file valid for the entire run (appropriate for
    Lorenz-type models or a frozen-truth scenario).  Time-advancing the model
    across windows requires additional coupling outside the scope of this source.
    """

    def __init__(
        self,
        dart_work_dir: str,
        obs_network: list[ObsNetworkEntry],
        perfect_model_obs_exe: str = "./perfect_model_obs",
    ):
        self.dart_work_dir = os.path.abspath(dart_work_dir)
        self.obs_network = obs_network
        self.perfect_model_obs_exe = perfect_model_obs_exe

    def _setup_window_dir(self, window_dir: str) -> None:
        """Create *window_dir* and symlink shared files from ``dart_work_dir``."""
        os.makedirs(window_dir, exist_ok=True)
        for name in os.listdir(self.dart_work_dir):
            if name in _SKIP_LINKS:
                continue
            src = os.path.join(self.dart_work_dir, name)
            dst = os.path.join(window_dir, name)
            if not os.path.exists(dst):
                os.symlink(src, dst)

    def write_obs_seq(
        self,
        output_file: str,
        analysis_time: datetime,
        date0: datetime,
        date1: datetime,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        obs_types: list[str],
        obs_type_map: dict | None,
    ) -> bool:
        """Generate synthetic obs for one window via ``perfect_model_obs``.

        Network entries are placed at ``analysis_time + entry.time_offset``
        and the window ``(date0, date1]`` bounds the namelist's obs times.

        Returns
        -------
        bool
            ``True`` if ``perfect_model_obs`` ran successfully and
            ``output_file`` was written; ``False`` otherwise.
        """
        active = [
            e for e in self.obs_network
            if e.obs_type in obs_types
            and lat_min <= e.lat <= lat_max
            and lon_min <= e.lon <= lon_max
        ]
        if not active:
            return False

        # Name the scratch dir for the analysis time, so it matches the output
        # filename rather than the window's lower edge.
        t = analysis_time
        secs_of_day = t.hour * 3600 + t.minute * 60 + t.second
        date_str = f"{t.year:04d}-{t.month:02d}-{t.day:02d}-{secs_of_day:05d}"
        window_dir = os.path.join(self.dart_work_dir, "windows", date_str)
        self._setup_window_dir(window_dir)

        obs_seq_in = os.path.join(window_dir, "obs_seq.in")
        obs_seq_out = os.path.join(window_dir, "obs_seq.out")
        src_nml = os.path.join(self.dart_work_dir, "input.nml")
        dest_nml = os.path.join(window_dir, "input.nml")

        try:
            _write_obs_seq_template(active, analysis_time, obs_seq_in)
            _patch_input_nml(src_nml, dest_nml, "obs_seq.in", "obs_seq.out", date0, date1)

            result = subprocess.run(
                [self.perfect_model_obs_exe],
                cwd=window_dir,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(
                    f"perfect_model_obs failed for analysis time "
                    f"{analysis_time.isoformat()}:\n{result.stderr}"
                )
                return False

            if not os.path.exists(obs_seq_out):
                return False

            shutil.move(obs_seq_out, output_file)
            return True

        finally:
            shutil.rmtree(window_dir, ignore_errors=True)
