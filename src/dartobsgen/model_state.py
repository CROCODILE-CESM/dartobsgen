"""Model-state providers: map assimilation windows to single-timeslice state files.

``perfect_model_obs`` reads exactly one model state, but model output commonly
holds many timeslices (or is spread over many files).  A
:class:`ModelStateProvider` bridges the two: given an assimilation window it
returns the path to a single-timeslice netCDF file plus the time DART will
read from it.  All model-specific knowledge (variable names, time decoding,
slice extraction) lives in the provider, so
:class:`~dartobsgen.sources.perfect_model.PerfectModelSource` stays
model-agnostic.
"""

from __future__ import annotations

import glob
import math
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta

_DART_EPOCH = datetime(1601, 1, 1)

# Days from 0001-01-01 to 1601-01-01 (proleptic Gregorian), matching
# dart_base_date_in_days in read_model_time() in DART's MOM6 model_mod.f90.
_MOM6_DAYS_TO_DART_EPOCH = 584388

_MOM6_TIME_EPOCH = "0001-01-01"

# MOM6 restart files name the time variable 'Time'; history files use 'time'.
_MOM6_TIME_VAR_NAMES = ("Time", "time")

# Calendars whose day counts agree with DART's Gregorian calendar for
# modern dates.  Anything else (noleap, julian, 360_day, ...) would shift
# dates relative to what DART computes from the same raw day number.
_GREGORIAN_CALENDARS = frozenset({"gregorian", "standard", "proleptic_gregorian"})


@dataclass(frozen=True)
class ModelState:
    """A single-timeslice model state ready for ``perfect_model_obs``.

    Parameters
    ----------
    path : str
        Path to a netCDF file holding exactly one timeslice, in the format
        the DART model interface reads (for MOM6: restart format).
    valid_time : datetime
        The time DART will read from the file, computed exactly as the
        model_mod's ``read_model_time`` computes it.  Synthetic observations
        must be placed at (or near) this time because ``perfect_model_obs``
        cannot advance large models.
    """

    path: str
    valid_time: datetime


class ModelStateProvider(ABC):
    """Supplies one single-timeslice model state per assimilation window."""

    @abstractmethod
    def state_for_window(self, date0: datetime, date1: datetime) -> ModelState | None:
        """Return the model state for the window ``(date0, date1]``.

        The interval is open below and closed above, matching the window
        ``perfect_model_obs`` is given (``first_obs = date0 + 1s``,
        ``last_obs = date1``).  A state landing exactly on ``date0`` belongs
        to the previous window; returning it here would place every
        observation one second before ``first_obs``.

        Returns
        -------
        ModelState or None
            ``None`` if no model state falls within the window; the window
            is then skipped (no obs_seq file is written).
        """

    def available_times(self) -> list[datetime] | None:
        """Return the valid times of every state this provider can serve.

        Sorted ascending.  The default returns ``None``, meaning the
        provider cannot enumerate its states; ``PerfectModelSource`` then
        skips its pre-flight coverage check rather than guessing.  Override
        this in providers that index their input up front, so a mismatch
        between the run's analysis times and the model output times is
        reported before any window runs.
        """
        return None


def state_vars_from_nml(nml_path: str) -> tuple[str, ...]:
    """Return the netCDF variable names in ``model_nml``'s ``model_state_variables``.

    ``model_state_variables`` is a flat list of 5-element records
    ``(variable, quantity, min, max, update)``; this returns the first
    element of each record.  Pass the result as ``required_vars`` to
    :class:`MOM6StateProvider` so model output is validated against exactly
    what DART will read.
    """
    import f90nml  # noqa: PLC0415

    nml = f90nml.read(nml_path)
    entries = nml["model_nml"]["model_state_variables"]
    names = entries[0::5]
    return tuple(str(v).strip() for v in names if v is not None and str(v).strip())


# Namelist entries that name an input file a ``perfect_model_obs`` run must be
# able to read.  Deliberately a fixed list rather than a scan for ``*_file`` /
# ``*_files``: such a scan sweeps up output files, state-file *lists* and log
# names (``filter_nml`` alone has several) and would demand files that are
# written, not read.
_NML_INPUT_FILE_KEYS: tuple[tuple[str, str], ...] = (
    ("model_nml", "template_file"),
    ("model_nml", "static_file"),
    ("model_nml", "ocean_geometry"),
    ("perfect_model_obs_nml", "input_state_files"),
)


def input_files_from_nml(nml_path: str) -> dict[str, str]:
    """Map each input file named in *nml_path* to the entry that names it.

    Returns ``{filename: "group:key"}`` for the namelist entries in
    :data:`_NML_INPUT_FILE_KEYS`, in namelist order.  A file named by more
    than one entry is reported once, against the first entry that names it.
    Groups and keys that are absent, blank, or set to a filename of ``''``
    are skipped, so a namelist for a model without these entries (Lorenz 96,
    say) yields an empty mapping rather than an error.

    The ``group:key`` values are for error messages — they let a missing file
    be reported against the namelist entry that asked for it.
    :class:`~dartobsgen.sources.perfect_model.PerfectModelSource` uses this
    to check its run directory before any window runs.
    """
    import f90nml  # noqa: PLC0415

    nml = f90nml.read(nml_path)
    files: dict[str, str] = {}
    for group, key in _NML_INPUT_FILE_KEYS:
        if group not in nml or key not in nml[group]:
            continue
        value = nml[group][key]
        entries = value if isinstance(value, list) else [value]
        for entry in entries:
            name = str(entry).strip() if entry is not None else ""
            if name:
                files.setdefault(name, f"{group}:{key}")
    return files


def mom6_time_to_datetime(raw_days: float) -> datetime:
    """Convert a MOM6 ``Time`` value (days since 0001-01-01) to the datetime DART sees.

    Mirrors ``read_model_time`` in DART's MOM6 ``model_mod.f90``: whole days
    via ``floor``, the fractional day converted to seconds and truncated to
    an integer.  A daily-mean slice stamped at noon therefore maps to 12:00.
    """
    days = math.floor(raw_days)
    seconds = int((raw_days - days) * 86400.0)
    return _DART_EPOCH + timedelta(days=days - _MOM6_DAYS_TO_DART_EPOCH, seconds=seconds)


class MOM6StateProvider(ModelStateProvider):
    """Serve single-timeslice MOM6 states from a run's model output.

    Works with both restart-format output (``Temp``, ``Salt``, ... on native
    layers) and z-space history output (``thetao``, ``so``, ... on ``z_l``
    levels, supported by DART's MOM6 model_mod via ``use_pseudo_depth``) —
    whatever matches ``model_state_variables`` in the run's ``input.nml``.

    Parameters
    ----------
    model_output : str or list[str]
        Path, glob pattern, or explicit list of paths to MOM6 output files.
        Files may hold one or many timeslices; the union of all slices
        across all files forms the available states.
    cache_dir : str
        Directory where extracted single-timeslice files are written
        (created on first use).  Extractions are cached by valid time, so
        reruns and parallel windows reuse existing slices.  Files that
        already hold a single timeslice are used in place, uncopied.
    required_vars : tuple of str, optional
        Variables every input file must contain; construction fails with a
        clear message if any are absent.  Use
        :func:`state_vars_from_nml` to take the list straight from
        ``model_state_variables`` in ``input.nml``.  ``None`` (default)
        skips the check.

    Notes
    -----
    **Slice selection** — one slice per window: the earliest slice whose
    DART-visible time falls in ``(date0, date1]``.  Additional slices in the
    same window are ignored.  The interval is open at the lower edge to match
    the window ``perfect_model_obs`` is given (``first_obs = date0 + 1s``);
    a slice landing exactly on ``date0`` belongs to the previous window, and
    selecting it here would place every obs one second before ``first_obs``.

    **Time handling** — selection and ``valid_time`` use the time exactly as
    DART's ``read_model_time`` computes it (see
    :func:`mom6_time_to_datetime`), keeping observation placement consistent
    with what ``perfect_model_obs`` computes.

    **Parallel safety** — extracted slices are written to a temporary name
    and moved into place atomically, so concurrent workers extracting the
    same slice cannot corrupt the cache.
    """

    def __init__(
        self,
        model_output: str | list[str],
        cache_dir: str,
        required_vars: tuple[str, ...] | None = None,
    ):
        if isinstance(model_output, str):
            files = sorted(glob.glob(model_output))
            if not files:
                raise FileNotFoundError(
                    f"no model output files match {model_output!r}"
                )
        else:
            files = [str(p) for p in model_output]
            missing = [p for p in files if not os.path.exists(p)]
            if missing:
                raise FileNotFoundError(f"model output file(s) not found: {missing}")

        self.cache_dir = os.path.abspath(cache_dir)
        self.required_vars = tuple(required_vars) if required_vars is not None else None
        # [(path, time_index, raw_days)], sorted by raw_days
        self._index = self._scan([os.path.abspath(f) for f in files])

    @staticmethod
    def _find_time_var(ds, path: str) -> str:
        for name in _MOM6_TIME_VAR_NAMES:
            if name in ds.variables:
                return name
        raise ValueError(
            f"{path}: no {' or '.join(repr(n) for n in _MOM6_TIME_VAR_NAMES)} "
            "variable; expected MOM6 model output"
        )

    def _scan(self, files: list[str]) -> list[tuple[str, int, float]]:
        """Validate each file and index every timeslice it contains."""
        import numpy as np  # noqa: PLC0415
        import xarray as xr  # noqa: PLC0415

        index: list[tuple[str, int, float]] = []
        for path in files:
            with xr.open_dataset(path, decode_times=False) as ds:
                time_var = self._find_time_var(ds, path)
                self._check_time_metadata(dict(ds[time_var].attrs), path)
                if self.required_vars is not None:
                    missing = [v for v in self.required_vars if v not in ds.variables]
                    if missing:
                        raise ValueError(
                            f"{path}: missing state variable(s) {missing}; "
                            "the file does not match the expected "
                            "model_state_variables."
                        )
                for i, raw in enumerate(np.atleast_1d(ds[time_var].values)):
                    index.append((path, i, float(raw)))
        index.sort(key=lambda entry: entry[2])
        return index

    @staticmethod
    def _check_time_metadata(attrs: dict, path: str) -> None:
        units = str(attrs.get("units", ""))
        if not units.lower().startswith(f"days since {_MOM6_TIME_EPOCH}"):
            raise ValueError(
                f"{path}: Time units {units!r}; DART's MOM6 read_model_time "
                f"assumes 'days since {_MOM6_TIME_EPOCH}'."
            )
        calendar = str(attrs.get("calendar", "gregorian")).lower()
        if calendar not in _GREGORIAN_CALENDARS:
            raise ValueError(
                f"{path}: calendar {calendar!r} does not match DART's "
                "Gregorian day count; dates would shift relative to what "
                "DART computes."
            )

    def available_times(self) -> list[datetime]:
        """Valid times of every indexed timeslice, ascending.

        ``_index`` is already sorted by raw day number, and
        :func:`mom6_time_to_datetime` is monotonic, so this preserves order.
        """
        return [mom6_time_to_datetime(raw) for _, _, raw in self._index]

    def state_for_window(self, date0: datetime, date1: datetime) -> ModelState | None:
        for path, time_index, raw in self._index:
            valid_time = mom6_time_to_datetime(raw)
            if date0 < valid_time <= date1:
                return ModelState(
                    path=self._extract(path, time_index, valid_time),
                    valid_time=valid_time,
                )
        return None

    def _extract(self, src_path: str, time_index: int, valid_time: datetime) -> str:
        """Write timeslice *time_index* of *src_path* to the cache; return its path."""
        import xarray as xr  # noqa: PLC0415

        out = os.path.join(
            self.cache_dir, f"mom6_state_{valid_time:%Y%m%d_%H%M%S}.nc"
        )
        with xr.open_dataset(src_path, decode_times=False) as ds:
            time_var = self._find_time_var(ds, src_path)
            if ds.sizes.get(time_var, 1) <= 1:
                return src_path  # already a single timeslice
            if os.path.exists(out):
                return out
            os.makedirs(self.cache_dir, exist_ok=True)
            tmp = f"{out}.tmp.{os.getpid()}"
            # isel with a list keeps time as a size-1 dimension, so the
            # slice keeps the structure of the original MOM6 file.
            ds.isel({time_var: [time_index]}).to_netcdf(tmp)
        os.replace(tmp, out)
        return out
