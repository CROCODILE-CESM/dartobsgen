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
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta

_DART_EPOCH = datetime(1601, 1, 1)

# Days from 0001-01-01 to 1601-01-01 (proleptic Gregorian), matching
# dart_base_date_in_days in read_model_time() in DART's MOM6 model_mod.f90.
_MOM6_DAYS_TO_DART_EPOCH = 584388

_MOM6_TIME_EPOCH = "0001-01-01"

# Restart-format variable names DART's MOM6 model_mod reads by default
# (model_state_variables in input.nml).
_MOM6_RESTART_VARS = ("Temp", "Salt", "u", "v", "h")

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
        The time DART will read from the file — i.e. after any truncation
        the model_mod's ``read_model_time`` applies.  Synthetic observations
        must be placed at (or near) this time because ``perfect_model_obs``
        cannot advance large models.
    """

    path: str
    valid_time: datetime


class ModelStateProvider(ABC):
    """Supplies one single-timeslice model state per assimilation window."""

    @abstractmethod
    def state_for_window(self, date0: datetime, date1: datetime) -> ModelState | None:
        """Return the model state for the half-open window ``[date0, date1)``.

        Returns
        -------
        ModelState or None
            ``None`` if no model state falls within the window; the window
            is then skipped (no obs_seq file is written).
        """


def mom6_time_to_datetime(raw_days: float) -> datetime:
    """Convert a MOM6 ``Time`` value (days since 0001-01-01) to the datetime DART sees.

    Mirrors ``read_model_time`` in DART's MOM6 ``model_mod.f90``: the day
    count is truncated to whole days and seconds are discarded, so e.g. a
    daily-mean slice stamped at noon maps to 00:00 of that day.
    """
    return _DART_EPOCH + timedelta(days=int(raw_days) - _MOM6_DAYS_TO_DART_EPOCH)


class MOM6StateProvider(ModelStateProvider):
    """Serve single-timeslice MOM6 states from restart-format model output.

    Parameters
    ----------
    model_output : str or list[str]
        Path, glob pattern, or explicit list of paths to MOM6 output in
        restart format.  Files may hold one or many timeslices; the union of
        all slices across all files forms the available states.
    cache_dir : str
        Directory where extracted single-timeslice files are written
        (created on first use).  Extractions are cached by valid time, so
        reruns and parallel windows reuse existing slices.  Files that
        already hold a single timeslice are used in place, uncopied.
    required_vars : tuple of str
        Variables every input file must contain.  Defaults to the restart
        names DART's MOM6 model_mod expects (``Temp``, ``Salt``, ``u``,
        ``v``, ``h``).  History/diagnostic output (``thetao``, ``so``, ...)
        is rejected; override only if ``model_state_variables`` in
        ``input.nml`` was changed to match your file.

    Notes
    -----
    **Slice selection** — one slice per window: the earliest slice whose
    DART-visible time falls in ``[date0, date1)``.  Additional slices in the
    same window are ignored.

    **Time handling** — DART truncates MOM6 times to whole days (see
    :func:`mom6_time_to_datetime`), so selection and ``valid_time`` use the
    truncated time, keeping observation placement consistent with what
    ``perfect_model_obs`` computes.

    **Parallel safety** — extracted slices are written to a temporary name
    and moved into place atomically, so concurrent workers extracting the
    same slice cannot corrupt the cache.
    """

    def __init__(
        self,
        model_output: str | list[str],
        cache_dir: str,
        required_vars: tuple[str, ...] = _MOM6_RESTART_VARS,
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
        self.required_vars = tuple(required_vars)
        # [(path, time_index, raw_days)], sorted by raw_days
        self._index = self._scan([os.path.abspath(f) for f in files])

    def _scan(self, files: list[str]) -> list[tuple[str, int, float]]:
        """Validate each file and index every timeslice it contains."""
        import numpy as np  # noqa: PLC0415
        import xarray as xr  # noqa: PLC0415

        index: list[tuple[str, int, float]] = []
        for path in files:
            with xr.open_dataset(path, decode_times=False) as ds:
                if "Time" not in ds.variables:
                    raise ValueError(
                        f"{path}: no 'Time' variable; expected MOM6 "
                        "restart-format output"
                    )
                self._check_time_metadata(dict(ds["Time"].attrs), path)
                missing = [v for v in self.required_vars if v not in ds.variables]
                if missing:
                    raise ValueError(
                        f"{path}: missing restart variable(s) {missing}. "
                        "DART's MOM6 model_mod reads restart-format output "
                        f"({', '.join(self.required_vars)} on native layers); "
                        "history/diagnostic files are not supported."
                    )
                for i, raw in enumerate(np.atleast_1d(ds["Time"].values)):
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

    def state_for_window(self, date0: datetime, date1: datetime) -> ModelState | None:
        for path, time_index, raw in self._index:
            valid_time = mom6_time_to_datetime(raw)
            if date0 <= valid_time < date1:
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
            if ds.sizes.get("Time", 1) <= 1:
                return src_path  # already a single timeslice
            if os.path.exists(out):
                return out
            os.makedirs(self.cache_dir, exist_ok=True)
            tmp = f"{out}.tmp.{os.getpid()}"
            # isel with a list keeps Time as a size-1 dimension, so the
            # slice keeps the structure of a real MOM6 restart file.
            ds.isel(Time=[time_index]).to_netcdf(tmp)
        os.replace(tmp, out)
        return out
