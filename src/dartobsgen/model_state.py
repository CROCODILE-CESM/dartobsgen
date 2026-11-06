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


# Namelist entries that name an input file a ``perfect_model_obs`` run must be
# able to read, used when a provider does not declare its own
# (``ModelStateProvider.input_file_keys``).  Deliberately a fixed list rather than a scan for ``*_file`` /
# ``*_files``: such a scan sweeps up output files, state-file *lists* and log
# names (``filter_nml`` alone has several) and would demand files that are
# written, not read.
DEFAULT_INPUT_FILE_KEYS: tuple[tuple[str, str], ...] = (
    ("model_nml", "template_file"),
    ("model_nml", "static_file"),
    ("model_nml", "ocean_geometry"),
    ("perfect_model_obs_nml", "input_state_files"),
)


class ModelStateProvider(ABC):
    """Supplies one single-timeslice model state per assimilation window.

    Subclasses implement :meth:`state_for_window`, and may override
    :meth:`available_times` and :attr:`input_file_keys`.  Providers backed by
    netCDF files usually subclass :class:`TimesliceStateProvider` instead,
    which supplies the file indexing, window selection and slice cache.
    """

    #: ``(namelist group, key)`` pairs naming the input files a
    #: ``perfect_model_obs`` run for this model must be able to read.
    #: ``PerfectModelSource`` checks them against its run directory.
    input_file_keys: tuple[tuple[str, str], ...] = DEFAULT_INPUT_FILE_KEYS

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


def state_vars_from_nml(nml_path: str, fields_per_entry: int = 5) -> tuple[str, ...]:
    """Return the netCDF variable names in ``model_nml``'s ``model_state_variables``.

    ``model_state_variables`` is a flat list of fixed-length records whose
    first element is the variable name.  The record length depends on the
    model: MOM6 uses 5 ``(variable, quantity, min, max, update)``, CICE
    uses 3 ``(variable, quantity, update)``.  This returns the first element
    of each record.  Pass the result as ``required_vars`` to a provider so
    model output is validated against exactly what DART will read.

    Parameters
    ----------
    nml_path : str
        Path to the ``input.nml``.
    fields_per_entry : int
        Number of elements in one record (default 5, MOM6).
    """
    if fields_per_entry < 1:
        raise ValueError(f"fields_per_entry must be >= 1, got {fields_per_entry}")
    import f90nml  # noqa: PLC0415

    nml = f90nml.read(nml_path)
    entries = nml["model_nml"]["model_state_variables"]
    names = entries[0::fields_per_entry]
    return tuple(str(v).strip() for v in names if v is not None and str(v).strip())


def input_files_from_nml(
    nml_path: str, keys: tuple[tuple[str, str], ...] | None = None
) -> dict[str, str]:
    """Map each input file named in *nml_path* to the entry that names it.

    Returns ``{filename: "group:key"}`` for the ``(group, key)`` namelist
    entries in *keys* (default :data:`DEFAULT_INPUT_FILE_KEYS`; a provider's
    :attr:`~ModelStateProvider.input_file_keys` is the usual source), in
    namelist order.  A file named by more
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
    for group, key in DEFAULT_INPUT_FILE_KEYS if keys is None else keys:
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


class TimesliceStateProvider(ModelStateProvider):
    """Base for providers that index timeslices across a set of netCDF files.

    Owns everything that does not depend on the model: resolving the file
    glob or list, indexing every timeslice by its DART-visible valid time,
    choosing one slice per window, and caching extracted slices.  A subclass
    supplies the model-specific parts through three hooks:

    * :meth:`_validate` — check a file looks like this model's output
      (time metadata, calendar, ...) and raise ``ValueError`` if not.
    * :meth:`_scan_file` — return ``(time_index, valid_time)`` for each
      timeslice in the file.  ``valid_time`` must be computed exactly as the
      model's DART ``read_model_time`` computes it.
    * :meth:`_materialise` — turn one indexed slice into a single-timeslice
      file (the default uses the file in place, which is right for models
      that write one timeslice per file).

    Parameters
    ----------
    model_output : str or list[str]
        Path, glob pattern, or explicit list of paths to model output files.
        Files may hold one or many timeslices; the union of all slices
        across all files forms the available states.
    cache_dir : str
        Directory where extracted single-timeslice files are written
        (created on first use).  Extractions are cached by valid time, so
        reruns and parallel windows reuse existing slices.
    required_vars : tuple of str, optional
        Variables every input file must contain; construction fails with a
        clear message if any are absent.  Use :func:`state_vars_from_nml` to
        take the list straight from ``model_state_variables`` in
        ``input.nml``.  ``None`` (default) skips the check.

    Notes
    -----
    **Slice selection** — one slice per window: the earliest slice whose
    DART-visible time falls in ``(date0, date1]``.  Additional slices in the
    same window are ignored.  The interval is open at the lower edge to match
    the window ``perfect_model_obs`` is given (``first_obs = date0 + 1s``);
    a slice landing exactly on ``date0`` belongs to the previous window, and
    selecting it here would place every obs one second before ``first_obs``.

    **Parallel safety** — extracted slices are written to a temporary name
    and moved into place atomically (see :meth:`_write_cached`), so
    concurrent workers extracting the same slice cannot corrupt the cache.
    """

    #: Prefix of cached slice filenames: ``{cache_prefix}_{YYYYmmdd_HHMMSS}.nc``.
    cache_prefix: str = "state"

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
        # [(path, time_index, valid_time)], sorted by valid_time
        self._index = self._scan([os.path.abspath(f) for f in files])

    # -- hooks ---------------------------------------------------------

    @abstractmethod
    def _validate(self, ds, path: str) -> None:
        """Raise ``ValueError`` if open dataset *ds* is not this model's output."""

    @abstractmethod
    def _scan_file(self, ds, path: str) -> list[tuple[int, datetime]]:
        """Return ``(time_index, valid_time)`` for every timeslice in *ds*."""

    def _materialise(self, ds, src_path: str, time_index: int, cache_path: str) -> str:
        """Return the path of a single-timeslice file for one indexed slice.

        *ds* is *src_path* opened with ``decode_times=False``.  The default
        returns *src_path* itself.  A model whose files hold several slices
        overrides this to write slice *time_index* to *cache_path* via
        :meth:`_write_cached`, and returns the path it wrote (or *src_path*
        when the file already holds a single slice).
        """
        return src_path

    # -- machinery -----------------------------------------------------

    def _scan(self, files: list[str]) -> list[tuple[str, int, datetime]]:
        """Validate each file and index every timeslice it contains."""
        import xarray as xr  # noqa: PLC0415

        index: list[tuple[str, int, datetime]] = []
        for path in files:
            with xr.open_dataset(path, decode_times=False) as ds:
                self._validate(ds, path)
                if self.required_vars is not None:
                    missing = [v for v in self.required_vars if v not in ds.variables]
                    if missing:
                        raise ValueError(
                            f"{path}: missing state variable(s) {missing}; "
                            "the file does not match the expected "
                            "model_state_variables."
                        )
                for time_index, valid_time in self._scan_file(ds, path):
                    index.append((path, time_index, valid_time))
        index.sort(key=lambda entry: entry[2])
        return index

    def available_times(self) -> list[datetime]:
        """Valid times of every indexed timeslice, ascending."""
        return [valid_time for _, _, valid_time in self._index]

    def state_for_window(self, date0: datetime, date1: datetime) -> ModelState | None:
        for path, time_index, valid_time in self._index:
            if date0 < valid_time <= date1:
                return ModelState(
                    path=self._extract(path, time_index, valid_time),
                    valid_time=valid_time,
                )
        return None

    def _extract(self, src_path: str, time_index: int, valid_time: datetime) -> str:
        """Return the single-timeslice file for one indexed slice."""
        import xarray as xr  # noqa: PLC0415

        cache_path = os.path.join(
            self.cache_dir, f"{self.cache_prefix}_{valid_time:%Y%m%d_%H%M%S}.nc"
        )
        with xr.open_dataset(src_path, decode_times=False) as ds:
            return self._materialise(ds, src_path, time_index, cache_path)

    def _write_cached(self, cache_path: str, write) -> str:
        """Create *cache_path* by calling ``write(tmp_path)``, atomically.

        Writes to a per-process temporary name and ``os.replace``s it into
        place, so concurrent workers never see a partial file.
        """
        os.makedirs(self.cache_dir, exist_ok=True)
        tmp = f"{cache_path}.tmp.{os.getpid()}"
        write(tmp)
        os.replace(tmp, cache_path)
        return cache_path


class MOM6StateProvider(TimesliceStateProvider):
    """Serve single-timeslice MOM6 states from a run's model output.

    Works with both restart-format output (``Temp``, ``Salt``, ... on native
    layers) and z-space history output (``thetao``, ``so``, ... on ``z_l``
    levels, supported by DART's MOM6 model_mod via ``use_pseudo_depth``) —
    whatever matches ``model_state_variables`` in the run's ``input.nml``.
    Parameters and slice selection are described on
    :class:`TimesliceStateProvider`.

    Notes
    -----
    **Time handling** — selection and ``valid_time`` use the time exactly as
    DART's ``read_model_time`` computes it (see
    :func:`mom6_time_to_datetime`), keeping observation placement consistent
    with what ``perfect_model_obs`` computes.

    **Slices** — files already holding a single timeslice are used in place,
    uncopied; multi-slice files are split into the cache.
    """

    cache_prefix = "mom6_state"

    @staticmethod
    def _find_time_var(ds, path: str) -> str:
        for name in _MOM6_TIME_VAR_NAMES:
            if name in ds.variables:
                return name
        raise ValueError(
            f"{path}: no {' or '.join(repr(n) for n in _MOM6_TIME_VAR_NAMES)} "
            "variable; expected MOM6 model output"
        )

    def _validate(self, ds, path: str) -> None:
        time_var = self._find_time_var(ds, path)
        self._check_time_metadata(dict(ds[time_var].attrs), path)

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

    def _scan_file(self, ds, path: str) -> list[tuple[int, datetime]]:
        import numpy as np  # noqa: PLC0415

        time_var = self._find_time_var(ds, path)
        return [
            (i, mom6_time_to_datetime(float(raw)))
            for i, raw in enumerate(np.atleast_1d(ds[time_var].values))
        ]

    def _materialise(self, ds, src_path: str, time_index: int, cache_path: str) -> str:
        time_var = self._find_time_var(ds, src_path)
        if ds.sizes.get(time_var, 1) <= 1:
            return src_path  # already a single timeslice
        if os.path.exists(cache_path):
            return cache_path
        # isel with a list keeps time as a size-1 dimension, so the slice
        # keeps the structure of the original MOM6 file.
        return self._write_cached(
            cache_path, lambda tmp: ds.isel({time_var: [time_index]}).to_netcdf(tmp)
        )


def cice_time_to_datetime(nyr: int, month: int, mday: int, sec: int) -> datetime:
    """Convert CICE restart time attributes to the datetime DART sees.

    Mirrors ``read_model_time`` in DART's CICE ``model_mod.f90``: the global
    attributes ``nyr``, ``month``, ``mday`` and ``sec`` (seconds into the
    day) give a Gregorian date.  DART cannot represent year 0, so
    ``nyr == 0`` is read as year 1, as DART does.
    """
    if nyr == 0:
        nyr = 1
    return datetime(int(nyr), int(month), int(mday)) + timedelta(seconds=int(sec))


_CICE_TIME_ATTRS = ("nyr", "month", "mday", "sec")


class CICEStateProvider(TimesliceStateProvider):
    """Serve single-timeslice CICE states from a run's restart files.

    CICE restart files hold exactly one timeslice, with the time stored in
    the global attributes ``nyr``, ``month``, ``mday`` and ``sec`` (there is
    no time variable), so each file is one state and is used in place; the
    ``cache_dir`` is never written to.  Parameters and slice selection are
    described on :class:`TimesliceStateProvider`.

    Use ``state_vars_from_nml(nml, fields_per_entry=3)`` to build
    ``required_vars`` from a CICE ``input.nml``.

    Notes
    -----
    ``valid_time`` is computed as DART's ``read_model_time`` computes it
    (see :func:`cice_time_to_datetime`).

    Only ``input_state_files`` is declared in :attr:`input_file_keys`: CICE's
    template and grid files are fixed names inside ``model_mod`` rather than
    namelist entries, so they cannot be checked from ``input.nml``.
    """

    cache_prefix = "cice_state"
    input_file_keys = (("perfect_model_obs_nml", "input_state_files"),)

    def _validate(self, ds, path: str) -> None:
        missing = [a for a in _CICE_TIME_ATTRS if a not in ds.attrs]
        if missing:
            raise ValueError(
                f"{path}: missing global attribute(s) {missing}; DART's CICE "
                "read_model_time needs nyr, month, mday and sec in the "
                "restart file."
            )

    def _scan_file(self, ds, path: str) -> list[tuple[int, datetime]]:
        try:
            valid_time = cice_time_to_datetime(*(int(ds.attrs[a]) for a in _CICE_TIME_ATTRS))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{path}: cannot decode CICE restart time: {exc}") from exc
        return [(0, valid_time)]
