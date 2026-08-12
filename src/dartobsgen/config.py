from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(kw_only=True)
class ObsGenConfig:
    """Configuration for a dartobsgen run.

    Analysis times run from ``first_analysis`` through ``end`` inclusive,
    spaced by ``assimilation_frequency``.  Each analysis time ``T`` produces
    one obs_seq file named for ``T`` and holding the observations in
    ``(T - freq/2, T + freq/2]``, which is DART's convention.

    Give **exactly one** of ``start`` or ``first_analysis``; the other is
    derived and both are populated after construction.  All fields are
    keyword-only.

    Parameters
    ----------
    start : datetime, optional
        Start of the **model run** — not the first analysis time.  The model
        advances one assimilation period before the first assimilation, so
        ``first_analysis = start + assimilation_frequency``.  Natural when
        the observations exist independently of the model (real data from
        CrocoLake or NNJA): pick the time you initialize the model.
    first_analysis : datetime, optional
        The first analysis time itself.  Natural when the observation times
        are fixed by something else and you need the analysis times to land
        on them — in particular ``PerfectModelSource``, where synthetic obs
        are placed at the valid times of the model states being interpolated,
        so the analysis times must match the model output times.
    end : datetime
        Last analysis time (inclusive).  Must be at or after
        ``first_analysis``.
    lat_min, lat_max : float
        Latitude bounds in degrees.
    lon_min, lon_max : float
        Longitude bounds in degrees (-180 to 180).
    obs_types : list[str]
        Observation types to include.  Accepts DART compound names
        (e.g. ``"ARGO_TEMPERATURE"``), DART variable names
        (e.g. ``"TEMPERATURE"``), or CrocoLake variable names
        (e.g. ``"TEMP"``).
    assimilation_frequency : timedelta
        Width of each assimilation window, and the spacing between analysis
        times.  Must be an even whole number of seconds so the half-width
        lands on an integer second.  Default ``timedelta(hours=6)``.
    output_dir : str
        Directory where obs_seq files are written.  Created if absent.
    output_prefix : str
        Filename prefix.  Files are named
        ``{output_prefix}.{timestamp}.out``.
    output_timestamp_format : str
        Format string for the timestamp portion of the filename, applied to
        the **analysis time**.
        Supports all Python ``strftime`` codes **and** the special
        token ``{S}`` which is replaced with the zero-padded
        seconds-of-day (00000–86400), matching DART's naming convention.
        Default: ``"%Y-%m-%d-{S}"`` → e.g. ``2010-05-01-21600``.
    obs_type_map : dict or None
        Custom mapping that overrides or extends the built-in
        ``DEFAULT_OBS_TYPE_MAP`` in ``CrocLakeSource``.  Each key is an
        obs type name; each value is a dict with keys
        ``"crocolake_var"`` and ``"db_name"`` (``None`` = all sources).
        Pass ``None`` to use defaults only.

    Raises
    ------
    ValueError
        If neither or both of ``start`` and ``first_analysis`` are given, if
        ``assimilation_frequency`` is not a positive even whole number of
        seconds, or if the run contains no analysis times at all
        (``first_analysis > end``).
    """

    start: datetime | None = None
    first_analysis: datetime | None = None

    end: datetime

    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float

    obs_types: list[str]

    assimilation_frequency: timedelta = field(default_factory=lambda: timedelta(hours=6))

    output_dir: str = "."
    output_prefix: str = "obs_seq"
    output_timestamp_format: str = "%Y-%m-%d-{S}"

    obs_type_map: dict | None = None

    def __post_init__(self) -> None:
        freq = self.assimilation_frequency
        total = freq.total_seconds()
        if total <= 0 or total != int(total) or int(total) % 2 != 0:
            raise ValueError(
                f"assimilation_frequency must be a positive even whole number of "
                f"seconds so that the window half-width is a whole second; "
                f"got {freq!r} ({total} s)."
            )

        if (self.start is None) == (self.first_analysis is None):
            raise ValueError(
                "Give exactly one of start or first_analysis.  start is the "
                "start of the model run, so the first analysis is one "
                "assimilation_frequency later; first_analysis is the first "
                "analysis time itself."
            )

        # Derive whichever was not given, so both are available downstream.
        if self.start is None:
            self.start = self.first_analysis - freq
        else:
            self.first_analysis = self.start + freq

        if self.first_analysis > self.end:
            raise ValueError(
                f"No analysis times in the run: first analysis "
                f"{self.first_analysis.isoformat()} is after end "
                f"{self.end.isoformat()}.  Remember that end is the last "
                f"analysis time, and that a start of "
                f"{self.start.isoformat()} puts the first analysis one "
                f"assimilation_frequency ({freq}) later."
            )
