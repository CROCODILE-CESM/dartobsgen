from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass
class ObsGenConfig:
    """Configuration for a dartobsgen run.

    Analysis times run from ``start + assimilation_frequency`` through ``end``
    inclusive.  Each analysis time ``T`` produces one obs_seq file named for
    ``T`` and holding the observations in ``(T - freq/2, T + freq/2]``, which
    is DART's convention.

    Parameters
    ----------
    start : datetime
        Start of the **model run** — not the first analysis time.  The model
        advances one assimilation period before the first assimilation, so
        the first analysis time is ``start + assimilation_frequency``.
    end : datetime
        Last analysis time (inclusive).
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
    """

    start: datetime
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
