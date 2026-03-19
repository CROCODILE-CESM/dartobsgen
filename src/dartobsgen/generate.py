from __future__ import annotations

import os
from datetime import datetime, timedelta

from .config import ObsGenConfig
from .sources.base import DataSource


def _format_timestamp(dt: datetime, fmt: str) -> str:
    """Format *dt* with strftime *fmt*, also replacing ``{S}`` with
    zero-padded seconds-of-day (00000–86400).

    This supports DART's standard obs_seq filename convention where the
    time component is total seconds elapsed since midnight.
    """
    seconds_of_day = dt.hour * 3600 + dt.minute * 60 + dt.second
    intermediate = fmt.replace("{S}", f"{seconds_of_day:05d}")
    return dt.strftime(intermediate)


def _make_windows(
    start: datetime, end: datetime, freq: timedelta
) -> list[tuple[datetime, datetime]]:
    """Return half-open time windows ``[t0, t0 + freq)`` starting at *start*.

    Windows are exactly *freq* wide.  The last window may extend
    beyond *end* to keep all window sizes uniform; data queries use
    ``JULD < date1`` so observations are fetched only within each window.
    """
    delta = freq
    windows: list[tuple[datetime, datetime]] = []
    t0 = start
    while t0 < end:
        windows.append((t0, t0 + delta))
        t0 += delta
    return windows


def generate_obs_sequences(config: ObsGenConfig, source: DataSource) -> list[str]:
    """Generate one DART obs_seq file per assimilation window.

    Iterates over non-overlapping half-open windows from ``config.start``
    to ``config.end``, calls ``source.write_obs_seq`` for each, and
    returns the paths of every file that was written.  Windows that
    contain no observations are silently skipped.

    Parameters
    ----------
    config : ObsGenConfig
        Run configuration (time range, bbox, obs types, window width,
        output path and naming settings).
    source : DataSource
        Observation data source (e.g. ``CrocLakeSource``).

    Returns
    -------
    list[str]
        Paths of obs_seq files written to disk (empty windows omitted).
    """
    os.makedirs(config.output_dir, exist_ok=True)
    windows = _make_windows(config.start, config.end, config.assimilation_frequency)
    written: list[str] = []

    for date0, date1 in windows:
        timestamp = _format_timestamp(date0, config.output_timestamp_format)
        output_file = os.path.join(
            config.output_dir, f"{config.output_prefix}.{timestamp}.out"
        )
        print(
            f"Window {date0.isoformat()} → {date1.isoformat()} "
            f"→ {os.path.basename(output_file)}"
        )
        success = source.write_obs_seq(
            output_file=output_file,
            date0=date0,
            date1=date1,
            lat_min=config.lat_min,
            lat_max=config.lat_max,
            lon_min=config.lon_min,
            lon_max=config.lon_max,
            obs_types=config.obs_types,
            obs_type_map=config.obs_type_map,
        )
        if success:
            written.append(output_file)

    return written
