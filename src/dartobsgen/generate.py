from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
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


def _make_analysis_windows(
    start: datetime, end: datetime, freq: timedelta
) -> list[tuple[datetime, datetime, datetime]]:
    """Return ``(analysis_time, date0, date1)`` triples, one per assimilation cycle.

    *start* is the **start of the model run**, not the first analysis time.
    The model must advance one assimilation period before the first
    assimilation, so analysis times are ``start + freq``, ``start + 2*freq``,
    ... up to and including *end*.

    Each analysis time ``T`` gets the window ``(T - freq/2, T + freq/2]``,
    matching DART's convention: obs_seq files are named for the analysis
    time (the model's stopping time) and hold the observations centered on
    it.  See ``obs_sequence_tool.rst`` — "the windows should be centered
    around the assimilation time starting at minus 1/2 the window time plus
    1 second, and ending at exactly plus 1/2 the window time."

    Windows are contiguous and non-overlapping: adjacent windows share a
    boundary instant, which the closed upper bound assigns to the earlier
    window.  The first window starts at ``start + freq/2`` and the last ends
    at ``end + freq/2``.

    Raises
    ------
    ValueError
        If *freq* is not an even whole number of seconds, since ``T - freq/2``
        would not land on an integer second and DART times are integer
        (days, seconds) pairs.
    """
    total = freq.total_seconds()
    if total <= 0 or total != int(total) or int(total) % 2 != 0:
        raise ValueError(
            f"assimilation_frequency must be a positive even whole number of "
            f"seconds so that the window half-width is a whole second; "
            f"got {freq!r} ({total} s)."
        )

    half = freq / 2
    windows: list[tuple[datetime, datetime, datetime]] = []
    analysis_time = start + freq
    while analysis_time <= end:
        windows.append((analysis_time, analysis_time - half, analysis_time + half))
        analysis_time += freq
    return windows


def _run_window(
    source: DataSource,
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
) -> str | None:
    """Write one obs_seq window. Returns the path if written, else None."""
    print(
        f"Analysis {analysis_time.isoformat()} "
        f"window ({date0.isoformat()}, {date1.isoformat()}] "
        f"→ {os.path.basename(output_file)}"
    )
    success = source.write_obs_seq(
        output_file=output_file,
        analysis_time=analysis_time,
        date0=date0,
        date1=date1,
        lat_min=lat_min,
        lat_max=lat_max,
        lon_min=lon_min,
        lon_max=lon_max,
        obs_types=obs_types,
        obs_type_map=obs_type_map,
    )
    return output_file if success else None


def generate_obs_sequences(
    config: ObsGenConfig,
    source: DataSource,
    max_workers: int | None = None,
) -> list[str]:
    """Generate one DART obs_seq file per assimilation cycle.

    Analysis times run from ``config.start + freq`` through ``config.end``
    inclusive; each gets the window ``(T - freq/2, T + freq/2]`` and a file
    named for ``T``, following DART's convention.  Calls
    ``source.write_obs_seq`` for each and returns the paths of every file
    that was written.  Windows that contain no observations are silently
    skipped.

    Parameters
    ----------
    config : ObsGenConfig
        Run configuration (model run span, bbox, obs types, window width,
        output path and naming settings).
    source : DataSource
        Observation data source (e.g. ``CrocLakeSource``).
    max_workers : int or None
        Number of parallel worker processes.
        ``None`` uses all available CPUs; ``1`` runs sequentially.

    Returns
    -------
    list[str]
        Paths of obs_seq files written to disk (empty windows omitted),
        in chronological order.
    """
    os.makedirs(config.output_dir, exist_ok=True)
    windows = _make_analysis_windows(
        config.start, config.end, config.assimilation_frequency
    )

    jobs = [
        (
            os.path.join(
                config.output_dir,
                f"{config.output_prefix}"
                f".{_format_timestamp(analysis_time, config.output_timestamp_format)}"
                f".out",
            ),
            analysis_time,
            date0,
            date1,
        )
        for analysis_time, date0, date1 in windows
    ]
    shared = dict(
        lat_min=config.lat_min,
        lat_max=config.lat_max,
        lon_min=config.lon_min,
        lon_max=config.lon_max,
        obs_types=config.obs_types,
        obs_type_map=config.obs_type_map,
    )

    cpu_count = os.cpu_count() or 1
    if max_workers == 1:
        print(f"Running sequentially (1 worker, {cpu_count} CPU(s) available) over {len(jobs)} window(s).")
        results = [
            _run_window(source, output_file, analysis_time, date0, date1, **shared)
            for output_file, analysis_time, date0, date1 in jobs
        ]
    else:
        effective = max_workers if max_workers is not None else cpu_count
        oversubscribed = effective > cpu_count
        note = f" [oversubscribed: {cpu_count} CPU(s) available]" if oversubscribed else ""
        print(f"Running in parallel with {effective} worker(s) over {len(jobs)} window(s).{note}")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _run_window, source, output_file, analysis_time, date0, date1,
                    **shared,
                )
                for output_file, analysis_time, date0, date1 in jobs
            ]
            results = [f.result() for f in futures]

    return [r for r in results if r is not None]
