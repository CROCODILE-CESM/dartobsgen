from __future__ import annotations

import math
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from .base import DataSource

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DART_EPOCH_PY = datetime(1601, 1, 1)
"""DART Gregorian epoch as a Python datetime (1601-01-01 00:00:00 UTC).

We use Python datetime arithmetic rather than pandas timedelta because
pd.Timestamp("1601-01-01") is before the nanosecond overflow boundary.
"""

_PRLC_RE = re.compile(r"^(.+)_PRLC(\d+)$")
"""Matches ADPUPA mandatory-level flat columns, e.g. ``TMDB_PRLC5000``."""

# ---------------------------------------------------------------------------
# Default obs-type map
# ---------------------------------------------------------------------------
# Keys   : DART obs type names
# Values : dicts describing how to extract the obs from an NNJA DataFrame
#
# NNJA column names confirmed from nnja-ai example notebooks:
#   ADPSFC:  OBS_TIMESTAMP, LAT, LON, SELV,
#            TMPSQ1.TMDB (dry-bulb temp, K),
#            WNDSQ1.WDIR (wind dir, °true), WNDSQ1.WSPD (wind speed, m/s)
#   ADPUPA:  OBS_TIMESTAMP, LAT, LON,
#            TMDB_PRLC{n}, WDIR_PRLC{n}, WSPD_PRLC{n}
#            where pressure_Pa = int(n) * 10
#            (e.g. TMDB_PRLC5000 → 500 hPa = 50 000 Pa)
# ---------------------------------------------------------------------------

DEFAULT_OBS_TYPE_MAP: dict[str, dict] = {
    # ------------------------------------------------------------------
    # Surface observations (ADPSFC METAR-type)
    # ------------------------------------------------------------------
    "METAR_TEMPERATURE_2_METER": {
        "nnja_dataset": "conv-adpsfc-NC000001",
        "kind": "scalar",
        "nnja_col": "TMPSQ1.TMDB",   # dry-bulb temperature (K)
        "vert_unit": "surface (m)",
        "vert_col": "SELV",           # station elevation above MSL (m)
        "default_err_var": 1.0,       # K²
    },
    "METAR_U_10_METER_WIND": {
        "nnja_dataset": "conv-adpsfc-NC000001",
        "kind": "U_wind",
        "wdir_col": "WNDSQ1.WDIR",   # wind direction (°true)
        "wspd_col": "WNDSQ1.WSPD",   # wind speed (m/s)
        "vert_unit": "height (m)",
        "vert_value": 10.0,           # 10 m AGL
        "default_err_var": 1.0,       # (m/s)²
    },
    "METAR_V_10_METER_WIND": {
        "nnja_dataset": "conv-adpsfc-NC000001",
        "kind": "V_wind",
        "wdir_col": "WNDSQ1.WDIR",
        "wspd_col": "WNDSQ1.WSPD",
        "vert_unit": "height (m)",
        "vert_value": 10.0,
        "default_err_var": 1.0,
    },
    # ------------------------------------------------------------------
    # Upper-air radiosonde (ADPUPA — mandatory pressure levels only)
    # ------------------------------------------------------------------
    "RADIOSONDE_TEMPERATURE": {
        "nnja_dataset": "conv-adpupa-NC002001",
        "kind": "radiosonde_scalar",
        "nnja_col_prefix": "TMDB",    # columns like TMDB_PRLC5000
        "vert_unit": "pressure (Pa)",
        "default_err_var": 1.0,       # K²
    },
    "RADIOSONDE_U_WIND_COMPONENT": {
        "nnja_dataset": "conv-adpupa-NC002001",
        "kind": "radiosonde_U_wind",
        "wdir_col_prefix": "WDIR",    # WDIR_PRLC{n}
        "wspd_col_prefix": "WSPD",    # WSPD_PRLC{n}
        "vert_unit": "pressure (Pa)",
        "default_err_var": 1.0,
    },
    "RADIOSONDE_V_WIND_COMPONENT": {
        "nnja_dataset": "conv-adpupa-NC002001",
        "kind": "radiosonde_V_wind",
        "wdir_col_prefix": "WDIR",
        "wspd_col_prefix": "WSPD",
        "vert_unit": "pressure (Pa)",
        "default_err_var": 1.0,
    },
}

# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def _to_dart_time(ts_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return ``(days, seconds)`` Series since the DART epoch (1601-01-01).

    ``days``    — integer days since 1601-01-01 00:00:00 UTC.
    ``seconds`` — integer seconds of day (0–86399).

    Uses Python datetime arithmetic to avoid the pandas nanosecond overflow
    that occurs when computing timedeltas from dates before ~1677.
    """
    if ts_series.dt.tz is not None:
        naive = ts_series.dt.tz_convert("UTC").dt.tz_localize(None)
    else:
        naive = ts_series

    deltas = [ts.to_pydatetime() - _DART_EPOCH_PY for ts in naive]
    days = pd.Series([d.days for d in deltas], index=naive.index)
    seconds = pd.Series([d.seconds for d in deltas], index=naive.index)
    return days, seconds


def _naive_utc(ts_series: pd.Series) -> pd.Series:
    """Strip timezone info (converting to UTC first if needed)."""
    if ts_series.dt.tz is not None:
        return ts_series.dt.tz_convert("UTC").dt.tz_localize(None)
    return ts_series


# ---------------------------------------------------------------------------
# Obs-row builders
# ---------------------------------------------------------------------------


def _make_dart_df(
    observation: pd.Series,
    lon: pd.Series,
    lat: pd.Series,
    vertical: pd.Series,
    vert_unit: str,
    obs_type: str,
    days: pd.Series,
    seconds: pd.Series,
    time: pd.Series,
    obs_err_var: float,
) -> pd.DataFrame:
    """Assemble a pydartdiags-compatible DataFrame for a single obs type."""
    n = len(observation)
    return pd.DataFrame(
        {
            "obs_num": 0,
            "observation": observation.to_numpy(dtype=float),
            "DART_quality_control": 0.0,
            "linked_list": "",
            "longitude": lon.to_numpy(dtype=float),
            "latitude": lat.to_numpy(dtype=float),
            "vertical": vertical.to_numpy(dtype=float),
            "vert_unit": vert_unit,
            "type": obs_type,
            "metadata": [[] for _ in range(n)],
            "external_FO": [[] for _ in range(n)],
            "seconds": seconds.to_numpy(dtype=int),
            "days": days.to_numpy(dtype=int),
            "time": time.to_numpy(),
            "obs_err_var": float(obs_err_var),
        }
    )


def _surface_scalar_to_dart(
    df: pd.DataFrame, entry: dict, obs_type: str
) -> pd.DataFrame:
    """Convert ADPSFC scalar-column obs to pydartdiags rows."""
    col = entry["nnja_col"]
    vert_col = entry.get("vert_col")

    needed = ["OBS_TIMESTAMP", "LAT", "LON", col]
    if vert_col and vert_col in df.columns:
        needed.append(vert_col)

    sub = df[needed].dropna(subset=["LAT", "LON", col])
    if sub.empty:
        return pd.DataFrame()

    if vert_col and vert_col in sub.columns:
        vertical = sub[vert_col].fillna(0.0)
    else:
        vertical = pd.Series(0.0, index=sub.index)

    days, secs = _to_dart_time(sub["OBS_TIMESTAMP"])
    return _make_dart_df(
        observation=sub[col],
        lon=sub["LON"],
        lat=sub["LAT"],
        vertical=vertical,
        vert_unit=entry["vert_unit"],
        obs_type=obs_type,
        days=days,
        seconds=secs,
        time=_naive_utc(sub["OBS_TIMESTAMP"]),
        obs_err_var=entry["default_err_var"],
    )


def _surface_wind_to_dart(
    df: pd.DataFrame, entry: dict, obs_type: str
) -> pd.DataFrame:
    """Convert ADPSFC wind (speed + direction) to U or V DART obs rows."""
    wdir_col = entry["wdir_col"]
    wspd_col = entry["wspd_col"]
    kind = entry["kind"]
    vert_value = float(entry["vert_value"])

    sub = df[["OBS_TIMESTAMP", "LAT", "LON", wdir_col, wspd_col]].dropna(
        subset=["LAT", "LON", wdir_col, wspd_col]
    )
    if sub.empty:
        return pd.DataFrame()

    wdir_rad = sub[wdir_col].to_numpy(dtype=float) * (math.pi / 180.0)
    wspd = sub[wspd_col].to_numpy(dtype=float)
    if kind == "U_wind":
        obs_vals = -wspd * np.sin(wdir_rad)
    else:
        obs_vals = -wspd * np.cos(wdir_rad)

    days, secs = _to_dart_time(sub["OBS_TIMESTAMP"])
    return _make_dart_df(
        observation=pd.Series(obs_vals, index=sub.index),
        lon=sub["LON"],
        lat=sub["LAT"],
        vertical=pd.Series(vert_value, index=sub.index),
        vert_unit=entry["vert_unit"],
        obs_type=obs_type,
        days=days,
        seconds=secs,
        time=_naive_utc(sub["OBS_TIMESTAMP"]),
        obs_err_var=entry["default_err_var"],
    )


def _radiosonde_scalar_to_dart(
    df: pd.DataFrame, entry: dict, obs_type: str
) -> pd.DataFrame:
    """Convert ADPUPA mandatory-level scalar obs to pydartdiags rows.

    Flat columns are named ``{prefix}_PRLC{suffix}`` where
    ``pressure_Pa = int(suffix) * 10``
    (e.g. ``TMDB_PRLC5000`` → 500 hPa = 50 000 Pa).
    """
    prefix = entry["nnja_col_prefix"]

    prlc_cols = {
        col: int(_PRLC_RE.match(col).group(2)) * 10
        for col in df.columns
        if _PRLC_RE.match(col) and _PRLC_RE.match(col).group(1) == prefix
    }
    if not prlc_cols:
        return pd.DataFrame()

    id_cols = ["OBS_TIMESTAMP", "LAT", "LON"]
    melted = (
        df[id_cols + list(prlc_cols)]
        .melt(
            id_vars=id_cols,
            value_vars=list(prlc_cols),
            var_name="prlc_col",
            value_name="observation",
        )
        .dropna(subset=["observation", "LAT", "LON"])
    )
    if melted.empty:
        return pd.DataFrame()

    melted = melted.reset_index(drop=True)
    melted["vertical"] = melted["prlc_col"].map(prlc_cols).astype(float)
    days, secs = _to_dart_time(melted["OBS_TIMESTAMP"])
    n = len(melted)
    return pd.DataFrame(
        {
            "obs_num": 0,
            "observation": melted["observation"].to_numpy(dtype=float),
            "DART_quality_control": 0.0,
            "linked_list": "",
            "longitude": melted["LON"].to_numpy(dtype=float),
            "latitude": melted["LAT"].to_numpy(dtype=float),
            "vertical": melted["vertical"].to_numpy(dtype=float),
            "vert_unit": entry["vert_unit"],
            "type": obs_type,
            "metadata": [[] for _ in range(n)],
            "external_FO": [[] for _ in range(n)],
            "seconds": secs.to_numpy(dtype=int),
            "days": days.to_numpy(dtype=int),
            "time": _naive_utc(melted["OBS_TIMESTAMP"]).to_numpy(),
            "obs_err_var": float(entry["default_err_var"]),
        }
    )


def _radiosonde_wind_to_dart(
    df: pd.DataFrame, entry: dict, obs_type: str
) -> pd.DataFrame:
    """Convert ADPUPA mandatory-level wind to U or V DART obs rows."""
    wdir_prefix = entry["wdir_col_prefix"]
    wspd_prefix = entry["wspd_col_prefix"]
    kind = entry["kind"]

    wdir_levels = {
        int(_PRLC_RE.match(c).group(2)) * 10: c
        for c in df.columns
        if _PRLC_RE.match(c) and _PRLC_RE.match(c).group(1) == wdir_prefix
    }
    wspd_levels = {
        int(_PRLC_RE.match(c).group(2)) * 10: c
        for c in df.columns
        if _PRLC_RE.match(c) and _PRLC_RE.match(c).group(1) == wspd_prefix
    }
    common_levels = sorted(set(wdir_levels) & set(wspd_levels))
    if not common_levels:
        return pd.DataFrame()

    id_cols = ["OBS_TIMESTAMP", "LAT", "LON"]
    pieces: list[pd.DataFrame] = []

    for pressure_pa in common_levels:
        wdir_col = wdir_levels[pressure_pa]
        wspd_col = wspd_levels[pressure_pa]
        chunk = df[id_cols + [wdir_col, wspd_col]].dropna(
            subset=["LAT", "LON", wdir_col, wspd_col]
        )
        if chunk.empty:
            continue
        chunk = chunk.reset_index(drop=True)

        wdir_rad = chunk[wdir_col].to_numpy(dtype=float) * (math.pi / 180.0)
        wspd = chunk[wspd_col].to_numpy(dtype=float)
        if kind == "radiosonde_U_wind":
            obs_vals = -wspd * np.sin(wdir_rad)
        else:
            obs_vals = -wspd * np.cos(wdir_rad)

        days, secs = _to_dart_time(chunk["OBS_TIMESTAMP"])
        n = len(chunk)
        pieces.append(
            pd.DataFrame(
                {
                    "obs_num": 0,
                    "observation": obs_vals,
                    "DART_quality_control": 0.0,
                    "linked_list": "",
                    "longitude": chunk["LON"].to_numpy(dtype=float),
                    "latitude": chunk["LAT"].to_numpy(dtype=float),
                    "vertical": float(pressure_pa),
                    "vert_unit": entry["vert_unit"],
                    "type": obs_type,
                    "metadata": [[] for _ in range(n)],
                    "external_FO": [[] for _ in range(n)],
                    "seconds": secs.to_numpy(dtype=int),
                    "days": days.to_numpy(dtype=int),
                    "time": _naive_utc(chunk["OBS_TIMESTAMP"]).to_numpy(),
                    "obs_err_var": float(entry["default_err_var"]),
                }
            )
        )

    if not pieces:
        return pd.DataFrame()
    return pd.concat(pieces, ignore_index=True)


# ---------------------------------------------------------------------------
# NNJASource
# ---------------------------------------------------------------------------


class NNJASource(DataSource):
    """Data source backed by the NNJA-AI (NOAA/NASA Joint Archive) cloud catalog.

    Uses the ``nnja-ai`` SDK to load ADPSFC (surface) and ADPUPA (upper-air)
    observations from GCS, then writes DART obs_seq files via
    ``pydartdiags.ObsSequence``.

    Parameters
    ----------
    catalog_mirror : str
        NNJA catalog GCS mirror.  ``"gcp_nodd"`` (default) uses the NOAA Open
        Data Dissemination mirror; ``"gcp_brightband"`` uses the Brightband
        mirror.
    obs_type_map : dict or None
        Additional obs-type entries merged on top of ``DEFAULT_OBS_TYPE_MAP``.
        Use this to override error variances, swap dataset IDs, or add custom
        obs types.  Pass ``None`` to use defaults only.

    Notes
    -----
    The catalog is opened lazily on first call to :meth:`write_obs_seq` so
    that :class:`NNJASource` instances can be safely pickled for
    ``ProcessPoolExecutor`` workers.

    Wind components are derived from NNJA speed + direction columns:

    .. code-block:: text

        U = −speed × sin(direction_rad)
        V = −speed × cos(direction_rad)

    Radiosonde pressure levels: ADPUPA flat columns are named
    ``{var}_PRLC{n}`` where ``pressure_Pa = int(n) × 10``
    (e.g. ``TMDB_PRLC5000`` → 500 hPa = 50 000 Pa).
    """

    def __init__(
        self,
        catalog_mirror: str = "gcp_nodd",
        obs_type_map: dict | None = None,
    ):
        self.catalog_mirror = catalog_mirror
        self._extra_obs_type_map = obs_type_map or {}
        self._catalog = None  # lazy — do NOT open here (pickle safety)

    def _get_catalog(self):
        from nnja_ai import DataCatalog  # noqa: PLC0415

        if self._catalog is None:
            self._catalog = DataCatalog(mirror=self.catalog_mirror)
        return self._catalog

    @staticmethod
    def _import_obs_sequence():
        from pydartdiags.obs_sequence.obs_sequence import ObsSequence  # noqa: PLC0415

        return ObsSequence

    def write_obs_seq(
        self,
        output_file: str,
        date0: datetime,
        date1: datetime,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        obs_types: list[str],
        obs_type_map: dict | None,
    ) -> bool:
        """Load NNJA obs for the given window, write a DART obs_seq file.

        Parameters
        ----------
        output_file : str
            Path to write the DART obs_seq file.
        date0, date1 : datetime
            Half-open time window ``[date0, date1)``.
        lat_min, lat_max, lon_min, lon_max : float
            Bounding box for spatial subsetting (degrees).
        obs_types : list[str]
            DART obs type names to generate (e.g. ``["METAR_TEMPERATURE_2_METER"]``).
        obs_type_map : dict or None
            Per-call overrides merged on top of the source-level map.

        Returns
        -------
        bool
            ``True`` if the file was written, ``False`` if no observations
            were found for this window.
        """
        # Build effective map: defaults → source-level extras → call-level overrides
        eff_map: dict[str, dict] = {
            **DEFAULT_OBS_TYPE_MAP,
            **self._extra_obs_type_map,
            **(obs_type_map or {}),
        }

        # Subset to requested obs types that exist in the map
        active = {ot: eff_map[ot] for ot in obs_types if ot in eff_map}
        if not active:
            return False

        # Group by NNJA dataset so we load each dataset at most once
        by_dataset: dict[str, list[tuple[str, dict]]] = {}
        for obs_type, entry in active.items():
            by_dataset.setdefault(entry["nnja_dataset"], []).append(
                (obs_type, entry)
            )

        catalog = self._get_catalog()
        from nnja_ai.exceptions import EmptyTimeSubsetError  # noqa: PLC0415

        # Ensure datetimes are UTC-aware so NNJA doesn't warn about naive datetimes
        if date0.tzinfo is None:
            date0 = date0.replace(tzinfo=timezone.utc)
        if date1.tzinfo is None:
            date1 = date1.replace(tzinfo=timezone.utc)

        dart_frames: list[pd.DataFrame] = []

        for ds_name, type_entries in by_dataset.items():
            try:
                nnja_sel = catalog[ds_name].sel(time=slice(date0, date1))
            except EmptyTimeSubsetError:
                print("No data in NNJA dataset", ds_name, "for window", date0, "to", date1)
                continue
            df = nnja_sel.load_dataset(backend="pandas")

            # Spatial filter (post-load; NNJA has no parquet pushdown for lat/lon)
            df = df[
                (df["LAT"] >= lat_min)
                & (df["LAT"] <= lat_max)
                & (df["LON"] >= lon_min)
                & (df["LON"] <= lon_max)
            ]
            if df.empty:
                continue

            for obs_type, entry in type_entries:
                kind = entry["kind"]
                if kind == "scalar":
                    obs_df = _surface_scalar_to_dart(df, entry, obs_type)
                elif kind in ("U_wind", "V_wind"):
                    obs_df = _surface_wind_to_dart(df, entry, obs_type)
                elif kind == "radiosonde_scalar":
                    obs_df = _radiosonde_scalar_to_dart(df, entry, obs_type)
                elif kind in ("radiosonde_U_wind", "radiosonde_V_wind"):
                    obs_df = _radiosonde_wind_to_dart(df, entry, obs_type)
                else:
                    continue

                if not obs_df.empty:
                    dart_frames.append(obs_df)

        if not dart_frames:
            return False

        combined = pd.concat(dart_frames, ignore_index=True)

        # Build and write obs_seq via pydartdiags
        ObsSequence = self._import_obs_sequence()
        obs_seq = ObsSequence(file=None)
        obs_seq.qc_copie_names = ["DART_quality_control"]
        obs_seq.df = combined
        obs_seq.write_obs_seq(output_file)
        return True
