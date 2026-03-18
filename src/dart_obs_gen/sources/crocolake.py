from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import datetime

from .base import DataSource

# ---------------------------------------------------------------------------
# Default mapping: obs type name → CrocoLake variable + optional DB_NAME filter
# ---------------------------------------------------------------------------
# Keys may be:
#   • DART compound names  e.g. "ARGO_TEMPERATURE"
#   • DART variable names  e.g. "TEMPERATURE"  (db_name=None → all sources)
#   • CrocoLake var names  e.g. "TEMP"          (db_name=None → all sources)
# ---------------------------------------------------------------------------
DEFAULT_OBS_TYPE_MAP: dict[str, dict] = {
    # Compound DART names (source + variable)
    "ARGO_TEMPERATURE":        {"crocolake_var": "TEMP",           "db_name": "ARGO"},
    "ARGO_SALINITY":           {"crocolake_var": "PSAL",           "db_name": "ARGO"},
    "ARGO_OXYGEN":             {"crocolake_var": "DOXY",           "db_name": "ARGO"},
    "BOTTLE_TEMPERATURE":      {"crocolake_var": "TEMP",           "db_name": "GLODAP"},
    "BOTTLE_SALINITY":         {"crocolake_var": "PSAL",           "db_name": "GLODAP"},
    "BOTTLE_OXYGEN":           {"crocolake_var": "DOXY",           "db_name": "GLODAP"},
    "BOTTLE_ALKALINITY":       {"crocolake_var": "TOT_ALKALINITY", "db_name": "GLODAP"},
    "BOTTLE_INORGANIC_CARBON": {"crocolake_var": "TCO2",           "db_name": "GLODAP"},
    "BOTTLE_NITRATE":          {"crocolake_var": "NITRATE",        "db_name": "GLODAP"},
    "BOTTLE_SILICATE":         {"crocolake_var": "SILICATE",       "db_name": "GLODAP"},
    "BOTTLE_PHOSPHATE":        {"crocolake_var": "PHOSPHATE",      "db_name": "GLODAP"},
    "GLIDER_TEMPERATURE":      {"crocolake_var": "TEMP",           "db_name": "SprayGliders"},
    "GLIDER_SALINITY":         {"crocolake_var": "PSAL",           "db_name": "SprayGliders"},
    # Variable-only DART names (all sources)
    "TEMPERATURE":             {"crocolake_var": "TEMP",           "db_name": None},
    "SALINITY":                {"crocolake_var": "PSAL",           "db_name": None},
    "OXYGEN":                  {"crocolake_var": "DOXY",           "db_name": None},
    "ALKALINITY":              {"crocolake_var": "TOT_ALKALINITY", "db_name": None},
    "INORGANIC_CARBON":        {"crocolake_var": "TCO2",           "db_name": None},
    "NITRATE":                 {"crocolake_var": "NITRATE",        "db_name": None},
    "SILICATE":                {"crocolake_var": "SILICATE",       "db_name": None},
    "PHOSPHATE":               {"crocolake_var": "PHOSPHATE",      "db_name": None},
    # CrocoLake variable names used directly (all sources)
    "TEMP":                    {"crocolake_var": "TEMP",           "db_name": None},
    "PSAL":                    {"crocolake_var": "PSAL",           "db_name": None},
    "DOXY":                    {"crocolake_var": "DOXY",           "db_name": None},
    "TOT_ALKALINITY":          {"crocolake_var": "TOT_ALKALINITY", "db_name": None},
    "TCO2":                    {"crocolake_var": "TCO2",           "db_name": None},
}


class CrocLakeSource(DataSource):
    """Data source backed by a CrocoLake parquet database.

    Parameters
    ----------
    crocolake_path : str
        Path to the CrocoLake parquet database directory.
    dart_path : str
        Path to the DART repository root.  Used to locate
        ``observations/obs_converters/CrocoLake/convert_crocolake_obs.py``.
    loose : bool
        If True (default), write observations even when QC / error
        columns are missing or NaN.
    """

    _ALWAYS_SELECTED = [
        "DB_NAME", "JULD", "LATITUDE", "LONGITUDE",
        "PRES", "PRES_QC", "PRES_ERROR",
    ]

    def __init__(self, crocolake_path: str, dart_path: str, loose: bool = True):
        self.crocolake_path = crocolake_path
        self.dart_path = dart_path
        self.loose = loose

        converter_path = os.path.join(
            dart_path, "observations", "obs_converters", "CrocoLake"
        )
        if converter_path not in sys.path:
            sys.path.insert(0, converter_path)

    def _import_converter(self):
        from convert_crocolake_obs import ObsSequence  # noqa: PLC0415
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
        """Build filters from obs_types, query CrocoLake, write obs_seq file.

        Returns True if the file was written, False if no observations
        were found for this window.
        """
        effective_map = {**DEFAULT_OBS_TYPE_MAP, **(obs_type_map or {})}

        # Resolve obs_types → (crocolake_var, db_name) pairs
        resolved: list[tuple[str, str | None]] = []
        for obs_type in obs_types:
            if obs_type in effective_map:
                entry = effective_map[obs_type]
                resolved.append((entry["crocolake_var"], entry["db_name"]))
            else:
                # Treat as a raw CrocoLake variable name (all sources)
                resolved.append((obs_type, None))

        # Group db_names by crocolake_var; None means "all sources"
        var_db_names: dict[str, set] = defaultdict(set)
        for crocolake_var, db_name in resolved:
            var_db_names[crocolake_var].add(db_name)

        # Build selected_vars (always-present columns + one set per variable)
        selected_vars = list(self._ALWAYS_SELECTED)
        for var in var_db_names:
            for col in (var, var + "_QC", var + "_ERROR"):
                if col not in selected_vars:
                    selected_vars.append(col)

        # Base spatial + temporal filters (half-open window: JULD > date0, JULD < date1)
        base_filters = [
            ("LATITUDE",  ">", lat_min),  ("LATITUDE",  "<", lat_max),
            ("LONGITUDE", ">", lon_min),  ("LONGITUDE", "<", lon_max),
            ("PRES",      ">", -1e30),    ("PRES",      "<",  1e30),
            ("JULD",      ">", date0),    ("JULD",      "<",  date1),
        ]

        # One filter group per unique crocolake_var.
        # If all obs_types for a variable specify a particular source,
        # add a DB_NAME "in" filter for predicate pushdown.
        # If any obs_type uses db_name=None (all sources), no DB_NAME filter.
        db_filters = []
        for var, db_names in var_db_names.items():
            group = list(base_filters) + [(var, ">", -1e30), (var, "<", 1e30)]
            if None not in db_names:
                group.append(("DB_NAME", "in", list(db_names)))
            db_filters.append(group)

        ObsSequence = self._import_converter()
        obs_seq = ObsSequence(
            crocolake_path=self.crocolake_path,
            selected_vars=selected_vars,
            db_filters=db_filters,
            obs_seq_out=output_file,
            loose=self.loose,
        )
        obs_seq.write_obs_seq()
        return os.path.exists(output_file)
