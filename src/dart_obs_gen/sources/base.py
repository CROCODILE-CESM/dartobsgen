from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime


class DataSource(ABC):
    """Abstract base class for dart_obs_gen data sources.

    Subclass this to plug in a new observation data backend without
    changing any calling code.
    """

    @abstractmethod
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
        """Write observations for one assimilation window to a DART obs_seq file.

        Parameters
        ----------
        output_file : str
            Full path for the output obs_seq file.
        date0 : datetime
            Start of the half-open window [date0, date1).
        date1 : datetime
            End of the half-open window (exclusive).
        lat_min, lat_max : float
            Latitude bounds (degrees).
        lon_min, lon_max : float
            Longitude bounds (degrees, -180 to 180).
        obs_types : list[str]
            Requested observation types.
        obs_type_map : dict or None
            Custom obs type mapping (merged with source defaults); None
            means use source defaults only.

        Returns
        -------
        bool
            True if the file was written, False if no observations
            were found for this window.
        """


class ObsSeqSource(DataSource):
    """Stub: future data source backed by a bank of existing obs_seq files.

    Implement ``write_obs_seq`` to serve observations from pre-existing
    DART obs_seq files instead of a live database.  The interface is
    identical to any other ``DataSource``, so callers require no changes.

    Parameters
    ----------
    obs_seq_dir : str
        Directory containing the existing obs_seq files to draw from.
    """

    def __init__(self, obs_seq_dir: str):
        self.obs_seq_dir = obs_seq_dir

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
        raise NotImplementedError(
            "ObsSeqSource is a placeholder.  Implement write_obs_seq() "
            "to serve observations from existing obs_seq files."
        )
