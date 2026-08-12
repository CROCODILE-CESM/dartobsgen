from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime


class DataSource(ABC):
    """Abstract base class for dartobsgen data sources.

    Subclass this to plug in a new observation data backend without
    changing any calling code.
    """

    @abstractmethod
    def write_obs_seq(
        self,
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
    ) -> bool:
        """Write observations for one assimilation window to a DART obs_seq file.

        Parameters
        ----------
        output_file : str
            Full path for the output obs_seq file.
        analysis_time : datetime
            The assimilation time T that this window is centered on — the
            model's stopping time, and the time the output file is named
            for.  Sources that place observations themselves (rather than
            reading timestamps from a database) should use this as the
            reference time.
        date0 : datetime
            Lower bound of the window (T - freq/2), **exclusive**.
        date1 : datetime
            Upper bound of the window (T + freq/2), **inclusive**.
            The window is (date0, date1], per DART's convention.
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

    def check_coverage(
        self, windows: list[tuple[datetime, datetime, datetime]]
    ) -> None:
        """Pre-flight check of *windows* against the data this source can serve.

        Called once by ``generate_obs_sequences`` before any window runs,
        with every ``(analysis_time, date0, date1)`` triple of the run.  The
        default is a no-op: sources backed by a continuous observation
        archive (``CrocLakeSource``, ``NNJASource``) have nothing to check,
        since any window is as good as any other.

        Override it in sources whose data lives at a fixed set of discrete
        times — ``PerfectModelSource``, where observations can only be
        generated at the valid times of the available model states.  Such a
        source should print a short coverage summary, and raise
        ``ValueError`` when no window can produce anything, so a
        misconfigured run fails immediately with a diagnosis instead of
        writing zero files without explanation.

        Parameters
        ----------
        windows : list of (datetime, datetime, datetime)
            Every ``(analysis_time, date0, date1)`` of the run, in
            chronological order.  Windows are contiguous, so they span
            ``(windows[0][1], windows[-1][2]]``.

        Raises
        ------
        ValueError
            If the source determines that no window can yield observations.
        """
        return None


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
        analysis_time: datetime,
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
