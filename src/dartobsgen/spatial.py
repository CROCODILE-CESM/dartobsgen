from __future__ import annotations

import numpy as np
import shapely
from shapely.geometry import Polygon


# ---------------------------------------------------------------------------
# Polygon construction helpers
# ---------------------------------------------------------------------------

def polygon_from_vertices(lat_arr, lon_arr) -> Polygon:
    """Build a shapely Polygon from 1D arrays of boundary vertices.

    Parameters
    ----------
    lat_arr : array-like
        Latitude of each boundary vertex (degrees).
    lon_arr : array-like
        Longitude of each boundary vertex (degrees, -180 to 180).

    Returns
    -------
    shapely.geometry.Polygon
    """
    return Polygon(zip(np.asarray(lon_arr), np.asarray(lat_arr)))


def polygon_from_netcdf_vertices(
    nc_file: str,
    lat_var: str,
    lon_var: str,
) -> Polygon:
    """Load boundary vertices from a NetCDF file and return a shapely Polygon.

    Parameters
    ----------
    nc_file : str
        Path to the NetCDF file.
    lat_var : str
        Name of the variable containing vertex latitudes.
    lon_var : str
        Name of the variable containing vertex longitudes.

    Returns
    -------
    shapely.geometry.Polygon
    """
    import xarray as xr  # noqa: PLC0415

    with xr.open_dataset(nc_file) as ds:
        lat = ds[lat_var].values.ravel()
        lon = ds[lon_var].values.ravel()
    return polygon_from_vertices(lat, lon)


def polygon_from_netcdf_mask(
    nc_file: str,
    mask_var: str,
    lat_var: str,
    lon_var: str,
) -> Polygon:
    """Extract the boundary of a 2D 0/1 mask and return a shapely Polygon.

    Contours the mask at level 0.5 using ``skimage.measure.find_contours``.
    If multiple contours are found the one with the most vertices is used
    (i.e. the largest boundary).

    Parameters
    ----------
    nc_file : str
        Path to the NetCDF file.
    mask_var : str
        Name of the 2D mask variable (0 = outside, 1 = inside).
    lat_var : str
        Name of the latitude coordinate variable (1D or 2D).
    lon_var : str
        Name of the longitude coordinate variable (1D or 2D).

    Returns
    -------
    shapely.geometry.Polygon

    Raises
    ------
    ValueError
        If no contour is found in the mask.
    """
    import xarray as xr  # noqa: PLC0415
    from skimage.measure import find_contours  # noqa: PLC0415

    with xr.open_dataset(nc_file) as ds:
        mask = ds[mask_var].values.squeeze().astype(float)
        lat = ds[lat_var].values
        lon = ds[lon_var].values

    contours = find_contours(mask, level=0.5)
    if not contours:
        raise ValueError(
            f"No contour found in '{mask_var}' at level 0.5. "
            "Check that the variable contains both 0 and 1 values."
        )

    contour = max(contours, key=len)
    lon_c, lat_c = _contour_to_lonlat(contour, lat, lon)
    return Polygon(zip(lon_c, lat_c))


def _contour_to_lonlat(
    contour: np.ndarray,
    lat_arr: np.ndarray,
    lon_arr: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert a skimage contour (float row/col indices) to lon/lat arrays.

    Supports both 1D regular-grid and 2D curvilinear coordinate arrays.
    """
    row = contour[:, 0]
    col = contour[:, 1]

    if lat_arr.ndim == 1 and lon_arr.ndim == 1:
        lat_c = np.interp(row, np.arange(len(lat_arr)), lat_arr)
        lon_c = np.interp(col, np.arange(len(lon_arr)), lon_arr)
    else:
        from scipy.ndimage import map_coordinates  # noqa: PLC0415

        lat_c = map_coordinates(lat_arr, [row, col], order=1)
        lon_c = map_coordinates(lon_arr, [row, col], order=1)

    return lon_c, lat_c


# ---------------------------------------------------------------------------
# Trimming
# ---------------------------------------------------------------------------

def trim_obs_seq(
    input_file: str,
    polygon: Polygon,
    output_file: str | None = None,
) -> bool:
    """Trim observations in a DART obs_seq file to those inside a polygon.

    Uses ``pydartdiags.ObsSequence`` to read and write the file, so this
    function is data-source agnostic — it works on any obs_seq file
    regardless of how it was produced.

    A fast bounding-box pre-filter is applied first; only points surviving
    the bbox check are tested with the exact ``shapely.contains_xy`` call.

    Parameters
    ----------
    input_file : str
        Path to the input DART obs_seq file.
    polygon : shapely.geometry.Polygon
        Boundary polygon in (longitude, latitude) coordinates (degrees).
        Build one with :func:`polygon_from_vertices`,
        :func:`polygon_from_netcdf_vertices`, or
        :func:`polygon_from_netcdf_mask`.
    output_file : str or None
        Destination path for the trimmed file.  ``None`` (default)
        overwrites ``input_file`` in place.

    Returns
    -------
    bool
        True if at least one observation survived the trim and the output
        file was written; False if no observations fell inside the polygon.
    """
    from pydartdiags.obs_sequence.obs_sequence import ObsSequence  # noqa: PLC0415

    obs_seq = ObsSequence(input_file)
    if obs_seq.df.empty:
        return False

    # Fast bounding-box pre-filter
    lon_min, lat_min, lon_max, lat_max = polygon.bounds
    df = obs_seq.df
    bbox_mask = (
        (df["longitude"] >= lon_min) & (df["longitude"] <= lon_max)
        & (df["latitude"] >= lat_min) & (df["latitude"] <= lat_max)
    )
    df_candidate = df.loc[bbox_mask].reset_index(drop=True)
    if df_candidate.empty:
        return False

    # Exact polygon containment (vectorised, shapely >= 2.0)
    inside = shapely.contains_xy(
        polygon,
        df_candidate["longitude"].to_numpy(),
        df_candidate["latitude"].to_numpy(),
    )
    obs_seq.df = df_candidate[inside].reset_index(drop=True)
    if obs_seq.df.empty:
        return False

    obs_seq.write_obs_seq(output_file if output_file is not None else input_file)
    return True
