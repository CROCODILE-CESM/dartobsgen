# Spatial masking

Trim any obs_seq file to observations inside a polygon using `trim_obs_seq`.
This works on any obs_seq file regardless of how it was produced.

```python
from dartobsgen import (
    trim_obs_seq,
    polygon_from_vertices,
    polygon_from_netcdf_vertices,
    polygon_from_netcdf_mask,
)
```

## Build a polygon from explicit vertices

```python
import numpy as np

lats = np.array([10.0, 10.0, 50.0, 50.0, 10.0])
lons = np.array([-90.0, -40.0, -40.0, -90.0, -90.0])
poly = polygon_from_vertices(lats, lons)
```

## Load a polygon from a NetCDF boundary file

```python
# NetCDF file with 1D arrays of boundary vertex coordinates
poly = polygon_from_netcdf_vertices(
    "domain_boundary.nc",
    lat_var="boundary_lat",
    lon_var="boundary_lon",
)
```

## Load a polygon from a 2D land/sea mask

```python
# NetCDF file with a 2D 0/1 mask variable (0=outside, 1=inside)
# lat_var and lon_var may be 1D (regular grid) or 2D (curvilinear)
poly = polygon_from_netcdf_mask(
    "ocean_mask.nc",
    mask_var="mask",
    lat_var="lat",
    lon_var="lon",
)
```

## Trim obs_seq files

```python
# Trim in place (overwrites the original file)
trim_obs_seq("obs_seq.2010-05-01-00000.out", poly)

# Write to a new file
trim_obs_seq("obs_seq.2010-05-01-00000.out", poly,
             output_file="obs_seq.2010-05-01-00000.trimmed.out")

# Trim all files produced by generate_obs_sequences
for path in written_files:
    trim_obs_seq(path, poly)
```

`trim_obs_seq` returns `True` if observations survived the trim and the
file was written, `False` if no observations fell inside the polygon.
A fast bounding-box pre-filter is applied before the exact polygon test.
