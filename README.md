# dartobsgen

A pip-installable Python package that generates non-overlapping DART `obs_seq` files from pluggable observation data sources.

## Install

```bash
cd /path/to/dartobsgen
pip install -e .
```

## Quick Start

```python
import datetime
from dartobsgen import ObsGenConfig, CrocLakeSource, generate_obs_sequences

config = ObsGenConfig(
    start=datetime.datetime(2010, 5, 1),
    end=datetime.datetime(2010, 5, 3),
    lat_min=5,   lat_max=60,
    lon_min=-100, lon_max=-30,
    obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
    assimilation_frequency=datetime.timedelta(hours=6),
    output_dir="./obs_output",
)

source = CrocLakeSource(
    crocolake_path="/path/to/crocolake/",
    dart_path="/path/to/DART/",
)

# Sequential
written_files = generate_obs_sequences(config, source)

# Parallel (all CPUs)
written_files = generate_obs_sequences(config, source, max_workers=None)

# Parallel (fixed number of workers)
written_files = generate_obs_sequences(config, source, max_workers=4)

print(written_files)
```

## Package Structure

```
dartobsgen/
├── pyproject.toml
├── README.md
└── src/
    └── dartobsgen/
        ├── __init__.py           # Public API
        ├── config.py             # ObsGenConfig dataclass
        ├── generate.py           # generate_obs_sequences(), _make_windows()
        └── sources/
            ├── __init__.py
            ├── base.py           # DataSource ABC + ObsSeqSource stub
            └── crocolake.py      # CrocLakeSource + DEFAULT_OBS_TYPE_MAP
```

## Output file naming

Files are named `{output_prefix}.{timestamp}.out` where the timestamp
is formatted using `output_timestamp_format` (default: `"%Y-%m-%d-{S}"`).

The special token `{S}` is replaced with **seconds-of-day** (0–86400,
zero-padded to 5 digits), matching DART's standard obs_seq naming convention.
All other tokens follow Python `strftime` format.

| Window start       | Default filename                    |
|--------------------|-------------------------------------|
| 2010-05-01 00:00   | `obs_seq.2010-05-01-00000.out`      |
| 2010-05-01 06:00   | `obs_seq.2010-05-01-21600.out`      |
| 2010-05-01 12:00   | `obs_seq.2010-05-01-43200.out`      |
| 2010-05-01 18:00   | `obs_seq.2010-05-01-64800.out`      |

To use a custom format (e.g. DART's compact `YYYYMMDDHH`):

```python
config = ObsGenConfig(..., output_timestamp_format="%Y%m%d%H")
# produces: obs_seq.2010050100.out, obs_seq.2010050106.out, ...
```

## Observation types

`obs_types` accepts three styles — they can be freely mixed:

| Style | Example | Meaning |
|---|---|---|
| DART compound name | `"ARGO_TEMPERATURE"` | TEMP from ARGO only |
| DART variable name | `"TEMPERATURE"` | TEMP from all sources |
| CrocoLake var name | `"TEMP"` | TEMP from all sources |

### Supported obs types

| DART compound name          | CrocoLake var    | DB source     |
|-----------------------------|------------------|---------------|
| `ARGO_TEMPERATURE`          | `TEMP`           | ARGO          |
| `ARGO_SALINITY`             | `PSAL`           | ARGO          |
| `ARGO_OXYGEN`               | `DOXY`           | ARGO          |
| `BOTTLE_TEMPERATURE`        | `TEMP`           | GLODAP        |
| `BOTTLE_SALINITY`           | `PSAL`           | GLODAP        |
| `BOTTLE_OXYGEN`             | `DOXY`           | GLODAP        |
| `BOTTLE_ALKALINITY`         | `TOT_ALKALINITY` | GLODAP        |
| `BOTTLE_INORGANIC_CARBON`   | `TCO2`           | GLODAP        |
| `BOTTLE_NITRATE`            | `NITRATE`        | GLODAP        |
| `BOTTLE_SILICATE`           | `SILICATE`       | GLODAP        |
| `BOTTLE_PHOSPHATE`          | `PHOSPHATE`      | GLODAP        |
| `GLIDER_TEMPERATURE`        | `TEMP`           | SprayGliders  |
| `GLIDER_SALINITY`           | `PSAL`           | SprayGliders  |
| `TEMPERATURE`               | `TEMP`           | all           |
| `SALINITY`                  | `PSAL`           | all           |
| `OXYGEN`                    | `DOXY`           | all           |

Pass a custom `obs_type_map` dict to `ObsGenConfig` to override or extend:

```python
my_map = {
    "MY_CUSTOM_TEMP": {"crocolake_var": "TEMP", "db_name": "MyDB"},
}
config = ObsGenConfig(..., obs_type_map=my_map)
```

## Time windows

Windows are half-open: `[t0, t0 + freq)`.  Adjacent windows share no
observations.  The last window may extend beyond `end` to keep all
window widths uniform.

`assimilation_frequency` accepts any `datetime.timedelta`, so sub-hourly
windows are fully supported:

```python
import datetime
from dartobsgen import ObsGenConfig

# 6-hour windows (default)
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(hours=6))

# 30-minute windows
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(minutes=30))
```

## Parallel generation

`generate_obs_sequences` runs windows in parallel using
`concurrent.futures.ProcessPoolExecutor`.  Control parallelism with the
`max_workers` argument:

```python
# All available CPUs (default)
written = generate_obs_sequences(config, source)

# Fixed number of worker processes
written = generate_obs_sequences(config, source, max_workers=4)

# Sequential (useful for debugging)
written = generate_obs_sequences(config, source, max_workers=1)
```

Each worker process independently opens the CrocoLake parquet database
and writes its own output file, so there are no shared-state conflicts.

**Note:** scripts that call `generate_obs_sequences` with `max_workers != 1`
must be run under a `if __name__ == "__main__":` guard (standard Python
multiprocessing requirement on macOS / Windows).

## Spatial masking

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

### Build a polygon from explicit vertices

```python
import numpy as np

lats = np.array([10.0, 10.0, 50.0, 50.0, 10.0])
lons = np.array([-90.0, -40.0, -40.0, -90.0, -90.0])
poly = polygon_from_vertices(lats, lons)
```

### Load a polygon from a NetCDF boundary file

```python
# NetCDF file with 1D arrays of boundary vertex coordinates
poly = polygon_from_netcdf_vertices(
    "domain_boundary.nc",
    lat_var="boundary_lat",
    lon_var="boundary_lon",
)
```

### Load a polygon from a 2D land/sea mask

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

### Trim obs_seq files

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

---

## Adding a new data source

Subclass `dartobsgen.DataSource` and implement `write_obs_seq()`:

```python
from dartobsgen import DataSource

class MySource(DataSource):
    def write_obs_seq(self, output_file, date0, date1,
                      lat_min, lat_max, lon_min, lon_max,
                      obs_types, obs_type_map) -> bool:
        # fetch data, write output_file, return True if written
        ...
```

`ObsSeqSource` in `dartobsgen.sources.base` is a pre-wired stub for
a future data source backed by a bank of existing obs_seq files.
