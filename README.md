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
        ├── spatial.py            # trim_obs_seq(), polygon helpers
        └── sources/
            ├── __init__.py
            ├── base.py           # DataSource ABC + ObsSeqSource stub
            ├── crocolake.py      # CrocLakeSource + DEFAULT_OBS_TYPE_MAP
            ├── nnja.py           # NNJASource
            └── perfect_model.py  # PerfectModelSource + ObsNetworkEntry
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

## NNJA data source

`NNJASource` accesses the [NNJA-AI](https://github.com/brightbandtech/nnja-ai)
cloud-hosted observation archive (NOAA/NASA Joint Archive) stored on GCS.

### Install the extra dependency

```bash
pip install nnja-ai
```

### Usage

```python
import datetime
from dartobsgen import ObsGenConfig, NNJASource, generate_obs_sequences

config = ObsGenConfig(
    start=datetime.datetime(2021, 1, 1),
    end=datetime.datetime(2021, 1, 2),
    lat_min=-90, lat_max=90,
    lon_min=-180, lon_max=180,
    obs_types=[
        "METAR_TEMPERATURE_2_METER",
        "METAR_U_10_METER_WIND",
        "METAR_V_10_METER_WIND",
    ],
    assimilation_frequency=datetime.timedelta(hours=6),
    output_dir="./obs_output",
)

source = NNJASource(catalog_mirror="gcp_nodd")

if __name__ == "__main__":
    written = generate_obs_sequences(config, source)
    print(written)
```

### Supported NNJA obs types

| DART obs type | NNJA dataset | Variable |
|---|---|---|
| `METAR_TEMPERATURE_2_METER` | `conv-adpsfc-NC000001` | `TMPSQ1.TMDB` (K) |
| `METAR_U_10_METER_WIND` | `conv-adpsfc-NC000001` | derived from `WNDSQ1.WSPD` + `WNDSQ1.WDIR` |
| `METAR_V_10_METER_WIND` | `conv-adpsfc-NC000001` | derived from `WNDSQ1.WSPD` + `WNDSQ1.WDIR` |
| `RADIOSONDE_TEMPERATURE` | `conv-adpupa-NC002001` | `TMDB_PRLC{n}` mandatory levels |
| `RADIOSONDE_U_WIND_COMPONENT` | `conv-adpupa-NC002001` | derived from `WDIR_PRLC{n}` + `WSPD_PRLC{n}` |
| `RADIOSONDE_V_WIND_COMPONENT` | `conv-adpupa-NC002001` | derived from `WDIR_PRLC{n}` + `WSPD_PRLC{n}` |

Radiosonde pressure suffix `n`: column `TMDB_PRLC5000` → 500 hPa = 50 000 Pa
(`pressure_Pa = int(n) × 10`).

### Custom error variances

Override the default error variances (1.0 for all obs types) by passing a
custom `obs_type_map` to `NNJASource` or to `ObsGenConfig`:

```python
source = NNJASource(
    catalog_mirror="gcp_nodd",
    obs_type_map={
        "METAR_TEMPERATURE_2_METER": {
            "nnja_dataset": "conv-adpsfc-NC000001",
            "kind": "scalar",
            "nnja_col": "TMPSQ1.TMDB",
            "vert_unit": "surface (m)",
            "vert_col": "SELV",
            "default_err_var": 4.0,   # 2 K std dev → 4 K² variance
        },
    },
)
```

### GCS mirror options

| Mirror | Description |
|---|---|
| `"gcp_nodd"` | NOAA Open Data Dissemination (default, open access) |
| `"gcp_brightband"` | Brightband mirror |

---

## Synthetic observations via `perfect_model_obs`

`PerfectModelSource` generates synthetic observations by running DART's
`perfect_model_obs` executable.  For each assimilation window it:

1. Builds a template `obs_seq.in` from a user-defined observing network.
2. Patches `input.nml` with the obs_seq filenames and window time bounds.
3. Runs `perfect_model_obs` in an isolated per-window directory.
4. Returns the resulting `obs_seq.out` as the window's obs file.

The caller uses it identically to `CrocLakeSource` or `NNJASource` — only
the source object changes.

### Prerequisites

`dart_work_dir` must contain:

- The compiled `perfect_model_obs` executable
- A base `input.nml` with a `perfect_model_obs_nml` block (the source
  patches only the obs_seq filenames and time-bound fields)
- Any initial-conditions files referenced by `input.nml`

This is the same directory structure used to run `perfect_model_obs` by hand.

### Usage

```python
import datetime
import numpy as np
from dartobsgen import ObsGenConfig, ObsNetworkEntry, PerfectModelSource, generate_obs_sequences

# Define the synthetic observing network.
# Each entry is one observation location + type.
lons = np.linspace(-180.0, 180.0, 40, endpoint=False)
network = [
    ObsNetworkEntry(
        obs_type="RAW_STATE_VARIABLE",
        lat=0.0,
        lon=float(lon),
        vertical=1.0,
        vert_unit="level",
        obs_err_var=1.0,
    )
    for lon in lons
]

config = ObsGenConfig(
    start=datetime.datetime(2000, 1, 1),
    end=datetime.datetime(2000, 1, 2),
    lat_min=-90, lat_max=90,
    lon_min=-180, lon_max=180,
    obs_types=["RAW_STATE_VARIABLE"],
    assimilation_frequency=datetime.timedelta(hours=6),
    output_dir="./obs_output",
)

source = PerfectModelSource(
    dart_work_dir="/path/to/DART/models/MOM6/work",
    obs_network=network,
)

if __name__ == "__main__":
    written = generate_obs_sequences(config, source, max_workers=1)
    print(written)
```

### `ObsNetworkEntry` fields

| Field | Type | Description |
|---|---|---|
| `obs_type` | `str` | DART obs type name, e.g. `"RAW_STATE_VARIABLE"`, `"TEMPERATURE"` |
| `lat` | `float` | Latitude in degrees |
| `lon` | `float` | Longitude in degrees (-180 to 180) |
| `vertical` | `float` | Vertical coordinate value |
| `vert_unit` | `str` | `"pressure (Pa)"`, `"height (m)"`, `"depth (m)"`, `"level"`, etc. |
| `obs_err_var` | `float` | Observation error variance |
| `time_offset` | `timedelta` | Offset from window `date0` (default `timedelta(0)`) |

### Controlling observation time within a window

By default all observations are placed at the window start (`date0`).
Use `time_offset` to shift individual entries:

```python
import datetime

# Obs at window start
entry_start = ObsNetworkEntry(..., time_offset=datetime.timedelta(0))

# Obs at 3 hours into a 6-hour window (window centre)
entry_centre = ObsNetworkEntry(..., time_offset=datetime.timedelta(hours=3))
```

### Parallel execution

Each window runs in its own subdirectory (`dart_work_dir/windows/{timestamp}/`)
with symlinks back to the shared executable and initial-conditions files.
This avoids file conflicts when `ProcessPoolExecutor` runs multiple windows
simultaneously.

Use `max_workers=1` when debugging, or when `perfect_model_obs` itself
advances the model state (since state advancement must be sequential):

```python
written = generate_obs_sequences(config, source, max_workers=1)
```

### Model state note

`perfect_model_obs` interpolates observations from the initial-conditions
file specified in `input.nml`.  `PerfectModelSource` uses a single fixed
initial-conditions file for the whole run, which is appropriate for
Lorenz-type toy models or a frozen-truth scenario.  Time-advancing the model
state between windows requires running `perfect_model_obs` sequentially and
updating `input.nml` to point to the advanced state — this is outside the
scope of this source.

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
