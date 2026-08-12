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
    start=datetime.datetime(2010, 5, 1),   # model run start; first analysis is 06Z
                                           # (or say it directly: first_analysis=...)
    end=datetime.datetime(2010, 5, 3),     # last analysis time (inclusive)
    lat_min=5,   lat_max=60,
    lon_min=-100, lon_max=-30,
    obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
    assimilation_frequency=datetime.timedelta(hours=6), # assimilate every 6 hours
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
        ├── generate.py           # generate_obs_sequences(), _make_analysis_windows()
        ├── model_state.py        # ModelStateProvider ABC + MOM6StateProvider
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
is formatted using `output_timestamp_format` (default: `"%Y-%m-%d-{S}"`)
and applied to the **analysis time** — the centre of the window, which is
also the model's stopping time. This a DART convention, `filter` assimilates 
the observations in a window centered on the analysis time, so the observation
sequence file is named `obs_seq.$analysis_time`.

The special token `{S}` is replaced with **seconds-of-day** (0–86400,
zero-padded to 5 digits), following DART-CESM typical obs_seq naming convention.
All other tokens follow Python `strftime` format.

| Analysis time      | Window                          | Default filename               |
|--------------------|---------------------------------|--------------------------------|
| 2010-05-01 06:00   | (03:00, 09:00]                  | `obs_seq.2010-05-01-21600.out` |
| 2010-05-01 12:00   | (09:00, 15:00]                  | `obs_seq.2010-05-01-43200.out` |
| 2010-05-01 18:00   | (15:00, 21:00]                  | `obs_seq.2010-05-01-64800.out` |
| 2010-05-02 00:00   | (05-01 21:00, 05-02 03:00]      | `obs_seq.2010-05-02-00000.out` |

To use a custom format (e.g. DART's compact `YYYYMMDDHH`):

```python
config = ObsGenConfig(..., output_timestamp_format="%Y%m%d%H")
# produces: obs_seq.2010050100.out, obs_seq.2010050106.out, ...
```

## Observation types

`obs_types` for CrocoLake as a source accepts three styles — they can be freely mixed:

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

Analysis times run from `first_analysis` through `end` inclusive, spaced by
`assimilation_frequency`. Give **exactly one** of `start` or `first_analysis`;
the other is derived, and both are populated on the config afterwards.

| Field | Meaning | Use it when |
|---|---|---|
| `start` | Start of the **model run**, not an analysis time. `first_analysis = start + freq`. | Observations exist independently of the model (`CrocLakeSource`, `NNJASource`) — pick the time you initialize the model. |
| `first_analysis` | The first analysis time itself. | The analysis times must land on times fixed by something else — in particular `PerfectModelSource`, where obs are placed at the valid times of the model states. |

`start` exists because in CESM+DART cycling the model advances one
assimilation period before the first assimilation. That implicit `+ freq` is
easy to trip over, so state it directly with `first_analysis` whenever you
know the analysis times you want:

```python
# These two are identical
ObsGenConfig(start=datetime.datetime(2010, 5, 1), ...)             # first analysis 06Z
ObsGenConfig(first_analysis=datetime.datetime(2010, 5, 1, 6), ...) # same thing, said directly
```

Passing neither, passing both, or configuring a run with no analysis times at
all (`first_analysis > end`) raises `ValueError` at construction.

All `ObsGenConfig` fields are keyword-only.

Each analysis time `T` gets the window `(T - freq/2, T + freq/2]` — open
below, closed above. This matches DART's documented convention (see
`assimilation_code/programs/obs_sequence_tool/obs_sequence_tool.rst`):
*"the windows should be centered around the assimilation time starting at
minus 1/2 the window time plus 1 second, and ending at exactly plus 1/2 the
window time."*

Windows are contiguous and non-overlapping. Adjacent windows share a boundary
instant, and the closed upper bound assigns it to the earlier window, so no
observation is ever written to two files. Note that observations before
`first_analysis - freq/2` are deliberately unused — they belong to an earlier
analysis, which happened before this run began.

`assimilation_frequency` accepts any `datetime.timedelta` that is an even
whole number of seconds (so that the half-width `freq/2` lands on an integer
second, since DART times are integer day/second pairs). Sub-hourly windows are
supported:

```python
import datetime
from dartobsgen import ObsGenConfig

# 6-hour windows (default): analyses at 06Z, 12Z, 18Z, 00Z
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(hours=6))

# 30-minute windows
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(minutes=30))
```

To land on conventional analysis times, either name the first one directly
with `first_analysis=datetime.datetime(2010, 5, 1, 6)` (06Z), or set `start`
one frequency before it: `start=datetime.datetime(2010, 5, 1)` (00Z).

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
    def write_obs_seq(self, output_file, analysis_time, date0, date1,
                      lat_min, lat_max, lon_min, lon_max,
                      obs_types, obs_type_map) -> bool:
        # fetch data, write output_file, return True if written
        ...
```

`analysis_time` is the cycle time `T` the window is centered on, and the time
`output_file` is named for. `date0`/`date1` bound the window `(date0, date1]`
— exclusive below, inclusive above. Sources that read timestamps from a
database (`CrocLakeSource`, `NNJASource`) only need the bounds; sources that
*place* observations themselves (`PerfectModelSource`) should position them
relative to `analysis_time`, since `date0` itself is outside the window.

`ObsSeqSource` in `dartobsgen.sources.base` is a pre-wired stub for
a future data source backed by a bank of existing obs_seq files.

### Optional: `check_coverage`

`DataSource.check_coverage(windows)` is called once before any window runs,
with every `(analysis_time, date0, date1)` triple of the run. The default is a
no-op, which is right for sources backed by a continuous archive — any window
is as good as any other.

Override it when your data lives at a fixed set of discrete times, so a run
whose windows miss those times fails with a diagnosis instead of writing zero
files and reporting success. `PerfectModelSource` does exactly this.

```python
class MySource(DataSource):
    def check_coverage(self, windows) -> None:
        if nothing_I_have_falls_in(windows):
            raise ValueError("... and here is the setting that would fix it")
```


---

## Under development: NNJA data source

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

## Under development:Synthetic observations via `perfect_model_obs`

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

### `ObsNetworkEntry` fields

| Field | Type | Description |
|---|---|---|
| `obs_type` | `str` | DART obs type name, e.g. `"RAW_STATE_VARIABLE"`, `"TEMPERATURE"` |
| `lat` | `float` | Latitude in degrees |
| `lon` | `float` | Longitude in degrees (-180 to 180) |
| `vertical` | `float` | Vertical coordinate value |
| `vert_unit` | `str` | `"pressure (Pa)"`, `"height (m)"`, `"model level"`, `"surface (m)"` |
| `obs_err_var` | `float` | Observation error variance |
| `time_offset` | `timedelta` | Offset from the analysis time (default `timedelta(0)`) |

### Driving it from model output: `state_provider`

`perfect_model_obs` cannot advance a large model, so it interpolates from a
state file you hand it. `MOM6StateProvider` indexes the timeslices of a MOM6
run's output and serves one per window, and observations are then placed at
**the valid time of that state**, not at the analysis time.

That inverts the usual configuration: the analysis times must line up with the
model output times. Use `first_analysis`, not `start`:

```python
config = ObsGenConfig(
    first_analysis=datetime.datetime(2015, 10, 4, 12),  # first model output time
    end=datetime.datetime(2015, 10, 8, 12),             # last analysis time
    assimilation_frequency=datetime.timedelta(hours=24),
    ...
)
```

Using `start` here would place the first analysis one frequency *after* the
first model output time, and every state could fall outside every window.

A window selects the earliest state in `(date0, date1]` — open below, closed
above, matching the window `perfect_model_obs` is given (`first_obs = date0 +
1s`, `last_obs = date1`). A state landing exactly on `date0` belongs to the
previous window; selecting it would place every observation one second before
`first_obs` and DART would abort with *"All obs in sequence are before
first_obs_days:first_obs_seconds"*.

`PerfectModelSource.check_coverage` reports the alignment before any window
runs:

```
Model state coverage: 1 of 5 window(s) have a state; 1 model output time(s)
(1 used, 0 shadowed, 0 outside all windows).
```

and raises when nothing lines up, naming the setting that would fix it:

```
No model state falls in any assimilation window, so no observations can be generated.
  model output times: 2015-10-04T12:00:00
  analysis times:     2015-10-06T00:00:00, 2015-10-07T00:00:00, 2015-10-08T00:00:00
  windows cover:      (2015-10-05T12:00:00, 2015-10-08T12:00:00] every 1 day, 0:00:00
  Observations are placed at the model state's valid time, so the analysis
  times must line up with the model output times.  Set
  first_analysis=2015-10-04T12:00:00 and end>=2015-10-04T12:00:00 to cover
  the model output.
```

It also flags *shadowed* states (a second state in a window that already has
an earlier one — only the earliest is used) and *off-centre* states (in a
window but not at its analysis time, so obs land away from the window centre).

### Controlling observation time within a window

With a `state_provider`, observations are placed at the state's valid time.
Without one, they are placed at the **analysis time** — the centre of the
window. Either way, `time_offset` shifts individual entries relative to that
reference; it must stay within `(-freq/2, +freq/2]` or `perfect_model_obs`
will reject the observation as outside the window.

```python
import datetime

# Obs at the analysis time / window centre (default)
entry_centre = ObsNetworkEntry(..., time_offset=datetime.timedelta(0))

# Obs 1 hour after the analysis time
entry_late = ObsNetworkEntry(..., time_offset=datetime.timedelta(hours=1))
```

### MOM6 example

`mom6_perfect_model.py` shows a complete ocean example: synthetic temperature
and salinity profiles on a sparse lat/lon grid at six depths, drawn from a
MOM6 history file via `MOM6StateProvider` with daily assimilation windows
anchored on the model output times.


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

