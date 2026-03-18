# dart_obs_gen

A pip-installable Python package that generates non-overlapping DART `obs_seq` files from pluggable observation data sources.

## Install

```bash
cd /path/to/dart_obs_gen
pip install -e .
```

## Quick Start

```python
import datetime
from dart_obs_gen import ObsGenConfig, CrocLakeSource, generate_obs_sequences

config = ObsGenConfig(
    start=datetime.datetime(2010, 5, 1),
    end=datetime.datetime(2010, 5, 3),
    lat_min=5,   lat_max=60,
    lon_min=-100, lon_max=-30,
    obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
    assimilation_frequency_hours=6,
    output_dir="./obs_output",
)

source = CrocLakeSource(
    crocolake_path="/path/to/crocolake/",
    dart_path="/path/to/DART/",
)

written_files = generate_obs_sequences(config, source)
print(written_files)
```

## Package Structure

```
dart_obs_gen/
├── pyproject.toml
├── README.md
└── src/
    └── dart_obs_gen/
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

## Adding a new data source

Subclass `dart_obs_gen.DataSource` and implement `write_obs_seq()`:

```python
from dart_obs_gen import DataSource

class MySource(DataSource):
    def write_obs_seq(self, output_file, date0, date1,
                      lat_min, lat_max, lon_min, lon_max,
                      obs_types, obs_type_map) -> bool:
        # fetch data, write output_file, return True if written
        ...
```

`ObsSeqSource` in `dart_obs_gen.sources.base` is a pre-wired stub for
a future data source backed by a bank of existing obs_seq files.
