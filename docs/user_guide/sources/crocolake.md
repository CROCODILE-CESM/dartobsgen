# CrocoLake

{py:class}`~dartobsgen.CrocLakeSource` reads a local
[CrocoLake](https://github.com/boom-lab/crocolake-python) parquet database —
ocean in-situ observations from ARGO, GLODAP and SprayGliders — and writes the
observations in each window as a DART obs_seq file.

## Usage

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

if __name__ == "__main__":
    written = generate_obs_sequences(config, source)
    print(written)
```

`dart_path` is the root of a DART clone; it is used to resolve the DART obs
type definitions. `crocolake_path` is the directory holding the parquet
database.

Because observations exist independently of any model run here, `start` is the
natural way to specify the time range — pick the time you initialize the model
and the first analysis lands one `assimilation_frequency` later. See
[Time windows](../time_windows.md).

## Observation types

`obs_types` accepts DART compound names, DART variable names, or CrocoLake
variable names, freely mixed. The full table of supported types and the
`obs_type_map` override are in
[Observation types](../obs_types.md).

## Parallelism

Each worker process opens the parquet database independently and writes its own
output file, so windows parallelize cleanly — see
[Parallel generation](../parallel.md).
