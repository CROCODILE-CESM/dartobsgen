# dartobsgen

A pip-installable Python package that generates non-overlapping DART `obs_seq`
files from pluggable observation data sources.

**Documentation: <https://crocodile-cesm.github.io/dartobsgen/>**

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

## Documentation

| Topic | |
|---|---|
| Time windows — `start` vs `first_analysis`, window bounds | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/time_windows.html) |
| Output file naming — the `{S}` seconds-of-day token | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/output_naming.html) |
| Observation types — naming styles and supported types | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/obs_types.html) |
| Parallel generation | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/parallel.html) |
| Spatial masking with `trim_obs_seq` | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/spatial_masking.html) |
| Data sources — CrocoLake, NNJA-AI, `perfect_model_obs` | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/sources/index.html) |
| Adding a new data source | [docs](https://crocodile-cesm.github.io/dartobsgen/user_guide/sources/extending.html) |
| API reference | [docs](https://crocodile-cesm.github.io/dartobsgen/api/index.html) |

## Package Structure

```
dartobsgen/
├── pyproject.toml
├── README.md
├── docs/                         # Sphinx documentation source
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

## Building the documentation

```bash
pip install -e ".[docs]"
sphinx-build -b html docs docs/_build/html
open docs/_build/html/index.html
```
