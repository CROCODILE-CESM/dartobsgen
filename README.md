# dartobsgen

A pip-installable Python package that generates non-overlapping DART `obs_seq`
files from pluggable observation data sources.

**Documentation: <https://crocodile-cesm.github.io/dartobsgen/>**

## Install

```bash
cd /path/to/dartobsgen
pip install -e .
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
