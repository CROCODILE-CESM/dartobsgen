# dartobsgen

A pip-installable Python package that generates non-overlapping DART `obs_seq`
files from pluggable observation data sources.

Specify what type of observations you want, for what time period, for what region on
the Earth, and for a given assimilation frequency. Select a source for the 
observations, e.g. a database of real observations (CrocoLake), a cloud archive 
(NNJA-AI), or synthetic observations generated from a model state (model run + DART 
perfect_model_obs). Then generate the observation sequences. 

One `obs_seq` file is written per assimilation cycle, named for its analysis
time which contains observations within the DART assimilation window. 

::::{grid} 1 1 2 2
:gutter: 4

:::{grid-item-card} {octicon}`rocket;1.5em;sd-mr-1` Quickstart
:link: quickstart
:link-type: doc

Generate your first set of `obs_seq` files.
:::

:::{grid-item-card} {octicon}`book;1.5em;sd-mr-1` User guide
:link: user_guide/index
:link-type: doc

Time windows, file naming, observation types, spatial masking, parallelism.
:::

:::{grid-item-card} {octicon}`database;1.5em;sd-mr-1` Data sources
:link: user_guide/sources/index
:link-type: doc

CrocoLake, NNJA-AI, and synthetic obs via `perfect_model_obs`.
:::

:::{grid-item-card} {octicon}`code;1.5em;sd-mr-1` API reference
:link: api/index
:link-type: doc

Every public class and function.
:::

::::

## Package structure

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

```{toctree}
:maxdepth: 2
:hidden:

installation
quickstart
user_guide/index
api/index
```
