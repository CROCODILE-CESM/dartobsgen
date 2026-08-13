# API reference

Everything below is importable directly from `dartobsgen`.

## Configuration and driver

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   dartobsgen.ObsGenConfig
   dartobsgen.generate_obs_sequences
```

## Data sources

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   dartobsgen.DataSource
   dartobsgen.CrocLakeSource
   dartobsgen.NNJASource
   dartobsgen.PerfectModelSource
   dartobsgen.ObsNetworkEntry
   dartobsgen.ObsSeqSource
```

## Model states

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   dartobsgen.ModelState
   dartobsgen.ModelStateProvider
   dartobsgen.MOM6StateProvider
   dartobsgen.mom6_time_to_datetime
   dartobsgen.state_vars_from_nml
```

## Spatial masking

```{eval-rst}
.. autosummary::
   :toctree: generated
   :nosignatures:

   dartobsgen.polygon_from_vertices
   dartobsgen.polygon_from_netcdf_vertices
   dartobsgen.polygon_from_netcdf_mask
   dartobsgen.trim_obs_seq
```
