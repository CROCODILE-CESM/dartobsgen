# Data sources

A data source answers one question: given an assimilation window, write the
observations that fall inside it to an obs_seq file. Everything else — the
window arithmetic, the file naming, the parallelism — is
{py:func}`~dartobsgen.generate_obs_sequences`' job, so swapping sources changes
one object and nothing else.

| Source | Observations from | Status |
|---|---|---|
| {py:class}`~dartobsgen.CrocLakeSource` | CrocoLake parquet database (ARGO, GLODAP, SprayGliders) | supported |
| {py:class}`~dartobsgen.NNJASource` | NNJA-AI cloud archive (NOAA/NASA Joint Archive) | under development |
| {py:class}`~dartobsgen.PerfectModelSource` | synthetic, via DART's `perfect_model_obs` | under development |
| {py:class}`~dartobsgen.ObsSeqSource` | a bank of existing obs_seq files | stub |

To write your own, see [](./extending.md).

```{toctree}
:maxdepth: 2

crocolake
nnja
perfect_model
extending
```
