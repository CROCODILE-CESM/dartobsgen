# Quickstart

A `dartobsgen` run has three pieces: a {py:class}`~dartobsgen.ObsGenConfig`
saying *when* and *where*, a {py:class}`~dartobsgen.DataSource` saying *where
the observations come from*, and
{py:func}`~dartobsgen.generate_obs_sequences` to drive one window at a time.

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

`generate_obs_sequences` returns the paths of the files it actually wrote.
Windows containing no observations are silently skipped, so the returned list
can be shorter than the number of analysis times.

## Where to go next

- [Time windows](user_guide/time_windows.md) — the `start` vs `first_analysis`
  distinction, and how each window is bounded. Worth reading before your first
  real run.
- [Output file naming](user_guide/output_naming.md) — the `{S}` seconds-of-day
  token and custom timestamp formats.
- [Observation types](user_guide/obs_types.md) — the three naming styles
  `obs_types` accepts, and the supported types.
- [Parallel generation](user_guide/parallel.md) — including the
  `if __name__ == "__main__":` requirement.
- [Data sources](user_guide/sources/index.md) — CrocoLake, NNJA-AI, and
  synthetic observations.
