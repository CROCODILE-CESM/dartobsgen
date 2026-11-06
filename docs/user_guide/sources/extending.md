# Adding a new data source

Subclass {py:class}`dartobsgen.DataSource` and implement `write_obs_seq()`:

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

## Optional: `check_coverage`

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

## Adding a model state provider

`PerfectModelSource` is model-agnostic; everything model-specific lives in a
`ModelStateProvider`. For models whose output is netCDF files, subclass
`TimesliceStateProvider` and implement two hooks (a third is optional):

```python
from dartobsgen import TimesliceStateProvider

class MyModelStateProvider(TimesliceStateProvider):
    cache_prefix = "mymodel_state"
    # namelist entries naming files pmo must read (checked at construction)
    input_file_keys = (("perfect_model_obs_nml", "input_state_files"),)

    def _validate(self, ds, path):
        """Raise ValueError if ds is not this model's output."""

    def _scan_file(self, ds, path):
        """Return [(time_index, valid_time), ...] for every slice in ds.

        valid_time must equal what the model's DART read_model_time returns."""

    # optional: default uses the file in place (one timeslice per file)
    def _materialise(self, ds, src_path, time_index, cache_path):
        """Write slice time_index to cache_path via self._write_cached(...)."""
```

The base class handles file resolution, the sorted index, `available_times`,
the `(date0, date1]` window selection and the atomic slice cache. Providers
that don't read netCDF files can subclass `ModelStateProvider` directly and
implement `state_for_window`.

If the model's `model_state_variables` records are not 5 fields long, pass
`fields_per_entry` to `state_vars_from_nml`.
