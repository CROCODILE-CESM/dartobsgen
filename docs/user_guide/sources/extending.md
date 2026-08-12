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
