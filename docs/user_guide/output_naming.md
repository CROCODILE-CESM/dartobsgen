# Output file naming

Files are named `{output_prefix}.{timestamp}.out` where the timestamp
is formatted using `output_timestamp_format` (default: `"%Y-%m-%d-{S}"`)
and applied to the **analysis time** — the centre of the window, which is
also the model's stopping time. This a DART convention, `filter` assimilates
the observations in a window centered on the analysis time, so the observation
sequence file is named `obs_seq.$analysis_time`.

The special token `{S}` is replaced with **seconds-of-day** (0–86400,
zero-padded to 5 digits), following DART-CESM typical obs_seq naming convention.
All other tokens follow Python `strftime` format.

| Analysis time      | Window                          | Default filename               |
|--------------------|---------------------------------|--------------------------------|
| 2010-05-01 06:00   | (03:00, 09:00]                  | `obs_seq.2010-05-01-21600.out` |
| 2010-05-01 12:00   | (09:00, 15:00]                  | `obs_seq.2010-05-01-43200.out` |
| 2010-05-01 18:00   | (15:00, 21:00]                  | `obs_seq.2010-05-01-64800.out` |
| 2010-05-02 00:00   | (05-01 21:00, 05-02 03:00]      | `obs_seq.2010-05-02-00000.out` |

To use a custom format (e.g. DART's compact `YYYYMMDDHH`):

```python
config = ObsGenConfig(..., output_timestamp_format="%Y%m%d%H")
# produces: obs_seq.2010050100.out, obs_seq.2010050106.out, ...
```
