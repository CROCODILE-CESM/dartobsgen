# Time windows

Analysis times run from `first_analysis` through `end` inclusive, spaced by
`assimilation_frequency`. Give **exactly one** of `start` or `first_analysis`;
the other is derived, and both are populated on the config afterwards.

| Field | Meaning | Use it when |
|---|---|---|
| `start` | Start of the **model run**, not an analysis time. `first_analysis = start + freq`. | Observations exist independently of the model (`CrocLakeSource`, `NNJASource`) — pick the time you initialize the model. |
| `first_analysis` | The first analysis time itself. | The analysis times must land on times fixed by something else — in particular `PerfectModelSource`, where obs are placed at the valid times of the model states. |

`start` exists because in CESM+DART cycling the model advances one
assimilation period before the first assimilation. That implicit `+ freq` is
easy to trip over, so state it directly with `first_analysis` whenever you
know the analysis times you want:

```python
# These two are identical
ObsGenConfig(start=datetime.datetime(2010, 5, 1), ...)             # first analysis 06Z
ObsGenConfig(first_analysis=datetime.datetime(2010, 5, 1, 6), ...) # same thing, said directly
```

Passing neither, passing both, or configuring a run with no analysis times at
all (`first_analysis > end`) raises `ValueError` at construction.

All `ObsGenConfig` fields are keyword-only.

## Window bounds

Each analysis time `T` gets the window `(T - freq/2, T + freq/2]` — open
below, closed above. This matches DART's documented convention (see
`assimilation_code/programs/obs_sequence_tool/obs_sequence_tool.rst`):
*"the windows should be centered around the assimilation time starting at
minus 1/2 the window time plus 1 second, and ending at exactly plus 1/2 the
window time."*

Windows are contiguous and non-overlapping. Adjacent windows share a boundary
instant, and the closed upper bound assigns it to the earlier window, so no
observation is ever written to two files. Note that observations before
`first_analysis - freq/2` are deliberately unused — they belong to an earlier
analysis, which happened before this run began.

## Assimilation frequency

`assimilation_frequency` accepts any `datetime.timedelta` that is an even
whole number of seconds (so that the half-width `freq/2` lands on an integer
second, since DART times are integer day/second pairs). Sub-hourly windows are
supported:

```python
import datetime
from dartobsgen import ObsGenConfig

# 6-hour windows (default): analyses at 06Z, 12Z, 18Z, 00Z
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(hours=6))

# 30-minute windows
config = ObsGenConfig(..., assimilation_frequency=datetime.timedelta(minutes=30))
```

To land on conventional analysis times, either name the first one directly
with `first_analysis=datetime.datetime(2010, 5, 1, 6)` (06Z), or set `start`
one frequency before it: `start=datetime.datetime(2010, 5, 1)` (00Z).
