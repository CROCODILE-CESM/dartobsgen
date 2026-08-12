# Synthetic observations via `perfect_model_obs`

```{warning}
Under development.
```

{py:class}`~dartobsgen.PerfectModelSource` generates synthetic observations by
running DART's `perfect_model_obs` executable. For each assimilation window it:

1. Builds a template `obs_seq.in` from a user-defined observing network.
2. Patches `input.nml` with the obs_seq filenames and window time bounds.
3. Runs `perfect_model_obs` in an isolated per-window directory.
4. Returns the resulting `obs_seq.out` as the window's obs file.

The caller uses it identically to `CrocLakeSource` or `NNJASource` — only
the source object changes.

## Prerequisites

`dart_work_dir` must contain:

- The compiled `perfect_model_obs` executable
- A base `input.nml` with a `perfect_model_obs_nml` block (the source
  patches only the obs_seq filenames and time-bound fields)
- Any initial-conditions files referenced by `input.nml`

This is the same directory structure used to run `perfect_model_obs` by hand.

## `ObsNetworkEntry` fields

| Field | Type | Description |
|---|---|---|
| `obs_type` | `str` | DART obs type name, e.g. `"RAW_STATE_VARIABLE"`, `"TEMPERATURE"` |
| `lat` | `float` | Latitude in degrees |
| `lon` | `float` | Longitude in degrees (-180 to 180) |
| `vertical` | `float` | Vertical coordinate value |
| `vert_unit` | `str` | `"pressure (Pa)"`, `"height (m)"`, `"model level"`, `"surface (m)"` |
| `obs_err_var` | `float` | Observation error variance |
| `time_offset` | `timedelta` | Offset from the analysis time (default `timedelta(0)`) |

## Driving it from model output: `state_provider`

`perfect_model_obs` cannot advance a large model, so it interpolates from a
state file you hand it. `MOM6StateProvider` indexes the timeslices of a MOM6
run's output and serves one per window, and observations are then placed at
**the valid time of that state**, not at the analysis time.

That inverts the usual configuration: the analysis times must line up with the
model output times. Use `first_analysis`, not `start`:

```python
config = ObsGenConfig(
    first_analysis=datetime.datetime(2015, 10, 4, 12),  # first model output time
    end=datetime.datetime(2015, 10, 8, 12),             # last analysis time
    assimilation_frequency=datetime.timedelta(hours=24),
    ...
)
```

Using `start` here would place the first analysis one frequency *after* the
first model output time, and every state could fall outside every window.

A window selects the earliest state in `(date0, date1]` — open below, closed
above, matching the window `perfect_model_obs` is given (`first_obs = date0 +
1s`, `last_obs = date1`). A state landing exactly on `date0` belongs to the
previous window; selecting it would place every observation one second before
`first_obs` and DART would abort with *"All obs in sequence are before
first_obs_days:first_obs_seconds"*.

`PerfectModelSource.check_coverage` reports the alignment before any window
runs:

```text
Model state coverage: 1 of 5 window(s) have a state; 1 model output time(s)
(1 used, 0 shadowed, 0 outside all windows).
```

and raises when nothing lines up, naming the setting that would fix it:

```text
No model state falls in any assimilation window, so no observations can be generated.
  model output times: 2015-10-04T12:00:00
  analysis times:     2015-10-06T00:00:00, 2015-10-07T00:00:00, 2015-10-08T00:00:00
  windows cover:      (2015-10-05T12:00:00, 2015-10-08T12:00:00] every 1 day, 0:00:00
  Observations are placed at the model state's valid time, so the analysis
  times must line up with the model output times.  Set
  first_analysis=2015-10-04T12:00:00 and end>=2015-10-04T12:00:00 to cover
  the model output.
```

It also flags *shadowed* states (a second state in a window that already has
an earlier one — only the earliest is used) and *off-centre* states (in a
window but not at its analysis time, so obs land away from the window centre).

## Controlling observation time within a window

With a `state_provider`, observations are placed at the state's valid time.
Without one, they are placed at the **analysis time** — the centre of the
window. Either way, `time_offset` shifts individual entries relative to that
reference; it must stay within `(-freq/2, +freq/2]` or `perfect_model_obs`
will reject the observation as outside the window.

```python
import datetime

# Obs at the analysis time / window centre (default)
entry_centre = ObsNetworkEntry(..., time_offset=datetime.timedelta(0))

# Obs 1 hour after the analysis time
entry_late = ObsNetworkEntry(..., time_offset=datetime.timedelta(hours=1))
```

## MOM6 example

`mom6_perfect_model.py` shows a complete ocean example: synthetic temperature
and salinity profiles on a sparse lat/lon grid at six depths, drawn from a
MOM6 history file via `MOM6StateProvider` with daily assimilation windows
anchored on the model output times.

## Parallel execution

Each window runs in its own subdirectory (`dart_work_dir/windows/{timestamp}/`)
with symlinks back to the shared executable and initial-conditions files.
This avoids file conflicts when `ProcessPoolExecutor` runs multiple windows
simultaneously.

Use `max_workers=1` when debugging, or when `perfect_model_obs` itself
advances the model state (since state advancement must be sequential):

```python
written = generate_obs_sequences(config, source, max_workers=1)
```
