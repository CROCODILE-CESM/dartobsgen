# Parallel generation

`generate_obs_sequences` runs windows in parallel using
`concurrent.futures.ProcessPoolExecutor`. Control parallelism with the
`max_workers` argument:

```python
# All available CPUs (default)
written = generate_obs_sequences(config, source)

# Fixed number of worker processes
written = generate_obs_sequences(config, source, max_workers=4)

# Sequential (useful for debugging)
written = generate_obs_sequences(config, source, max_workers=1)
```

Each worker process independently opens the CrocoLake parquet database
and writes its own output file, so there are no shared-state conflicts.

```{note}
Scripts that call `generate_obs_sequences` with `max_workers != 1`
must be run under a `if __name__ == "__main__":` guard (standard Python
multiprocessing requirement on macOS / Windows).
```

{py:class}`~dartobsgen.PerfectModelSource` runs each window in its own
subdirectory so that concurrent `perfect_model_obs` invocations do not collide
— see [its parallel execution notes](sources/perfect_model.md#parallel-execution)
for the case where `max_workers=1` is required.
