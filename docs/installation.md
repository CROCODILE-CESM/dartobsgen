# Installation

`dartobsgen` requires Python 3.10 or newer.

## From a clone

```bash
git clone https://github.com/CROCODILE-CESM/dartobsgen.git
cd dartobsgen
pip install -e .
```

## What gets installed

The core dependencies (`pyarrow`, `dask[dataframe]`, `pandas`, `shapely`,
`xarray`, `netcdf4`, `scikit-image`, `scipy`, `f90nml`, `gsw`, `pydartdiags`,
`nnja-ai`) all come in with the package.

## What you also need

`dartobsgen` writes DART `obs_seq` files, so a DART checkout is required for
most workflows:

- {py:class}`~dartobsgen.CrocLakeSource` needs `dart_path` — the root of a
  DART clone — to resolve obs type definitions.
- {py:class}`~dartobsgen.PerfectModelSource` needs a `dart_work_dir`
  containing a **compiled** `perfect_model_obs` executable and its
  `input.nml`. See
  [Synthetic observations](user_guide/sources/perfect_model.md#prerequisites).
- {py:func}`~dartobsgen.trim_obs_seq` and
  {py:class}`~dartobsgen.NNJASource` work on `obs_seq` files directly and need
  no DART build.

See the [DART documentation](https://docs.dart.ucar.edu/en/latest/) for building DART.

## Development install

```bash
pip install -e ".[dev]"     # adds pytest
pytest
```

## Building the documentation

```bash
pip install -e ".[docs]"
sphinx-build -b html docs docs/_build/html
open docs/_build/html/index.html
```

The docs build imports `dartobsgen` for the API reference, but only
`numpy`, `pandas` and `shapely` are imported at module scope — the heavier
dependencies are imported lazily inside the functions that use them. A
docs-only environment therefore does not need the full dependency set:

```bash
pip install --no-deps -e .
pip install ".[docs]"
```
