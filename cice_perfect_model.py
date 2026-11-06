"""Generate synthetic CICE sea-ice observations with dartobsgen.

Uses PerfectModelSource to drive DART's perfect_model_obs with a CICE
restart state, producing one obs_seq file per assimilation window.

The same pieces as mom6_perfect_model.py, with CICEStateProvider in place of
MOM6StateProvider.  What differs for CICE:

* Each restart file (``iced.*``) holds one timeslice, with the time in its
  global attributes ``nyr``, ``month``, ``mday`` and ``sec``.  MODEL_OUTPUT is
  therefore a glob over restart files; nothing is extracted or cached.
* CICE's ``model_state_variables`` records are 3 fields long
  ``(variable, quantity, update)``, so ``fields_per_entry=3``.
* PMO_RUN_DIR needs the compiled perfect_model_obs and an input.nml.  CICE's
  template and grid files are fixed names inside DART's CICE model_mod, not
  namelist entries, so PerfectModelSource cannot check them for you.

Observations are placed at each restart's valid time, so the analysis times
must line up with the restart times: use ``first_analysis=<first restart
time>``, not ``start``.

EDIT the paths, dates and obs types below for your run.  The obs types must
be ones DART's obs_def_cice_mod defines (e.g. SYN_SEAICE_CONCENTR) and that
are listed in your input.nml's ``obs_kind_nml``.
"""

from __future__ import annotations

import datetime
import os

import numpy as np

from dartobsgen import (
    CICEStateProvider,
    ObsGenConfig,
    ObsNetworkEntry,
    PerfectModelSource,
    generate_obs_sequences,
    state_vars_from_nml,
)

PMO_RUN_DIR = "/path/to/cice_pmo_run"
MODEL_OUTPUT = "/path/to/cice_run/iced.*.nc"
OUTPUT_DIR = "./obs_output_cice"
STATE_CACHE_DIR = "./state_cache"  # unused for CICE (one slice per file)

# Sparse lat/lon grid of synthetic sea-ice concentration observations (Arctic)
LATS = np.arange(65.0, 86.0, 5.0)
LONS = np.arange(-180.0, 180.0, 30.0)


def build_network() -> list[ObsNetworkEntry]:
    return [
        ObsNetworkEntry(
            obs_type="SYN_SEAICE_CONCENTR",
            lat=float(lat),
            lon=float(lon),
            vertical=0.0,
            vert_unit="surface (m)",
            obs_err_var=0.01,  # (0.1 concentration)^2
        )
        for lat in LATS
        for lon in LONS
    ]


def main() -> None:
    network = build_network()
    print(f"Network: {len(network)} obs")

    config = ObsGenConfig(
        first_analysis=datetime.datetime(2010, 1, 2, 0),  # first restart time
        end=datetime.datetime(2010, 1, 31, 0),
        lat_min=60.0,
        lat_max=90.0,
        lon_min=-180.0,
        lon_max=180.0,
        obs_types=["SYN_SEAICE_CONCENTR"],
        assimilation_frequency=datetime.timedelta(days=1),
        output_dir=OUTPUT_DIR,
        output_prefix="obs_seq",
    )

    provider = CICEStateProvider(
        MODEL_OUTPUT,
        cache_dir=STATE_CACHE_DIR,
        required_vars=state_vars_from_nml(
            os.path.join(PMO_RUN_DIR, "input.nml"), fields_per_entry=3
        ),
    )
    source = PerfectModelSource(
        pmo_run_dir=PMO_RUN_DIR,
        obs_network=network,
        state_provider=provider,
    )

    written = generate_obs_sequences(config, source, max_workers=1)
    print(f"Done. {len(written)} file(s) written.")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
