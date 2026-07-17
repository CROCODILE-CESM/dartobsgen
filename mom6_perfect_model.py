"""Generate synthetic MOM6 ocean observations with dartobsgen.

Uses PerfectModelSource to drive DART's perfect_model_obs with a MOM6
model state, producing one obs_seq file per assimilation window.

@todo HK DART_WORK_DIR needs a better name, because we do not really
want people running in the source tree DART/models/MOM6/work. 
Prerequisites in DART_WORK_DIR:
  - Compiled perfect_model_obs executable
  - input.nml with a perfect_model_obs_nml block
  - mom6.r.nc (template_file), mom6.static.nc, ocean_geometry.nc
    referenced by model_nml

MODEL_OUTPUT points at restart-format output from a MOM6 run (a single
multi-timeslice file, or a glob over per-time files).  Each assimilation
window uses the earliest timeslice inside it; windows with no slice are
skipped.
"""

from __future__ import annotations

import datetime

import numpy as np

import os

from dartobsgen import (
    MOM6StateProvider,
    ObsGenConfig,
    ObsNetworkEntry,
    PerfectModelSource,
    generate_obs_sequences,
    polygon_from_netcdf_mask,
    state_vars_from_nml,
    trim_obs_seq,
)

_HERE = os.path.dirname(os.path.abspath(__file__))

DART_WORK_DIR = os.path.join(_HERE, "pmo_run")
OUTPUT_DIR = "./obs_output"

# MOM6 output (restart or z-space history format, matching the
# model_state_variables in input.nml): one multi-timeslice file or a glob.
MODEL_OUTPUT = os.path.join(
    _HERE,
    "example_mom6/EEP_MITgcm185Lvgrid_Whitt2026hgrid.mom6.h.z.2015-10-004/mom6.h.nc",
)
STATE_CACHE_DIR = "./state_cache"

# Argo-like profile depths in metres
PROFILE_DEPTHS = [10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]

# Sparse lat/lon grid for synthetic profile locations, inside the
# Eastern Equatorial Pacific example domain (lat ±12, lon 190–265°E).
LATS = np.arange(-10.0, 11.0, 4.0)
LONS = np.arange(-165.0, -96.0, 10.0)


def build_network() -> list[ObsNetworkEntry]:
    network = []
    for lat in LATS:
        for lon in LONS:
            for depth in PROFILE_DEPTHS:
                network.append(ObsNetworkEntry(
                    obs_type="FLOAT_TEMPERATURE",
                    lat=float(lat),
                    lon=float(lon),
                    vertical=depth,
                    vert_unit="height (m)",
                    obs_err_var=0.04,   # (0.2 °C)²
                ))
                # DART salinity obs are kg/kg (model_mod converts from psu)
                network.append(ObsNetworkEntry(
                    obs_type="FLOAT_SALINITY",
                    lat=float(lat),
                    lon=float(lon),
                    vertical=depth,
                    vert_unit="height (m)",
                    obs_err_var=1.0e-8,   # (0.0001 kg/kg = 0.1 psu)²
                ))
    return network


def main() -> None:
    network = build_network()
    n_profiles = len(LATS) * len(LONS)
    print(f"Network: {len(network)} obs  "
          f"({n_profiles} locations × {len(PROFILE_DEPTHS)} depths × 2 variables)")

    config = ObsGenConfig(
        start=datetime.datetime(2015, 10, 1),
        end=datetime.datetime(2015, 10, 8),
        lat_min=-12.0,
        lat_max=12.0,
        lon_min=-170.0,
        lon_max=-95.0,
        obs_types=["FLOAT_TEMPERATURE", "FLOAT_SALINITY"],
        assimilation_frequency=datetime.timedelta(hours=24),
        output_dir=OUTPUT_DIR,
        output_prefix="obs_seq",
    )

    # Validate the model output against exactly what DART will read.
    provider = MOM6StateProvider(
        MODEL_OUTPUT,
        cache_dir=STATE_CACHE_DIR,
        required_vars=state_vars_from_nml(os.path.join(DART_WORK_DIR, "input.nml")),
    )
    source = PerfectModelSource(
        dart_work_dir=DART_WORK_DIR,
        obs_network=network,
        state_provider=provider,
    )

    print(f"Writing obs_seq files to: {OUTPUT_DIR}")
    # max_workers=1 runs windows sequentially; set to None for parallel.
    written = generate_obs_sequences(config, source, max_workers=1)
    print(f"Done. {len(written)} file(s) written.")

    # ------------------------------------------------------------------
    # Optional: trim to the MOM6 ocean mask so obs over land are removed.
    # Set the path to your MOM6 ocean_mask.nc (or ocean_static.nc).
    # ------------------------------------------------------------------
    # poly = polygon_from_netcdf_mask(
    #     "/path/to/MOM6/INPUT/ocean_mask.nc",
    #     mask_var="mask",
    #     lat_var="lat",
    #     lon_var="lon",
    # )
    # trimmed = [p for p in written if trim_obs_seq(p, poly)]
    # print(f"{len(trimmed)} file(s) retained obs after land-mask trim.")

    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
