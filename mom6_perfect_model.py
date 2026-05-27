"""Generate synthetic MOM6 ocean observations with dartobsgen.

Uses PerfectModelSource to drive DART's perfect_model_obs with a MOM6
model state, producing one obs_seq file per assimilation window.

@todo HK DART_WORK_DIR needs a better name, because we do not really
want people running in the source tree DART/models/MOM6/work. 
Prerequisites in DART_WORK_DIR:
  - Compiled perfect_model_obs executable
  - input.nml with a perfect_model_obs_nml block
  - MOM6 IC/restart file referenced by input.nml
"""

from __future__ import annotations

import datetime

import numpy as np

from dartobsgen import (
    ObsGenConfig,
    ObsNetworkEntry,
    PerfectModelSource,
    generate_obs_sequences,
    polygon_from_netcdf_mask,
    trim_obs_seq,
)

DART_WORK_DIR = "/Users/hkershaw/DART/Crocodile/Observations/dart_obs_gen/DART/models/MOM6/work"
OUTPUT_DIR = "./obs_output"

# Argo-like profile depths in metres
PROFILE_DEPTHS = [10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]

# Sparse lat/lon grid for synthetic profile locations
LATS = np.arange(-60.0, 61.0, 20.0)
LONS = np.arange(-160.0, 161.0, 30.0)


def build_network() -> list[ObsNetworkEntry]:
    network = []
    for lat in LATS:
        for lon in LONS:
            for depth in PROFILE_DEPTHS:
                network.append(ObsNetworkEntry(
                    obs_type="OCEAN_TEMPERATURE",
                    lat=float(lat),
                    lon=float(lon),
                    vertical=depth,
                    vert_unit="height (m)",
                    obs_err_var=0.04,   # (0.2 °C)²
                ))
                network.append(ObsNetworkEntry(
                    obs_type="OCEAN_SALINITY",
                    lat=float(lat),
                    lon=float(lon),
                    vertical=depth,
                    vert_unit="height (m)",
                    obs_err_var=0.01,   # (0.1 PSU)²
                ))
    return network


def main() -> None:
    network = build_network()
    n_profiles = len(LATS) * len(LONS)
    print(f"Network: {len(network)} obs  "
          f"({n_profiles} locations × {len(PROFILE_DEPTHS)} depths × 2 variables)")

    config = ObsGenConfig(
        start=datetime.datetime(2010, 1, 1),
        end=datetime.datetime(2010, 1, 8),
        lat_min=-90.0,
        lat_max=90.0,
        lon_min=-180.0,
        lon_max=180.0,
        obs_types=["OCEAN_TEMPERATURE", "OCEAN_SALINITY"],
        assimilation_frequency=datetime.timedelta(hours=24),
        output_dir=OUTPUT_DIR,
        output_prefix="obs_seq",
    )

    source = PerfectModelSource(
        dart_work_dir=DART_WORK_DIR,
        obs_network=network,
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
