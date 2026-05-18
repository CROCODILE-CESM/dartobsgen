"""Demo script: generate DART obs_seq files from CrocoLake.

Run from the dart_obs_gen directory after installing the package:

    pip install -e .
    python generate_obs_seqs.py

Output files are written to ./obs_output/ and named using 
the default convention, e.g. obs_seq.2010-05-01-00000.out
"""

from __future__ import annotations

import datetime

from dartobsgen import (
    CrocLakeSource,
    NNJASource,
    ObsGenConfig,
    ObsNetworkEntry,
    PerfectModelSource,
    generate_obs_sequences,
    polygon_from_netcdf_mask,
    polygon_from_netcdf_vertices,
    trim_obs_seq,
)

DART_PATH = "/Users/hkershaw/DART/Crocodile/Observations/DART"
CROCOLAKE_PATH = "/Users/hkershaw/DART/Crocodile/Observations/crocolake/"

OUTPUT_DIR = "./obs_output"


requested_obs = ["ARGO_TEMPERATURE",
    "ARGO_SALINITY",
    "ARGO_OXYGEN",
    "BOTTLE_TEMPERATURE",
    "BOTTLE_SALINITY",
    "BOTTLE_OXYGEN",
    "BOTTLE_ALKALINITY",
    "BOTTLE_INORGANIC_CARBON",
    "BOTTLE_NITRATE",
    "BOTTLE_SILICATE",
    "BOTTLE_PHOSPHATE",
    "GLIDER_TEMPERATURE",
    "GLIDER_SALINITY" ]


# def main() -> None:
#     config = ObsGenConfig(
#         start=datetime.datetime(2010, 5, 1),
#         end=datetime.datetime(2010, 5, 3),
#         lat_min=5.0,
#         lat_max=60.0,
#         lon_min=-100.0,
#         lon_max=-30.0,
#         obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
#         assimilation_frequency=datetime.timedelta(hours=6),
#         output_dir=OUTPUT_DIR,
#         output_prefix="obs_seq",
#         # Default format produces e.g. obs_seq.2010-05-01-00000.out
#         # Uncomment for compact DART format: obs_seq.2010050100.out
#         # output_timestamp_format="%Y%m%d%H",
#     )

#     source = CrocLakeSource(
#         crocolake_path=CROCOLAKE_PATH,
#         dart_path=DART_PATH,
#     )

#     print(f"Generating obs_seq files in: {OUTPUT_DIR}")
#     print(f"Time range : {config.start} → {config.end}")
#     print(f"Window size: {config.assimilation_frequency}")
#     print(f"Obs types  : {config.obs_types}")
#     print()

#     # Parallel across all available CPUs.
#     # Use max_workers=1 to run sequentially (easier to debug).
#     written = generate_obs_sequences(config, source, max_workers=None)

#     print()
#     print(f"Done. {len(written)} file(s) written:")
#     for path in written:
#         print(f"  {path}")

    # ------------------------------------------------------------------
    # Optional: trim to a polygon boundary
    # ------------------------------------------------------------------
    # Uncomment one of the blocks below and set the correct file/variable
    # names, then call trim_obs_seq on each written file.

    # -- Option 1: polygon from 1D boundary vertices in a NetCDF file --
    # poly = polygon_from_netcdf_vertices(
    #     "/path/to/boundary.nc",
    #     lat_var="boundary_lat",
    #     lon_var="boundary_lon",
    # )

    # -- Option 2: polygon from a 2D 0/1 mask in a NetCDF file ---------
    # poly = polygon_from_netcdf_mask(
    #     "/path/to/ocean_mask.nc",
    #     mask_var="mask",
    #     lat_var="lat",
    #     lon_var="lon",
    # )

    # -- Apply the trim (overwrite in place) ----------------------------
    # if written:
    #     print("\nApplying polygon trim...")
    #     trimmed = [p for p in written if trim_obs_seq(p, poly)]
    #     print(f"{len(trimmed)} file(s) retained observations after trim.")


# ------------------------------------------------------------------
# NNJA example (uncomment to use instead of, or alongside, CrocoLake)
# ------------------------------------------------------------------
def main() -> None:
    """Generate surface obs from NNJA-AI cloud archive."""
    config = ObsGenConfig(
        start=datetime.datetime(2021, 1, 1),
        end=datetime.datetime(2021, 1, 2),
        lat_min=-90.0,
        lat_max=90.0,
        lon_min=-180.0,
        lon_max=180.0,
        obs_types=[
            "METAR_TEMPERATURE_2_METER",
            "METAR_U_10_METER_WIND",
            "METAR_V_10_METER_WIND",
            # "RADIOSONDE_TEMPERATURE",
            # "RADIOSONDE_U_WIND_COMPONENT",
            # "RADIOSONDE_V_WIND_COMPONENT",
        ],
        assimilation_frequency=datetime.timedelta(hours=6),
        output_dir=OUTPUT_DIR,
        output_prefix="obs_seq",
    )

    source = NNJASource(catalog_mirror="gcp_nodd")

    print(f"Generating NNJA obs_seq files in: {OUTPUT_DIR}")
    written = generate_obs_sequences(config, source, max_workers=None)
    print(f"Done. {len(written)} file(s) written.")
    for path in written:
        print(f"  {path}")


# ------------------------------------------------------------------
# PerfectModelSource example (Lorenz 96)
# ------------------------------------------------------------------
# Prerequisites in DART_WORK_DIR:
#   - compiled perfect_model_obs executable
#   - input.nml with perfect_model_obs_nml block
#   - perfect_input.nc  (initial conditions, referenced by input.nml)
#
# def main() -> None:
#     """Generate synthetic Lorenz 96 obs via perfect_model_obs."""
#
#     DART_WORK_DIR = "/path/to/DART/models/MOM6/work"
#
#     # 40-point Lorenz 96 ring: evenly spaced, single level
#     import numpy as np
#     longitudes = np.linspace(-180.0, 180.0, 40, endpoint=False)
#     network = [
#         ObsNetworkEntry(
#             obs_type="RAW_STATE_VARIABLE",
#             lat=0.0,
#             lon=float(lon),
#             vertical=1.0,
#             vert_unit="level",
#             obs_err_var=1.0,
#         )
#         for lon in longitudes
#     ]
#
#     config = ObsGenConfig(
#         start=datetime.datetime(2000, 1, 1),
#         end=datetime.datetime(2000, 1, 2),
#         lat_min=-90.0,
#         lat_max=90.0,
#         lon_min=-180.0,
#         lon_max=180.0,
#         obs_types=["RAW_STATE_VARIABLE"],
#         assimilation_frequency=datetime.timedelta(hours=6),
#         output_dir=OUTPUT_DIR,
#     )
#
#     source = PerfectModelSource(
#         dart_work_dir=DART_WORK_DIR,
#         obs_network=network,
#     )
#
#     if __name__ == "__main__":
#         written = generate_obs_sequences(config, source, max_workers=1)
#         print(f"Done. {len(written)} file(s) written.")
#         for path in written:
#             print(f"  {path}")


if __name__ == "__main__":
    main()
