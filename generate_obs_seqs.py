"""Demo script: generate DART obs_seq files from CrocoLake.

Run from the dart_obs_gen directory after installing the package:

    pip install -e .
    python generate_obs_seqs.py

Output files are written to ./obs_output/ and named using 
the default convention, e.g. obs_seq.2010-05-01-00000.out
"""

from __future__ import annotations

import datetime

from dartobsgen import CrocLakeSource, ObsGenConfig, generate_obs_sequences

DART_PATH = "/Users/hkershaw/DART/Crocodile/Observations/DART"
CROCOLAKE_PATH = "/Users/hkershaw/DART/Crocodile/Observations/crocolake/"

OUTPUT_DIR = "./obs_output"


def main() -> None:
    config = ObsGenConfig(
        start=datetime.datetime(2010, 5, 1),
        end=datetime.datetime(2010, 5, 3),
        lat_min=5.0,
        lat_max=60.0,
        lon_min=-100.0,
        lon_max=-30.0,
        obs_types=["ARGO_TEMPERATURE", "ARGO_SALINITY"],
        assimilation_frequency=datetime.timedelta(hours=6),
        output_dir=OUTPUT_DIR,
        output_prefix="obs_seq",
        # Default format produces e.g. obs_seq.2010-05-01-00000.out
        # Uncomment for compact DART format: obs_seq.2010050100.out
        # output_timestamp_format="%Y%m%d%H",
    )

    source = CrocLakeSource(
        crocolake_path=CROCOLAKE_PATH,
        dart_path=DART_PATH,
    )

    print(f"Generating obs_seq files in: {OUTPUT_DIR}")
    print(f"Time range : {config.start} → {config.end}")
    print(f"Window size: {config.assimilation_frequency}")
    print(f"Obs types  : {config.obs_types}")
    print()

    # Parallel across all available CPUs.
    # Use max_workers=1 to run sequentially (easier to debug).
    written = generate_obs_sequences(config, source, max_workers=2)

    print()
    print(f"Done. {len(written)} file(s) written:")
    for path in written:
        print(f"  {path}")


if __name__ == "__main__":
    main()
