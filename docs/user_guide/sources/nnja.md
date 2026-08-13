# NNJA-AI

```{warning}
Under development.
```

{py:class}`~dartobsgen.NNJASource` accesses the
[NNJA-AI](https://github.com/brightbandtech/nnja-ai) cloud-hosted observation
archive (NOAA/NASA Joint Archive) stored on GCS.

## Install the extra dependency

```bash
pip install nnja-ai
```

## Usage

```python
import datetime
from dartobsgen import ObsGenConfig, NNJASource, generate_obs_sequences

config = ObsGenConfig(
    start=datetime.datetime(2021, 1, 1),
    end=datetime.datetime(2021, 1, 2),
    lat_min=-90, lat_max=90,
    lon_min=-180, lon_max=180,
    obs_types=[
        "METAR_TEMPERATURE_2_METER",
        "METAR_U_10_METER_WIND",
        "METAR_V_10_METER_WIND",
    ],
    assimilation_frequency=datetime.timedelta(hours=6),
    output_dir="./obs_output",
)

source = NNJASource(catalog_mirror="gcp_nodd")

if __name__ == "__main__":
    written = generate_obs_sequences(config, source)
    print(written)
```

## Supported NNJA obs types

| DART obs type | NNJA dataset | Variable |
|---|---|---|
| `METAR_TEMPERATURE_2_METER` | `conv-adpsfc-NC000001` | `TMPSQ1.TMDB` (K) |
| `METAR_U_10_METER_WIND` | `conv-adpsfc-NC000001` | derived from `WNDSQ1.WSPD` + `WNDSQ1.WDIR` |
| `METAR_V_10_METER_WIND` | `conv-adpsfc-NC000001` | derived from `WNDSQ1.WSPD` + `WNDSQ1.WDIR` |
| `RADIOSONDE_TEMPERATURE` | `conv-adpupa-NC002001` | `TMDB_PRLC{n}` mandatory levels |
| `RADIOSONDE_U_WIND_COMPONENT` | `conv-adpupa-NC002001` | derived from `WDIR_PRLC{n}` + `WSPD_PRLC{n}` |
| `RADIOSONDE_V_WIND_COMPONENT` | `conv-adpupa-NC002001` | derived from `WDIR_PRLC{n}` + `WSPD_PRLC{n}` |

Radiosonde pressure suffix `n`: column `TMDB_PRLC5000` → 500 hPa = 50 000 Pa
(`pressure_Pa = int(n) × 10`).

## Custom error variances

Override the default error variances (1.0 for all obs types) by passing a
custom `obs_type_map` to `NNJASource` or to `ObsGenConfig`:

```python
source = NNJASource(
    catalog_mirror="gcp_nodd",
    obs_type_map={
        "METAR_TEMPERATURE_2_METER": {
            "nnja_dataset": "conv-adpsfc-NC000001",
            "kind": "scalar",
            "nnja_col": "TMPSQ1.TMDB",
            "vert_unit": "surface (m)",
            "vert_col": "SELV",
            "default_err_var": 4.0,   # 2 K std dev → 4 K² variance
        },
    },
)
```

## GCS mirror options

| Mirror | Description |
|---|---|
| `"gcp_nodd"` | NOAA Open Data Dissemination (default, open access) |
| `"gcp_brightband"` | Brightband mirror |
