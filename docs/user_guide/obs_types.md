# Observation types

`obs_types` for CrocoLake as a source accepts three styles — they can be freely
mixed:

| Style | Example | Meaning |
|---|---|---|
| DART compound name | `"ARGO_TEMPERATURE"` | TEMP from ARGO only |
| DART variable name | `"TEMPERATURE"` | TEMP from all sources |
| CrocoLake var name | `"TEMP"` | TEMP from all sources |

## Supported obs types

| DART compound name          | CrocoLake var    | DB source     |
|-----------------------------|------------------|---------------|
| `ARGO_TEMPERATURE`          | `TEMP`           | ARGO          |
| `ARGO_SALINITY`             | `PSAL`           | ARGO          |
| `ARGO_OXYGEN`               | `DOXY`           | ARGO          |
| `BOTTLE_TEMPERATURE`        | `TEMP`           | GLODAP        |
| `BOTTLE_SALINITY`           | `PSAL`           | GLODAP        |
| `BOTTLE_OXYGEN`             | `DOXY`           | GLODAP        |
| `BOTTLE_ALKALINITY`         | `TOT_ALKALINITY` | GLODAP        |
| `BOTTLE_INORGANIC_CARBON`   | `TCO2`           | GLODAP        |
| `BOTTLE_NITRATE`            | `NITRATE`        | GLODAP        |
| `BOTTLE_SILICATE`           | `SILICATE`       | GLODAP        |
| `BOTTLE_PHOSPHATE`          | `PHOSPHATE`      | GLODAP        |
| `GLIDER_TEMPERATURE`        | `TEMP`           | SprayGliders  |
| `GLIDER_SALINITY`           | `PSAL`           | SprayGliders  |
| `TEMPERATURE`               | `TEMP`           | all           |
| `SALINITY`                  | `PSAL`           | all           |
| `OXYGEN`                    | `DOXY`           | all           |

Pass a custom `obs_type_map` dict to `ObsGenConfig` to override or extend:

```python
my_map = {
    "MY_CUSTOM_TEMP": {"crocolake_var": "TEMP", "db_name": "MyDB"},
}
config = ObsGenConfig(..., obs_type_map=my_map)
```

Other sources define their own obs type maps: see
[NNJA-AI](sources/nnja.md#supported-nnja-obs-types) for its dataset and
variable mapping.
