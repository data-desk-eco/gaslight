# `plumes`

Methane plume observations from Carbon Mapper and UNEP IMEO — point
detections of methane emissions with estimated rates. An independent
emissions signal to cross-reference against flares and leases.

- **Grain:** one row per plume observation (`plume_id`)
- **Rows:** 6,581
- **Source:** Carbon Mapper methane plumes, UNEP IMEO methane plumes
- **Scope:** Permian bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `plume_id` | VARCHAR | Provider plume identifier. |
| `source` | VARCHAR | Data provider — 'cm' (Carbon Mapper) or 'imeo' (UNEP IMEO). |
| `satellite` | VARCHAR | Observing platform/instrument. |
| `date` | DATE | Observation date. |
| `latitude` | DOUBLE | Plume latitude, decimal degrees WGS84. |
| `longitude` | DOUBLE | Plume longitude, decimal degrees WGS84. |
| `emission_rate` | DOUBLE | Estimated methane emission rate, kg/hr. |
| `emission_uncertainty` | DOUBLE | Uncertainty on the emission rate, kg/hr. |
| `sector` | VARCHAR | Attributed sector — 'og' (oil & gas), 'coal', 'waste', or 'other'. |

## Caveats

Two providers are merged via `source` ('cm' = Carbon Mapper, 'imeo' = UNEP
IMEO); their methodologies and detection limits differ. `emission_rate` is an
instantaneous estimate (kg/hr), not a monthly total, with `emission_uncertainty`.

## Example

```sql
SELECT source, count(*) AS plumes, round(avg(emission_rate)) AS avg_kg_hr
FROM plumes WHERE sector = 'og' GROUP BY source;
```

---

[← back to index](README.md)

