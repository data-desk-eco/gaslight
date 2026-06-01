# `flare_sites`

One row per VIIRS Nightfire flare site detected in the Permian during the
analysis window — the satellite-observed flare locations and how active
each has been. The primary "where are the flares" layer.

- **Grain:** one row per VNF flare site (`flare_id`)
- **Rows:** 1,297
- **Source:** VIIRS Nightfire (VNF)
- **Scope:** Detected nights only, 2021-01-01 onward, Permian (Texas) bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `flare_id` | INTEGER | VIIRS Nightfire site identifier (assigned by the Earth Observation Group). |
| `lat` | DOUBLE | Site latitude, decimal degrees WGS84 (mean of detected passes). |
| `lon` | DOUBLE | Site longitude, decimal degrees WGS84 (mean of detected passes). |
| `detection_days` | BIGINT | Number of distinct nights the site was detected burning (2021+). |
| `first_detected` | DATE | First detected night in the window. |
| `last_detected` | DATE | Most recent detected night. |
| `total_rh_mw` | DOUBLE | Sum of per-night radiant heat across all detected nights, megawatts. |
| `avg_rh_mw` | DOUBLE | Mean radiant heat over nights with a positive reading, megawatts. |

## Caveats

`total_rh_mw` and `avg_rh_mw` are radiant heat in **megawatts** — a measure
of fire intensity, *not* a gas volume. Position is the mean of all detected
passes. Presence here means a flare was *seen burning*; it is independent of
whether the operator reported flaring (see `monthly_flaring`).

## Example

```sql
-- most persistently-burning sites
SELECT flare_id, lat, lon, detection_days, total_rh_mw
FROM flare_sites
ORDER BY detection_days DESC
LIMIT 25;
```

---

[← back to index](README.md)

