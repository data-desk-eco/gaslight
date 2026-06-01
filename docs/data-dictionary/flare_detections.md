# `flare_detections`

The per-night detection time series behind `flare_sites` — one row for every
night a given flare was observed burning. Powers timelines and lets you see
when a flare started, stopped, or intensified.

- **Grain:** one row per flare site × detected night
- **Rows:** 205,969
- **Source:** VIIRS Nightfire (VNF)
- **Scope:** Detected nights only, 2021-01-01 onward.

## Schema

| column | type | description |
| --- | --- | --- |
| `flare_id` | INTEGER | VIIRS Nightfire site identifier; joins to flare_sites. |
| `date` | DATE | Detection date (night of the satellite pass). |
| `rh_mw` | DOUBLE | Radiant heat for that night, megawatts. |

## Caveats

Only nights with a detection are present (no zero rows for quiet nights, and
cloudy nights may simply be missing). `rh_mw` is radiant heat in megawatts.

## Example

```sql
SELECT date, rh_mw
FROM flare_detections
WHERE flare_id = (SELECT flare_id FROM flare_sites ORDER BY total_rh_mw DESC LIMIT 1)
ORDER BY date;
```

---

[← back to index](README.md)

