# `vnf_detections`

The per-night detection time series behind `vnf_sites` — one row for every
night a given flare was observed burning. Powers timelines and lets you see
when a flare started, stopped, or intensified.

- **Grain:** one row per flare site × detected night
- **Rows:** 205,969
- **Source:** VIIRS Nightfire (VNF)
- **Scope:** Detected nights only, 2021-01-01 onward.

## Schema

| column | type | description |
| --- | --- | --- |
| `flare_id` | INTEGER | VIIRS Nightfire site identifier; joins to vnf_sites. |
| `date` | DATE | Detection date (night of the satellite pass). |
| `rh_mw` | DOUBLE | Radiant heat for that night, megawatts. |
| `flow_rate` | DOUBLE | EOG-estimated flaring flow rate for that night (VNF `Flow_Rate` field, as published). Null when the Nightfire fit did not converge. |

## Caveats

Only nights with a detection are present (no zero rows for quiet nights, and
cloudy nights may simply be missing). `rh_mw` is radiant heat in megawatts;
`flow_rate` is the EOG-estimated flaring flow rate (VNF `Flow_Rate` field,
as published), present only on the ~63% of detections with a converged
Nightfire fit.

## Example

```sql
SELECT date, rh_mw
FROM vnf_detections
WHERE flare_id = (SELECT flare_id FROM vnf_sites ORDER BY total_rh_mw DESC LIMIT 1)
ORDER BY date;
```

---

[← back to index](README.md)

