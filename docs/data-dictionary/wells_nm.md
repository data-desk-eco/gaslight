# `wells_nm`

Every active, un-plugged oil/gas well on the New Mexico side of the Permian,
from the NM OCD well search. The counterpart to `wells_tx`; header
attributes only (no PDQ lease/flaring metrics exist for New Mexico).

- **Grain:** one row per well (API)
- **Rows:** 49,829
- **Source:** New Mexico OCD spill/release incidents
- **Scope:** NM OCD wells (surface location, cancelled APDs and plugged wells excluded) inside the Permian bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `api` | VARCHAR | Well API number (NM OCD, 30-###-##### format). |
| `well_name` | VARCHAR | Well name. |
| `well_number` | VARCHAR | Well number within the lease/unit. |
| `well_type` | VARCHAR | Well type (Oil, Gas, Injection, etc.). |
| `status` | VARCHAR | OCD well status (Active, New, etc.). |
| `operator` | VARCHAR | Current OCD operator-of-record (company name). |
| `ogrid` | VARCHAR | NM operator (OGRID) number. |
| `district` | VARCHAR | OCD district office (Hobbs, Artesia, etc.). |
| `section` | VARCHAR | Public-land-survey section. |
| `township` | VARCHAR | Public-land-survey township. |
| `range` | VARCHAR | Public-land-survey range. |
| `footages` | VARCHAR | Footage call from the section lines (e.g. "935 FSL, 1443 FWL"). |
| `apd_date` | DATE | Initial APD approval date. |
| `spud_date` | DATE | Spud date, where recorded. |
| `last_production` | DATE | Most recent month with reported production (first of month). |
| `measured_depth` | DOUBLE | Measured depth, feet. |
| `true_vertical_depth` | DOUBLE | True vertical depth, feet. |
| `latitude` | DOUBLE | Well latitude, decimal degrees (NAD83). |
| `longitude` | DOUBLE | Well longitude, decimal degrees (NAD83). |

## Caveats

No flaring/production figures — New Mexico has no equivalent of the RRC PDQ
disposition feed, so unlike `wells_tx` these rows carry location and status
only. `operator` is the current OCD operator-of-record. Coordinates are
NAD83 as published by the OCD.

## Example

```sql
-- active NM Permian wells by operator
SELECT operator, count(*) AS n
FROM wells_nm WHERE status = 'Active'
GROUP BY operator ORDER BY n DESC LIMIT 25;
```

---

[← back to index](README.md)

