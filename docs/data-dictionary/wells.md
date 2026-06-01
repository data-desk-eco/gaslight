# `wells`

Every oil/gas well in the Permian (Texas), with location, operator, and the
flaring metrics of the lease it belongs to. The most useful table for
lead-hunting: filter by operator, intensity, or area.

- **Grain:** one row per well (API)
- **Rows:** 324,692
- **Source:** RRC well, operator, and P-4 gatherer records, RRC Production Data Query (PDQ)
- **Scope:** Wells with valid coordinates inside the Permian (Texas) bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `api` | VARCHAR | Well API number (RRC unique well identifier). |
| `oil_gas_code` | VARCHAR | Whether the well is on an Oil (O) or Gas (G) lease. |
| `lease_district` | VARCHAR | RRC district of the lease (alphanumeric). |
| `lease_number` | VARCHAR | RRC lease number, zero-padded to 6 digits. |
| `well_number` | VARCHAR | Well number within the lease. |
| `operator_no` | VARCHAR | RRC operator number. |
| `operator_name` | VARCHAR | Current operator (company); "Unknown" if unmatched. |
| `latitude` | DOUBLE | Well latitude, decimal degrees WGS84. |
| `longitude` | DOUBLE | Well longitude, decimal degrees WGS84. |
| `flared_mcf` | DOUBLE | Lease total reported gas flared/vented, MCF, 2021+ (repeated per well). |
| `produced_mcf` | DOUBLE | Lease total gas produced, MCF, 2021+ (denominator for intensity). |
| `flaring_intensity_pct` | DOUBLE | Lease flared ÷ produced, percent (null when no production). |
| `lease_name` | VARCHAR | Lease name. |

## Caveats

`flared_mcf`, `produced_mcf`, and `flaring_intensity_pct` are **lease-level**
totals (2021+), repeated on every well of the lease — they are not per-well.
Wells whose lease reported no flaring show 0 / null. Flaring figures are
operator-reported (see the dataset caveat). `operator_name` is the well's
current RRC operator; "Unknown" where unmatched.

## Example

```sql
-- highest-intensity leases, via their wells
SELECT DISTINCT lease_district, lease_number, lease_name, operator_name,
       flared_mcf, flaring_intensity_pct
FROM wells
WHERE flaring_intensity_pct IS NOT NULL
ORDER BY flaring_intensity_pct DESC
LIMIT 25;
```

---

[← back to index](README.md)

