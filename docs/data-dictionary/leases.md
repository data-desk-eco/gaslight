# `leases`

Lease-level rollup of reported flaring for every Permian lease that reported
any flaring in the window — totals, intensity, and well count. The quickest
way to rank leases by reported flaring.

- **Grain:** one row per lease (`lease_district` + `lease_number`)
- **Rows:** 28,219
- **Source:** RRC Production Data Query (PDQ), RRC well, operator, and P-4 gatherer records
- **Scope:** Permian (Texas) leases with reported flaring, 2021+.

## Schema

| column | type | description |
| --- | --- | --- |
| `lease_district` | VARCHAR | RRC district (alphanumeric). |
| `lease_number` | VARCHAR | RRC lease number, zero-padded to 6 digits. |
| `lease_name` | VARCHAR | Lease name (most common spelling across months). |
| `operator_no` | VARCHAR | RRC operator number of the current operator-of-record. |
| `operator_name` | VARCHAR | Current operator-of-record (latest reported month). |
| `total_flared_mcf` | DOUBLE | Total gas flared/vented over the window, MCF (reported). |
| `total_gas_prod_mcf` | DOUBLE | Total gas produced over the window, MCF. |
| `flaring_intensity_pct` | DOUBLE | total_flared ÷ total_produced, percent (nullable). |
| `well_count` | BIGINT | Number of wells on the lease (within the Permian bbox). |

## Caveats

Only leases with reported flaring appear (a lease absent here reported none).
Volumes are operator-reported. `flaring_intensity_pct` is null when the lease
has flaring but no recorded production denominator.

`operator_name`/`operator_no` are the **current** operator-of-record (latest
reported month) — a lease-level label, not an attribution of the whole
total. Do **not** rank operators by summing `total_flared_mcf` here: it
credits acquirers with a predecessor's pre-deal flaring. For per-operator
totals use `monthly_flaring` (operator-attributed per month). `operator_no`
is the RRC legal-entity id and does not fold subsidiaries/acquisitions into
a parent — company rollups still need a separate crosswalk.

## Example

```sql
SELECT lease_district, lease_number, lease_name, operator_name,
       total_flared_mcf, flaring_intensity_pct, well_count
FROM leases
ORDER BY total_flared_mcf DESC
LIMIT 25;
```

---

[← back to index](README.md)

