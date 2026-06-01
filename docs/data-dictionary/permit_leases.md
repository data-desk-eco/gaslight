# `permit_leases`

The leases underlying each SWR 32 filing. Commingle permits cover multiple
leases; this flattens a filing to its constituent leases so permits can be
joined to production, wells, and gatherers.

- **Grain:** one row per filing × underlying lease
- **Rows:** 80,217
- **Source:** RRC Statewide Rule 32 flaring/venting permits
- **Scope:** Leases of permits present in `permits`.

## Schema

| column | type | description |
| --- | --- | --- |
| `filing_no` | VARCHAR | RRC filing number; joins to permits. |
| `property_type` | VARCHAR | Lease type (Oil Lease, Gas Lease, Drilling Permit). |
| `lease_district` | VARCHAR | RRC district of the lease (alphanumeric, e.g. 7C, 08, 8A). |
| `lease_number` | VARCHAR | RRC lease number, zero-padded to 6 digits. |
| `lease_name` | VARCHAR | Lease name on the filing. |
| `requested_release_rate_mcf_day` | DOUBLE | Requested release rate for this lease, MCF/day (nullable). |

## Caveats

`requested_release_rate_mcf_day` is per-lease as stated on the filing and may
be null. Join to leases on (`lease_district`, `lease_number`).

## Example

```sql
SELECT pl.filing_no, pl.lease_name, pl.requested_release_rate_mcf_day
FROM permit_leases pl
WHERE pl.lease_district = '08'
ORDER BY pl.requested_release_rate_mcf_day DESC NULLS LAST
LIMIT 20;
```

---

[← back to index](README.md)

