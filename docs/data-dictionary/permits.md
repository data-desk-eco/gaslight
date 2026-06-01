# `permits`

Texas RRC Statewide Rule 32 flaring/venting exception permits, one row per
filing, geolocated to the permitted flare. What operators have asked the
state for permission to flare or vent.

- **Grain:** one row per SWR 32 permit filing (`filing_no`)
- **Rows:** 9,815
- **Source:** RRC Statewide Rule 32 flaring/venting permits
- **Scope:** Permitted flare locations inside the Permian (Texas) bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `filing_no` | VARCHAR | RRC filing number (unique permit id). |
| `latitude` | DOUBLE | Permitted flare latitude, decimal degrees WGS84. |
| `longitude` | DOUBLE | Permitted flare longitude, decimal degrees WGS84. |
| `name` | VARCHAR | Site/facility name on the filing. |
| `county` | VARCHAR | Texas county. |
| `district` | VARCHAR | RRC district of the flare location. |
| `release_type` | VARCHAR | Type of release permitted (e.g. flaring, venting). |
| `operator_no` | VARCHAR | RRC operator number of the filer. |
| `operator_name` | VARCHAR | Operator (company) that filed the permit. |
| `property_type` | VARCHAR | Property classification (Oil Lease, Gas Lease, Gas Plant, Drilling Permit, …). |
| `status` | VARCHAR | Filing status (Approved, Denied, Returned, Hearing Pending, Cancelled). |
| `effective_dt` | DATE | Requested effective date of the exception (nullable). |
| `expiration_dt` | DATE | Requested expiration date of the exception (nullable). |
| `release_rate_mcf_day` | DOUBLE | Requested release rate summed across the filing's leases, MCF/day. |
| `exception_reasons` | VARCHAR | Free-text reason(s) the operator gave for the exception. |
| `is_gas_plant` | BOOLEAN | True if the filing is a gas-plant property (excluded from the web map layer). |

## Caveats

Gas-plant filings are **kept** and flagged with `is_gas_plant` (the web map
hides them, but they are leads worth seeing). Dates are the requested
effective/expiration dates parsed from the detail pages; some are null when
the filing did not state them (no imputation).

## Example

```sql
-- approved active permits by operator
SELECT operator_name, count(*) AS filings
FROM permits
WHERE status = 'Approved' AND NOT is_gas_plant
GROUP BY 1 ORDER BY filings DESC LIMIT 20;
```

---

[← back to index](README.md)

