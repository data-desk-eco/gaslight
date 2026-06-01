# `gatherers`

Who moves and buys each lease's gas — the gatherers, purchasers, and
nominators reported on RRC P-4 forms. Connects a flaring lease to the
midstream companies handling its gas.

- **Grain:** one row per lease × role × entity (current/historical)
- **Rows:** 1,690,145
- **Source:** RRC well, operator, and P-4 gatherer records
- **Scope:** Permian (Texas) district leases.

## Schema

| column | type | description |
| --- | --- | --- |
| `oil_gas_code` | VARCHAR | Oil (O) or Gas (G) lease. |
| `district` | VARCHAR | RRC district (alphanumeric). |
| `lease_number` | VARCHAR | RRC lease number, zero-padded to 6 digits. |
| `type` | VARCHAR | Role of the entity — Gatherer, Purchaser, or Nominator. |
| `percentage` | DOUBLE | Share attributed to this entity, percent. |
| `gpn_number` | VARCHAR | Gatherer/Purchaser/Nominator P-5 organisation number. |
| `gpn_name` | VARCHAR | Entity name (resolved from the org number; fallback "Unknown (number)"). |
| `is_current` | VARCHAR | String 'true'/'false' — whether this is the current arrangement. |
| `first_date` | VARCHAR | Earliest effective date seen for this entity on the lease (nullable). |
| `last_date` | VARCHAR | Latest effective date seen (nullable). |

## Caveats

Roles: Gatherer, Purchaser, Nominator. `is_current` distinguishes the active
arrangement from historical ones. `gpn_name` falls back to "Unknown (number)"
when the org number does not resolve to a known operator.

## Example

```sql
SELECT type, gpn_name, percentage, is_current, first_date, last_date
FROM gatherers
WHERE district = '08' AND lease_number = '000001'
ORDER BY is_current DESC, last_date DESC NULLS LAST;
```

---

[← back to index](README.md)

