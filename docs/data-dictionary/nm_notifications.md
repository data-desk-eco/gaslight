# `nm_notifications`

New Mexico OCD spill/release incident notifications — the New Mexico
counterpart to the Texas SWR-32 permit and plume layers. Includes routine
flaring and venting notices as well as spills, so the NM Delaware side of
the Permian is covered alongside Texas.

- **Grain:** one row per reported incident (`incident_number` may repeat across materials)
- **Rows:** 209,653
- **Source:** New Mexico OCD spill/release incidents
- **Scope:** Permian bbox, 2021+.

## Schema

| column | type | description |
| --- | --- | --- |
| `incident_number` | VARCHAR | OCD incident identifier. |
| `incident_date` | DATE | Date the incident occurred. |
| `notification_date` | DATE | Date the operator notified the OCD. |
| `incident_type` | VARCHAR | Incident category — 'Flare', 'Vent', 'Vent with Flaring', 'Oil Release', etc. |
| `severity` | VARCHAR | OCD severity classification (Minor / Major). |
| `operator` | VARCHAR | Reporting operator name. |
| `ogrid` | VARCHAR | NM operator (OGRID) number. |
| `facility_name` | VARCHAR | Facility name, where given. |
| `well_name` | VARCHAR | Associated well name, where given. |
| `api` | VARCHAR | Well API number, where given. |
| `material` | VARCHAR | Material released (e.g. 'Natural Gas Vented', 'Crude Oil'). |
| `volume_released` | DOUBLE | Volume released, in `volume_unit`. |
| `volume_unit` | VARCHAR | Unit for the volumes (usually 'Mcf' for gas, 'BBL' for liquids). |
| `cause` | VARCHAR | Reported cause (e.g. 'Equipment Failure', 'Liquids Unloading'). |
| `spill_source` | VARCHAR | Reported source (e.g. 'Well', 'Flow Line - Production'). |
| `lease_type` | VARCHAR | Land/lease type — 'Federal', 'State', 'Private'. |
| `county` | VARCHAR | New Mexico county (name + FIPS). |
| `district` | VARCHAR | OCD district office (Hobbs, Artesia, …). |
| `ulstr` | VARCHAR | Unit-letter / section / township / range legal location. |
| `latitude` | DOUBLE | Incident latitude, decimal degrees WGS84. |
| `longitude` | DOUBLE | Incident longitude, decimal degrees WGS84. |

## Caveats

Self-reported by operators to the NM OCD, like the RRC disposition volumes
on the Texas side — not an independent observation. `incident_type` is
dominated by routine `Flare` notices; `Vent` / `Vent with Flaring` carry the
vented-gas volume in `volume_released` (`volume_unit` usually 'Mcf'). Many
spill rows carry no volume. Coordinates are operator-supplied.

## Example

```sql
SELECT incident_type, count(*) AS n,
  round(sum(volume_released) FILTER (volume_unit ILIKE 'mcf')) AS mcf
FROM nm_notifications GROUP BY incident_type ORDER BY n DESC;
```

---

[← back to index](README.md)

