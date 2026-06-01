# `facilities`

RRC R-3 gas processing facilities (gas plants). Used to distinguish
plant-related flares from wellsite flaring, and useful context for where
gathered gas is processed.

- **Grain:** one row per gas processing facility (`serial_number`)
- **Rows:** 596
- **Source:** RRC R-3 gas processing facilities
- **Scope:** Permian bbox.

## Schema

| column | type | description |
| --- | --- | --- |
| `serial_number` | VARCHAR | RRC facility serial number. |
| `facility_name` | VARCHAR | Facility (plant) name. |
| `plant_type` | VARCHAR | Type of processing plant. |
| `latitude` | DOUBLE | Facility latitude, decimal degrees WGS84. |
| `longitude` | DOUBLE | Facility longitude, decimal degrees WGS84. |

## Caveats

Coordinates are facility points. A flare within ~5 km of a plant is more
likely plant-related than wellsite flaring.

## Example

```sql
SELECT facility_name, plant_type, latitude, longitude FROM facilities;
```

---

[← back to index](README.md)

