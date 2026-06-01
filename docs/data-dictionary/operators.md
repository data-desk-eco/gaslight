# `operators`

RRC operator-number → name lookup. Reference table for resolving operator
numbers found elsewhere (wells, permits, gatherers).

- **Grain:** one row per RRC operator number
- **Rows:** 77,888
- **Source:** RRC well, operator, and P-4 gatherer records
- **Scope:** RRC-wide (statewide) — the one table not geographically clipped.

## Schema

| column | type | description |
| --- | --- | --- |
| `operator_no` | VARCHAR | RRC operator number. |
| `operator_name` | VARCHAR | Operator (company) name. |
| `status` | VARCHAR | RRC operator status. |

## Caveats

Statewide, not Permian-only — it is a name lookup. `status` reflects the
operator's RRC standing.

## Example

```sql
SELECT * FROM operators WHERE operator_name ILIKE '%diamondback%';
```

---

[← back to index](README.md)

