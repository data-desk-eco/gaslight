# `monthly_flaring`

Monthly lease-level gas disposition — how much gas each lease reported
flaring/venting (RRC disposition code 04) each month, alongside total
disposed and produced volumes. The reported-flaring time series.

- **Grain:** one row per lease × month with reported flaring
- **Rows:** 639,578
- **Source:** RRC Production Data Query (PDQ)
- **Scope:** Permian (Texas) districts, months with flaring, 2021+.

## Schema

| column | type | description |
| --- | --- | --- |
| `oil_gas_code` | VARCHAR | Oil (O) or Gas (G) lease. |
| `lease_district` | VARCHAR | RRC district (alphanumeric). |
| `lease_number` | VARCHAR | RRC lease number, zero-padded to 6 digits. |
| `lease_name` | VARCHAR | Lease name. |
| `operator_no` | VARCHAR | RRC operator number. |
| `operator_name` | VARCHAR | Reporting operator. |
| `field_name` | VARCHAR | RRC field name. |
| `year` | INTEGER | Production cycle year. |
| `month` | INTEGER | Production cycle month (1–12). |
| `date` | DATE | First day of the cycle month (convenience date). |
| `gas_flared_mcf` | DOUBLE | Gas-well gas flared/vented this month, MCF (disposition code 04). |
| `csgd_flared_mcf` | DOUBLE | Casinghead (oil-well) gas flared/vented this month, MCF. |
| `total_flared_mcf` | DOUBLE | gas_flared_mcf + csgd_flared_mcf, MCF. |
| `total_disposed_mcf` | DOUBLE | Total gas disposed of this month across all disposition codes, MCF. |
| `total_gas_prod_mcf` | DOUBLE | Total gas produced this month, MCF. |

## Caveats

**Flaring months only.** A row exists only for lease-months that reported
flaring/venting; this is not a complete production table. All volumes are
operator-reported. `total_flared_mcf` = `gas_flared_mcf` + `csgd_flared_mcf`.

## Example

```sql
-- monthly reported flaring trend, basin-wide
SELECT date, round(sum(total_flared_mcf)) AS flared_mcf
FROM monthly_flaring
GROUP BY date ORDER BY date;
```

---

[← back to index](README.md)

