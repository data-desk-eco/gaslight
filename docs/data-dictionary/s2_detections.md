# `s2_detections`

Sentinel-2 shortwave-infrared (SWIR) flare sites — a higher-spatial-
resolution complement to VIIRS Nightfire, better at separating nearby
flares. One row per detected site (clustered to an H3 cell), with the
site's per-date observations carried in the `detections` array.

- **Grain:** one row per H3 flare site
- **Rows:** 3,260
- **Source:** Sentinel-2 SWIR flare detection
- **Scope:** The Texas clip of permian-flaring's published catalogue (its top 3,000
sites by detection score, basin-wide; gaslight keeps only the Texas ones).
2025-01-01 to 2026-05-31 window (the Sentinel-2 archive used by
permian-flaring), unlike the other satellite layers which are 2021+.

## Schema

| column | type | description |
| --- | --- | --- |
| `h3` | VARCHAR | H3 cell id (resolution 11) identifying the flare site; stable across rebuilds. |
| `lat` | DOUBLE | Site latitude, decimal degrees WGS84 (detection centroid). |
| `lon` | DOUBLE | Site longitude, decimal degrees WGS84 (detection centroid). |
| `n_detections` | BIGINT | Number of Sentinel-2 acquisitions the site was detected on. |
| `n_dates` | BIGINT | Number of distinct dates detected (<= n_detections). |
| `n_clear_obs` | BIGINT | Number of distinct dates Sentinel-2 got a cloud-free look at the site (the persistence denominator, from the SCL coverage pass). |
| `persistence` | DOUBLE | Fraction of clear-sky looks the site was flaring (n_dates / n_clear_obs, 0-1); high = a steady flare, low = sporadic. |
| `first_date` | DATE | First detection date. |
| `last_date` | DATE | Most recent detection date. |
| `max_b12` | DOUBLE | Peak B12 (SWIR-2) reflectance across all detections. |
| `mean_max_b12` | DOUBLE | Mean of the per-detection peak B12 reflectance. |
| `b12_b11_ratio` | DOUBLE | Peak B12/B11 reflectance ratio (a temperature proxy; >~1.15 indicates true combustion). |
| `min_glint_score` | DOUBLE | Lowest sun-glint score across detections (low = unlikely glint). |
| `total_score` | DOUBLE | Composite detection-quality score used to rank the catalogue. |
| `corroborated` | BOOLEAN | Whether the site is corroborated by independent evidence (see nearest_source). |
| `nearest_source` | VARCHAR | Nearest corroborating source (e.g. VNF, RRC, Carbon Mapper, OSM), or 'uncorroborated'. |
| `detections` | JSON | JSON array of per-date observations -- '{date, max_b12, pixels}' objects. |

## Caveats

Detected and scored by the sibling **permian-flaring** project (the S2
detection engine), not by gaslight; gaslight consumes the ranked catalogue
as a display layer. `corroborated` means a site sits near independent
evidence (VIIRS, RRC permits/wells, a Carbon Mapper plume, or OSM flare
infrastructure) — see `nearest_source`. `b12`/`b11` are SWIR reflectances;
a real flare's peak B12/B11 ratio exceeds ~1.15 (lower can be sun glint).
`detections` is a JSON array of `{date, max_b12, pixels}` objects, one per
acquisition the site was seen on.

## Example

```sql
-- brightest, most-detected S2 flare sites
SELECT h3, lat, lon, n_dates, n_detections, max_b12, corroborated, nearest_source
FROM s2_detections
ORDER BY total_score DESC
LIMIT 25;
```

---

[← back to index](README.md)

