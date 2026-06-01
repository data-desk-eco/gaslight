# `s2_detections`

Sentinel-2 shortwave-infrared (SWIR) flare detections, clustered into
persistent flare locations. A higher-spatial-resolution complement to VIIRS
Nightfire, better at separating nearby flares.

- **Grain:** one row per Sentinel-2 detection (member of a cluster)
- **Rows:** 7,241
- **Source:** Sentinel-2 SWIR flare detection
- **Scope:** Permian (Texas) clip; quality-filtered detections.

## Schema

| column | type | description |
| --- | --- | --- |
| `cluster_id` | VARCHAR | Identifier of the persistent flare cluster this detection belongs to. |
| `date` | VARCHAR | Detection date (Sentinel-2 acquisition). |
| `max_b12` | DOUBLE | Peak B12 (SWIR-2) reflectance of the detection. |
| `pixels` | BIGINT | Number of hot pixels in the detection. |
| `det_lon` | DOUBLE | Detection longitude, decimal degrees WGS84. |
| `det_lat` | DOUBLE | Detection latitude, decimal degrees WGS84. |
| `cluster_lon` | DOUBLE | Cluster anchor longitude, decimal degrees WGS84. |
| `cluster_lat` | DOUBLE | Cluster anchor latitude, decimal degrees WGS84. |
| `cluster_max_b12` | DOUBLE | Maximum B12 reflectance across the cluster. |
| `cluster_avg_b12` | DOUBLE | Mean B12 reflectance across the cluster. |
| `cluster_date_count` | BIGINT | Number of distinct dates the cluster was detected (persistence count). |
| `cluster_persistence` | DOUBLE | Fraction of clear acquisitions on which the cluster was detected. |
| `cluster_seasonal` | BOOLEAN | Whether the cluster's activity looks seasonal. |

## Caveats

**Pending refresh.** These detections use the current s2-flares method and
will be regenerated with the latest openflaring method in a following update.
`b12`/`b11` are SWIR band reflectances; `cluster_*` columns describe the
persistent flare cluster a detection belongs to.

## Example

```sql
-- most persistent S2 flare clusters
SELECT DISTINCT cluster_id, cluster_lat, cluster_lon, cluster_date_count, cluster_max_b12
FROM s2_detections
ORDER BY cluster_date_count DESC
LIMIT 25;
```

---

[← back to index](README.md)

