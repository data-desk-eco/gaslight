# Permian Basin Flaring Dataset

*Satellite-observed flaring, state permits, production, and methane plumes*

This is the data behind *gaslight*, an analysis of natural-gas flaring in
the Permian Basin of West Texas. It links four independent kinds of evidence
about where and how much gas is being burned or vented:

1. **What satellites see** — VIIRS Nightfire infrared flare detections and
   Sentinel-2 shortwave-infrared detections.
2. **What operators are permitted to do** — Texas Railroad Commission (RRC)
   Statewide Rule 32 flaring/venting exception permits.
3. **What operators report** — RRC monthly lease-level gas disposition
   (how much gas was flared/vented) and production volumes, plus the wells,
   operators, and gas gatherers/purchasers tied to each lease.
4. **What methane sensors detect** — Carbon Mapper and UNEP IMEO plume
   observations.

The dataset is a single DuckDB database, `gaslight.duckdb`. Every table is a
plain, documented table — open it with the free DuckDB binary (or read the
tables straight into pandas/R/QGIS) and start querying.

> [!IMPORTANT]
> **Reported vs. observed flaring — read this first.** The flaring *volumes*
> in this dataset (`monthly_flaring`, and the `flared_mcf` / `flaring_intensity_pct`
> columns derived from them) are **self-reported by operators to the RRC**.
> The satellite layers (`flare_sites`, `flare_detections`, `s2_detections`)
> are **independent observations** of combustion from space. These two do not
> always agree — a flare can burn brightly on satellite imagery in a month an
> operator reports little or no flaring. That gap is the central finding of the
> reporting this dataset supports; treat the two families of evidence as
> *separate*, not interchangeable.

## Scope

**Geography.** Everything is clipped to the Permian Basin bounding box:
latitude 30.0–33.5°N, longitude -104.5 to -100.0°W. Above 32°N, points must
sit east of -103.064°W (the Texas–New Mexico border) so the dataset is
Texas-only. The one exception is `operators`, an RRC-wide name lookup with no
geography.

**Time.** Time-series tables (`flare_detections`, `monthly_flaring`) and the
rollups derived from them cover **2021-01-01 onward**, matching the satellite
analysis window. Longer RRC history exists upstream and the window is a single
pipeline setting that can be widened on request.

## Method notes

- **375 m spatial matching.** Flares are linked to wells/permits using a fixed
  375 m radius — the half-width of a VIIRS M-band pixel (750 m). A ±0.0034°
  lat/lon bounding box is the cheap pre-filter.
- **No imputation.** Missing values stay missing. Nothing is modelled, filled,
  or guessed; if a permit has no effective date or a lease has no production in
  a month, the cell is null/absent.
- **Coordinates** are decimal degrees, WGS84. There is no geometry/WKB column —
  use `latitude`/`longitude` directly.
- **Lease numbers** are zero-padded to 6 digits everywhere, so `lease_district`
  + `lease_number` joins cleanly across tables.

## How to use

Install DuckDB (a single dependency-free binary, <https://duckdb.org/docs/installation>)
and open the file:

```sh
duckdb gaslight.duckdb
```

```sql
-- list tables
SHOW TABLES;
-- in-database column dictionary
SELECT * FROM _dictionary WHERE table_name = 'wells';
-- the brightest flare sites by total radiant heat
SELECT flare_id, lat, lon, detection_days, total_rh_mw
FROM flare_sites ORDER BY total_rh_mw DESC LIMIT 20;
```

Prefer another tool? Every table also reads directly into pandas
(`duckdb.sql("SELECT * FROM flare_sites").df()`), R, or QGIS (via the
latitude/longitude columns).

## Tables

| table | grain | rows | what it is |
| --- | --- | --- | --- |
| [`flare_sites`](flare_sites.md) | one row per VNF flare site (`flare_id`) | 1,297 | Satellite-observed flare locations and how active each has been. |
| [`flare_detections`](flare_detections.md) | one row per flare site × detected night | 205,969 | Per-night detection time series behind each flare site. |
| [`permits`](permits.md) | one row per SWR 32 permit filing (`filing_no`) | 9,815 | RRC SWR 32 flaring/venting exception permits, geolocated. |
| [`permit_leases`](permit_leases.md) | one row per filing × underlying lease | 80,217 | The leases underlying each SWR 32 permit filing. |
| [`wells`](wells.md) | one row per well (API) | 324,692 | Every Permian well, with operator and its lease's flaring metrics. |
| [`leases`](leases.md) | one row per lease (`lease_district` + `lease_number`) | 28,219 | Lease-level rollup of reported flaring, intensity, and well count. |
| [`monthly_flaring`](monthly_flaring.md) | one row per lease × month with reported flaring | 639,578 | Monthly lease-level reported gas flared/vented and produced. |
| [`gatherers`](gatherers.md) | one row per lease × role × entity (current/historical) | 1,690,145 | Gatherers, purchasers, and nominators handling each lease's gas. |
| [`plumes`](plumes.md) | one row per plume observation (`plume_id`) | 6,581 | Methane plume detections (Carbon Mapper + UNEP IMEO). |
| [`facilities`](facilities.md) | one row per gas processing facility (`serial_number`) | 596 | RRC R-3 gas processing facilities (gas plants). |
| [`operators`](operators.md) | one row per RRC operator number | 77,888 | RRC operator-number → name reference lookup. |
| [`s2_detections`](s2_detections.md) | one row per Sentinel-2 detection (member of a cluster) | 7,241 | Sentinel-2 SWIR flare detections, clustered (refresh pending). |

## Sources & attribution

- **VIIRS Nightfire (VNF)** — Earth Observation Group, Payne Institute, Colorado School of Mines. Free for use with attribution to the Earth Observation Group. <https://eogdata.mines.edu/products/vnf/>
- **Sentinel-2 SWIR flare detection** — Copernicus / ESA imagery, processed by the s2-flares library. Copernicus Sentinel data, free and open. Detections are derived products. <https://dataspace.copernicus.eu/>
- **RRC Statewide Rule 32 flaring/venting permits** — Texas Railroad Commission. Texas public records. <https://www.rrc.texas.gov/>
- **RRC well, operator, and P-4 gatherer records** — Texas Railroad Commission (Mainframe File Transfer, EBCDIC). Texas public records. <https://www.rrc.texas.gov/resource-center/research/data-sets-available-for-download/>
- **RRC Production Data Query (PDQ)** — Texas Railroad Commission. Texas public records. <https://www.rrc.texas.gov/resource-center/research/online-research-queries/>
- **RRC R-3 gas processing facilities** — Texas Railroad Commission. Texas public records. <https://www.rrc.texas.gov/>
- **Carbon Mapper methane plumes** — Carbon Mapper. CC BY 4.0 (verify current terms before redistribution). <https://carbonmapper.org/>
- **UNEP IMEO methane plumes** — UN Environment Programme — International Methane Emissions Observatory. Public; attribute UNEP IMEO. <https://methanedata.unep.org/>

All RRC data are Texas public records. Satellite and methane layers are free/open with attribution as noted — verify current provider terms before republishing derived products.

*This dictionary is generated from `_meta.yaml`; the same content populates the `_dictionary` and `_sources` tables inside the database.*

