# tx-swr32

Dark flaring analysis for the Permian Basin. Matches VIIRS Nightfire satellite flare detections to SWR 32 permitted flare locations and oil/gas lease footprints, then cross-references permit dates and self-reported flaring volumes to identify unpermitted ("dark") flaring.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper
- `scripts/scrape_permit_details.py` — downloads permit detail HTML pages
- `scripts/parse_permit_details.py` — parses HTML to CSVs (permit_details, permit_properties, flare_locations)
- `scripts/download_rrc.py` — downloads EBCDIC files + well shapefiles from RRC MFT (Playwright)
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` (Permian districts 6E/7B/7C/08/8A)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data
- `queries/schema.sql` → `load.sql` → `transform.sql` → `views.sql` — layered SQL pipeline

## Methodology

1. **Dark flaring**: VNF flare sites matched to nearest SWR 32 permit location within 1km. For each detection-day, if any nearby permit covers the date, it's "permitted"; otherwise "dark".
2. **Lease matching**: spatial via `lease_locations` (convex hull of wells per lease, buffered ~500m). VNF sites within a lease footprint (`ST_Contains`) get allocated to that lease. `permit_lease_map` is used separately to calculate per-lease permit coverage, not for lease assignment.
3. **Reported flaring**: PDQ gas disposition data (code 04 = vented/flared) cross-referenced with permit coverage to estimate unpermitted volumes.
4. **Operator attribution**: nearest permit filing operator, with `sole`/`majority`/`contested` confidence levels.
5. **Exclusions**: EPA GHGRP non-upstream facilities within 1.5km; Gas Plant permits filtered out.
6. **Plume attribution**: Carbon Mapper + IMEO methane plumes matched to wells and VNF sites within 1km. Classified as flaring/unlit/wellpad/unmatched.

## Key details

- **EBCDIC districts**: numeric codes mapped to alphanumeric (08→7B, 09→7C, 10→08, 11→8A)
- **Database layout**: `raw.*` holds loaded data; `main.*` has entity tables and views. Re-run transform+views without reloading raw data.
- **Lease footprints**: convex hull of well surface locations from EBCDIC type 13 records, with 0.005° (~500m) buffer. `ST_Contains` for matching.
- **VNF load**: `all_varchar=true` on profile CSVs for speed.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API).
- **Permit coverage**: `permit_lease_map` maps each SWR 32 filing to its underlying leases (including commingle permits with multiple leases). Used to calculate daily permit coverage per lease-month, not for VNF-to-lease assignment.

## Commands

- `make db` — full pipeline (schema → load → transform → views)
- `make refresh` — rebuild DB from scratch
- `make plumes` — fetch latest plume data
- `make clean` — removes derived data
- `duckdb data/dark_flaring.duckdb` — query interactively
