# Methane plume integration

## Goal

Add satellite methane plume data to the dark flaring analysis to identify **venting** (unlit flares) -- sites where methane is being released without combustion, which is worse for the climate than flaring.

## Data sources

- **Carbon Mapper** (`api.carbonmapper.org`): Airborne/satellite hyperspectral methane plume detections. Fetched via paginated CSV API with Permian bbox filter.
- **IMEO/UNEP MARS** (local GeoJSON): Global methane plume database from multiple satellites (TROPOMI, etc.). Filtered to Permian Basin bbox at fetch time.

## Methodology

1. **Fetch**: Download CM plumes for Permian bbox; filter IMEO GeoJSON to Permian. Output two CSVs.
2. **Load**: Insert both into unified `plumes` table with normalized schema.
3. **Well matching**: Spatial join each plume to nearest RRC well within 1km (same radius as VNF matching).
4. **VNF cross-reference**: For each plume, find nearest VNF flare site within 1km. Check if that site had a thermal detection within +-1 day of the plume observation.
5. **Classification**:
   - `unlit` -- plume near a VNF site with NO concurrent flare detection (venting)
   - `flaring` -- plume near a VNF site WITH a concurrent detection (incomplete combustion)
   - `wellpad` -- plume matched to a well but not near any VNF site
   - `unmatched` -- no well or VNF match

## Tables and views

- `plumes` -- raw plume detections from both sources
- `plume_attributed` -- plumes with well match, operator, VNF cross-reference, and classification
- `plume_summary` -- counts and emissions by classification and source
- `unlit_flares` -- detail view of potential unlit flares
- `top_plume_operators` -- operators ranked by total methane emissions

## Pipeline

```
scripts/fetch_plumes.py  ->  data/plumes_cm.csv, data/plumes_imeo.csv
queries/plume_schema.sql ->  CREATE TABLE plumes
queries/load_plumes.sql  ->  INSERT INTO plumes (from both CSVs)
queries/plume_analysis.sql -> plume_attributed + views
```

Integrated into `make db` after `dark_flaring.sql`.
