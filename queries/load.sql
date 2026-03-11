LOAD spatial;

-- SWR 32 permits (parse "Oil Lease-08-43066" -> type, district, number)
INSERT INTO raw.permits
SELECT * REPLACE (replace(operator_name, '&amp;', '&') AS operator_name),
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 1) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 2) END,
    CASE WHEN property LIKE '%-%-%' THEN split_part(property, '-', 3) END
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true);

-- Wells (Permian, with locations)
INSERT INTO raw.wells
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- Operators
INSERT INTO raw.operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- Permit details (filing metadata from detail pages)
INSERT INTO raw.permit_details
SELECT * FROM read_csv('data/permit_details.csv', header=true, all_varchar=true);

-- Permit properties (leases/permits per filing from detail pages)
INSERT INTO raw.permit_properties
SELECT * FROM read_csv('data/permit_properties.csv', header=true, all_varchar=true);

-- Flare locations (all permitted flare GPS coordinates, including Gas Plant)
INSERT INTO raw.flare_locations
SELECT fl.*, CASE WHEN fl.latitude != 0 AND fl.longitude != 0
                  THEN ST_Point(fl.longitude, fl.latitude) END
FROM read_csv('data/flare_locations.csv', header=true, auto_detect=true) fl;

-- VNF: pre-aggregated parquet (site × day, nighttime detections, permit era)
INSERT INTO raw.vnf
SELECT flare_id, lat, lon, date, clear, detected, rh_mw, temp_k, n_passes,
    CASE WHEN lat IS NOT NULL THEN ST_Point(lon, lat) END
FROM read_parquet('data/vnf.parquet');

-- PDQ: pre-filtered gas disposition parquet (only rows with flaring/venting)
INSERT INTO raw.gas_disposition
SELECT * FROM read_parquet('data/gas_disposition.parquet');

-- PDQ: lease summary master (for lease name/operator lookups)
INSERT INTO raw.pdq_leases
SELECT
    OIL_GAS_CODE, DISTRICT_NO, LEASE_NO, OPERATOR_NO, FIELD_NO,
    DISTRICT_NAME, LEASE_NAME, OPERATOR_NAME, FIELD_NAME,
    CYCLE_YEAR_MONTH_MIN::VARCHAR, CYCLE_YEAR_MONTH_MAX::VARCHAR
FROM read_csv('data/pdq/OG_SUMMARY_MASTER_LARGE_DATA_TABLE.dsv',
    delim='}', header=true, all_varchar=true, ignore_errors=true);

-- Non-upstream facility exclusion zones (EPA GHGRP)
INSERT INTO raw.excluded_facilities
SELECT *, CASE WHEN latitude != 0 AND longitude != 0
               THEN ST_Point(longitude, latitude) END
FROM read_csv('data/excluded_facilities.csv', header=true, auto_detect=true);

-- Carbon Mapper plumes
INSERT INTO raw.plumes
SELECT
    plume_id,
    'cm' AS source,
    platform AS satellite,
    datetime::DATE AS date,
    plume_latitude::DOUBLE AS latitude,
    plume_longitude::DOUBLE AS longitude,
    emission_auto::DOUBLE AS emission_rate,
    emission_uncertainty_auto::DOUBLE AS emission_rate_uncertainty,
    CASE
        WHEN ipcc_sector ILIKE '%oil%' OR ipcc_sector ILIKE '%gas%' THEN 'og'
        WHEN ipcc_sector ILIKE '%coal%' THEN 'coal'
        WHEN ipcc_sector ILIKE '%waste%' THEN 'waste'
        WHEN ipcc_sector IS NOT NULL AND ipcc_sector != '' THEN 'other'
    END AS sector,
    ST_Point(plume_longitude::DOUBLE, plume_latitude::DOUBLE)
FROM read_csv('data/plumes_cm.csv', header=true, all_varchar=true, quote='"')
WHERE plume_latitude IS NOT NULL AND plume_longitude IS NOT NULL;

-- IMEO plumes (already filtered to Permian by fetch script)
INSERT INTO raw.plumes
SELECT
    plume_id,
    'imeo' AS source,
    satellite,
    date,
    latitude,
    longitude,
    emission_rate,
    emission_uncertainty,
    CASE
        WHEN sector ILIKE '%oil%' OR sector ILIKE '%gas%' OR sector = 'og' THEN 'og'
        WHEN sector ILIKE '%coal%' THEN 'coal'
        WHEN sector ILIKE '%waste%' THEN 'waste'
        WHEN sector IS NOT NULL AND sector != '' THEN 'other'
    END,
    ST_Point(longitude, latitude)
FROM read_csv('data/plumes_imeo.csv', header=true, auto_detect=true)
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- OTLS survey polygons (filtered to Permian bbox)
INSERT INTO raw.surveys
SELECT
    ABSTRACT_N,
    ABSTRACT_L,
    LEVEL1_SUR,
    LEVEL2_BLO,
    LEVEL3_SUR,
    LEFT(ABSTRACT_N, 3),
    geom
FROM ST_Read('data/survALLp.shp')
WHERE ST_XMin(geom) >= -104.5 AND ST_XMax(geom) <= -100.0
  AND ST_YMin(geom) >= 30.0 AND ST_YMax(geom) <= 33.5;

-- Spatial indexes
CREATE INDEX IF NOT EXISTS idx_raw_wells_geom ON raw.wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_vnf_geom ON raw.vnf USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_flare_loc_geom ON raw.flare_locations USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_plumes_geom ON raw.plumes USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_raw_surveys_geom ON raw.surveys USING RTREE (geom);
