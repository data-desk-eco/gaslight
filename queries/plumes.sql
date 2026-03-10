LOAD spatial;

-- Carbon Mapper plumes
INSERT INTO plumes
SELECT
    plume_id,
    'cm' AS source,
    platform AS satellite,
    datetime::DATE AS date,
    plume_latitude::DOUBLE AS latitude,
    plume_longitude::DOUBLE AS longitude,
    emission_auto::DOUBLE AS emission_rate,
    emission_uncertainty_auto::DOUBLE AS emission_uncertainty,
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
INSERT INTO plumes
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

CREATE INDEX IF NOT EXISTS idx_plumes_geom ON plumes USING RTREE (geom);

-- Match radius: 1km (same as VNF-to-well matching)
SET VARIABLE plume_well_radius = 0.01;

-- Match each plume to nearest well within 1km (excluding plumes near non-upstream facilities)
CREATE OR REPLACE TEMP TABLE plume_well_candidates AS
SELECT p.plume_id,
       w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
       ST_Distance_Sphere(p.geom, w.geom) / 1000.0 AS distance_km
FROM plumes p
JOIN wells w ON w.geom IS NOT NULL
    AND w.longitude BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND w.latitude  BETWEEN p.latitude  - 0.02 AND p.latitude  + 0.02
    AND ST_DWithin(p.geom, w.geom, getvariable('plume_well_radius'))
WHERE NOT EXISTS (
    SELECT 1 FROM excluded_facilities ef
    WHERE ef.geom IS NOT NULL
      AND ef.longitude BETWEEN p.longitude - 0.015 AND p.longitude + 0.015
      AND ef.latitude  BETWEEN p.latitude  - 0.015 AND p.latitude  + 0.015
);

CREATE OR REPLACE TEMP TABLE plume_nearest_well AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plume_id ORDER BY distance_km) AS rn
    FROM plume_well_candidates
)
WHERE rn = 1;

-- Cross-reference with VNF: find nearest upstream VNF site for each plume (within 1km)
-- Excludes sites near non-upstream facilities (same filter as flaring.sql)
CREATE OR REPLACE TEMP TABLE plume_vnf_site AS
WITH site_geom AS (
    SELECT flare_id, AVG(lat) AS lat, AVG(lon) AS lon, ST_Point(AVG(lon), AVG(lat)) AS geom
    FROM vnf WHERE detected GROUP BY flare_id
    HAVING NOT EXISTS (
        SELECT 1 FROM excluded_facilities ef
        WHERE ef.geom IS NOT NULL
          AND ef.longitude BETWEEN AVG(lon) - 0.015 AND AVG(lon) + 0.015
          AND ef.latitude  BETWEEN AVG(lat) - 0.015 AND AVG(lat) + 0.015
    )
),
candidates AS (
    SELECT p.plume_id, p.date AS plume_date,
           fs.flare_id, fs.lat AS vnf_lat, fs.lon AS vnf_lon,
           ST_Distance_Sphere(p.geom, fs.geom) / 1000.0 AS distance_km
    FROM plumes p
    JOIN site_geom fs
    ON fs.lon BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
       AND fs.lat BETWEEN p.latitude - 0.02 AND p.latitude + 0.02
       AND ST_DWithin(p.geom, fs.geom, 0.01)
)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plume_id ORDER BY distance_km) AS rn
    FROM candidates
) WHERE rn = 1;

-- Check if the VNF site had a detection on the same day as the plume (+-1 day)
CREATE OR REPLACE TEMP TABLE plume_vnf_detection AS
SELECT pvs.plume_id, pvs.flare_id, pvs.distance_km AS vnf_distance_km,
       v.date AS vnf_date, v.rh_mw, v.temp_k,
       CASE WHEN v.date IS NOT NULL THEN true ELSE false END AS flare_detected
FROM plume_vnf_site pvs
LEFT JOIN vnf v ON v.flare_id = pvs.flare_id
    AND v.detected
    AND v.date BETWEEN pvs.plume_date - INTERVAL 1 DAY AND pvs.plume_date + INTERVAL 1 DAY;

-- Deduplicate (one row per plume, prefer detected)
CREATE OR REPLACE TEMP TABLE plume_vnf AS
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plume_id ORDER BY flare_detected DESC) AS rn
    FROM plume_vnf_detection
) WHERE rn = 1;

-- Final attributed plumes table
CREATE OR REPLACE TABLE plume_attributed AS
SELECT
    p.plume_id, p.source, p.satellite, p.date, p.latitude, p.longitude,
    p.emission_rate, p.emission_uncertainty, p.sector,
    pw.api, pw.oil_gas_code, pw.lease_district, pw.lease_number, pw.operator_no,
    pw.distance_km AS well_distance_km,
    o.operator_name,
    pv.flare_id AS vnf_flare_id,
    pv.vnf_distance_km,
    COALESCE(pv.flare_detected, false) AS flare_detected,
    pv.rh_mw AS vnf_rh_mw,
    CASE
        WHEN pv.flare_id IS NOT NULL AND NOT COALESCE(pv.flare_detected, false) THEN 'unlit'
        WHEN pv.flare_id IS NOT NULL AND COALESCE(pv.flare_detected, false) THEN 'flaring'
        WHEN pw.api IS NOT NULL THEN 'wellpad'
        ELSE 'unmatched'
    END AS classification
FROM plumes p
LEFT JOIN plume_nearest_well pw USING (plume_id)
LEFT JOIN plume_vnf pv USING (plume_id)
LEFT JOIN operators o ON LPAD(o.operator_number, 6, '0') = LPAD(pw.operator_no, 6, '0');

CREATE OR REPLACE VIEW plume_summary AS
SELECT classification, source,
    count(*) AS plume_count,
    count(DISTINCT COALESCE(api, plume_id)) AS sites,
    round(avg(emission_rate), 1) AS avg_emission_rate,
    round(sum(emission_rate), 0) AS total_emission_rate,
    min(date) AS earliest, max(date) AS latest
FROM plume_attributed
GROUP BY classification, source
ORDER BY classification, source;

CREATE OR REPLACE VIEW unlit_flares AS
SELECT plume_id, date, latitude, longitude, emission_rate, emission_uncertainty,
    source, satellite, operator_name, api, vnf_flare_id, vnf_distance_km,
    well_distance_km
FROM plume_attributed
WHERE classification = 'unlit'
ORDER BY emission_rate DESC;

CREATE OR REPLACE VIEW top_plume_operators AS
SELECT COALESCE(operator_name, 'Unknown') AS operator,
    count(*) AS plume_count,
    count(DISTINCT api) AS well_sites,
    round(sum(emission_rate), 0) AS total_emission_kg_hr,
    round(avg(emission_rate), 1) AS avg_emission_kg_hr,
    sum(CASE WHEN classification = 'unlit' THEN 1 ELSE 0 END) AS unlit_count,
    sum(CASE WHEN classification = 'flaring' THEN 1 ELSE 0 END) AS flaring_count
FROM plume_attributed
WHERE api IS NOT NULL
GROUP BY 1
ORDER BY total_emission_kg_hr DESC;
