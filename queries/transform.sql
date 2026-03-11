LOAD spatial;

-- ============================================================
-- Entity tables: flare sites, detections, spatial matches
-- ============================================================

-- Match radii (meters, for ST_Distance_Sphere)
SET VARIABLE permit_radius = 1000;    -- 1km for VNF ↔ permit location
SET VARIABLE plume_radius = 1000;     -- 1km for plume ↔ well/VNF
SET VARIABLE start_date = '2021-01-01'::DATE;

-- Flare sites: one row per VNF site with exclusion flag
CREATE OR REPLACE TABLE flare_sites AS
SELECT
    f.flare_id,
    f.lat,
    f.lon,
    f.geom,
    f.first_detected,
    f.last_detected,
    f.detection_days,
    EXISTS (
        SELECT 1 FROM raw.excluded_facilities ef
        WHERE ef.geom IS NOT NULL
          AND ef.longitude BETWEEN f.lon - 0.015 AND f.lon + 0.015
          AND ef.latitude  BETWEEN f.lat - 0.015 AND f.lat + 0.015
    ) AS near_excluded_facility
FROM (
    SELECT flare_id,
        AVG(lat) AS lat, AVG(lon) AS lon,
        ST_Point(AVG(lon), AVG(lat)) AS geom,
        MIN(date) AS first_detected, MAX(date) AS last_detected,
        COUNT(*) AS detection_days
    FROM raw.vnf WHERE detected AND date >= getvariable('start_date')
    GROUP BY flare_id
) f;

CREATE INDEX IF NOT EXISTS idx_flare_sites_geom ON flare_sites USING RTREE (geom);

-- Upstream flare locations (exclude Gas Plant permits, Gas Plant facility types, and non-Permian)
CREATE OR REPLACE TABLE flare_locations AS
SELECT fl.*
FROM raw.flare_locations fl
WHERE fl.filing_no::VARCHAR NOT IN (
    SELECT filing_no FROM raw.permits WHERE property_type = 'Gas Plant'
)
AND COALESCE(fl.facility_type, '') NOT ILIKE '%gas plant%'
AND fl.latitude BETWEEN 30.0 AND 33.5
AND fl.longitude BETWEEN -104.5 AND -100.0;

-- Permit lease map: flatten permit_properties to all underlying leases per filing
-- For commingle permits, this maps the commingle filing to its oil/gas leases
-- For non-commingle filings, this maps the filing to its single lease
CREATE OR REPLACE TABLE permit_lease_map AS
SELECT
    pp.filing_no,
    pp.property_type,
    pp.district AS lease_district,
    pp.property_id AS lease_number,
    pp.lease_name,
    pp.requested_release_rate_mcf_day
FROM raw.permit_properties pp
WHERE pp.property_type IN ('Oil Lease', 'Gas Lease', 'Drilling Permit')
  AND pp.property_id IS NOT NULL AND pp.property_id != '';

CREATE INDEX IF NOT EXISTS idx_flare_locations_geom ON flare_locations USING RTREE (geom);

-- ============================================================
-- Spatial matching: flare sites ↔ permit locations
-- ============================================================

-- All matches within 1km (not just nearest)
CREATE OR REPLACE TABLE site_permit_matches AS
SELECT
    f.flare_id,
    fl.filing_no,
    ST_Distance_Sphere(f.geom, fl.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY f.flare_id ORDER BY ST_Distance_Sphere(f.geom, fl.geom)) AS rank
FROM flare_sites f
JOIN flare_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.03 AND f.lon + 0.03
    AND fl.latitude  BETWEEN f.lat - 0.03 AND f.lat + 0.03
    AND ST_Distance_Sphere(f.geom, fl.geom) < getvariable('permit_radius')
WHERE NOT f.near_excluded_facility;

-- Permit coverage: which permits cover which sites, with parsed dates
-- Uses permit_details when available for richer metadata, falls back to raw.permits
-- Includes Submitted/Hearing Pending (benefit of the doubt per Earthworks methodology)
CREATE OR REPLACE TABLE site_permit_coverage AS
SELECT
    sm.flare_id,
    sm.filing_no,
    sm.distance_km,
    sm.rank,
    COALESCE(pd.operator, p.operator_name) AS operator_name,
    p.operator_no,
    p.property,
    p.property_type,
    p.lease_district,
    p.lease_number,
    COALESCE(pd.exception_status, p.status) AS permit_status,
    COALESCE(
        TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'),
        TRY_STRPTIME(p.effective_dt, '%m/%d/%Y')
    )::DATE AS effective_dt,
    COALESCE(
        TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'),
        TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y')
    )::DATE AS expiration_dt,
    pd.filing_type,
    pd.site_name,
    pd.exception_reasons
FROM site_permit_matches sm
JOIN raw.permits p ON p.filing_no = sm.filing_no
LEFT JOIN raw.permit_details pd ON pd.filing_no = sm.filing_no
WHERE COALESCE(pd.exception_status, p.status) IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
  AND p.property_type != 'Gas Plant';

-- Operator attribution per site (from nearest permit location)
CREATE OR REPLACE TABLE site_operators AS
WITH nearest AS (
    SELECT flare_id, filing_no, distance_km, operator_name, operator_no, lease_district
    FROM site_permit_coverage
    WHERE rank = 1
),
nearby_ops AS (
    SELECT
        sm.flare_id,
        n.operator_name AS attributed_operator,
        p.operator_name AS nearby_operator,
        COUNT(DISTINCT sm.filing_no) AS n_permits
    FROM site_permit_matches sm
    JOIN raw.permits p ON p.filing_no = sm.filing_no
    JOIN nearest n USING (flare_id)
    GROUP BY 1, 2, 3
),
agg AS (
    SELECT flare_id, attributed_operator,
        COUNT(DISTINCT nearby_operator) AS n_operators,
        SUM(CASE WHEN nearby_operator = attributed_operator THEN n_permits ELSE 0 END) * 1.0
          / SUM(n_permits) AS own_share
    FROM nearby_ops GROUP BY 1, 2
)
SELECT
    n.flare_id,
    n.operator_name,
    n.operator_no,
    n.filing_no AS nearest_filing_no,
    n.distance_km AS nearest_permit_km,
    n.lease_district,
    CASE
        WHEN a.n_operators = 1 THEN 'sole'
        WHEN a.own_share > 0.5 THEN 'majority'
        ELSE 'contested'
    END AS confidence
FROM nearest n
LEFT JOIN agg a USING (flare_id);

-- ============================================================
-- Reported flaring from production reports (PDQ disposition data)
-- ============================================================

-- PDQ district_no → RRC district ID mapping
CREATE OR REPLACE TABLE pdq_district_map AS
SELECT * FROM (VALUES
    ('01', '01'), ('02', '02'), ('03', '03'), ('04', '04'),
    ('05', '05'), ('06', '06'), ('07', '6E'), ('08', '7B'),
    ('09', '7C'), ('10', '08'), ('11', '8A'), ('12', '8B'),
    ('13', '09'), ('14', '10')
) AS t(pdq_district, rrc_district);

-- Monthly reported flaring by lease, with district mapping
CREATE OR REPLACE TABLE reported_flaring AS
SELECT
    gd.oil_gas_code,
    dm.rrc_district AS district,
    gd.lease_no,
    gd.cycle_year::INT AS year,
    gd.cycle_month::INT AS month,
    gd.operator_no,
    gd.operator_name,
    gd.lease_name,
    gd.field_name,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) AS gas_flared_mcf,
    COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS csgd_flared_mcf,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) + COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS total_flared_mcf,
    COALESCE(gd.lease_gas_total_vol, 0) + COALESCE(gd.lease_csgd_total_vol, 0) AS total_gas_prod_mcf
FROM raw.gas_disposition gd
LEFT JOIN pdq_district_map dm ON dm.pdq_district = gd.district_no;

-- ============================================================
-- Lease locations from OTLS survey polygons
-- ============================================================

-- Well → OTLS survey spatial join: each well falls within a survey polygon
CREATE OR REPLACE TABLE well_surveys AS
SELECT DISTINCT
    w.oil_gas_code, w.lease_district, w.lease_number, w.api,
    s.abstract_n, s.abstract_l, s.survey_name, s.block, s.section
FROM raw.wells w
JOIN raw.surveys s
    ON ST_Contains(s.geom, ST_Point(w.longitude, w.latitude))
WHERE w.latitude != 0 AND w.longitude != 0;

-- Lease boundaries: union of OTLS survey polygons containing each lease's wells
-- Filtered to max 10km extent to exclude leases with mismatched wells
CREATE OR REPLACE TABLE lease_locations AS
WITH lease_surveys AS (
    SELECT DISTINCT
        ws.oil_gas_code, ws.lease_district, ws.lease_number,
        ws.abstract_n, s.geom
    FROM well_surveys ws
    JOIN raw.surveys s ON s.abstract_n = ws.abstract_n
),
agg AS (
    SELECT oil_gas_code, lease_district, lease_number,
        COUNT(DISTINCT abstract_n) AS survey_count,
        ST_Union_Agg(geom) AS geom
    FROM lease_surveys
    GROUP BY oil_gas_code, lease_district, lease_number
)
SELECT agg.oil_gas_code, agg.lease_district, agg.lease_number,
    agg.survey_count, wc.well_count, agg.geom
FROM agg
JOIN (
    SELECT oil_gas_code, lease_district, lease_number, count(*) AS well_count
    FROM raw.wells WHERE latitude != 0
    GROUP BY 1, 2, 3
) wc USING (oil_gas_code, lease_district, lease_number)
WHERE greatest(ST_XMax(agg.geom) - ST_XMin(agg.geom),
               ST_YMax(agg.geom) - ST_YMin(agg.geom)) * 111 < 10;

CREATE INDEX IF NOT EXISTS idx_lease_locations_geom ON lease_locations USING RTREE (geom);

-- VNF site ↔ lease spatial matches: flare site within OTLS lease boundary
CREATE OR REPLACE TABLE site_lease_matches AS
SELECT
    fs.flare_id,
    ll.lease_district,
    ll.lease_number,
    ll.oil_gas_code,
    ll.well_count
FROM flare_sites fs
JOIN lease_locations ll
    ON ll.geom IS NOT NULL
    AND fs.lon BETWEEN ST_XMin(ll.geom) AND ST_XMax(ll.geom)
    AND fs.lat BETWEEN ST_YMin(ll.geom) AND ST_YMax(ll.geom)
    AND ST_Contains(ll.geom, fs.geom)
WHERE NOT fs.near_excluded_facility;

-- ============================================================
-- Spatial matching: plumes ↔ wells and VNF sites
-- ============================================================

-- Plume ↔ well matches within 1km (excluding plumes near non-upstream facilities)
CREATE OR REPLACE TABLE plume_well_matches AS
SELECT
    p.plume_id,
    w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
    ST_Distance_Sphere(p.geom, w.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, w.geom)) AS rank
FROM raw.plumes p
JOIN raw.wells w ON w.geom IS NOT NULL
    AND w.longitude BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND w.latitude  BETWEEN p.latitude  - 0.02 AND p.latitude  + 0.02
    AND ST_Distance_Sphere(p.geom, w.geom) < getvariable('plume_radius')
WHERE NOT EXISTS (
    SELECT 1 FROM raw.excluded_facilities ef
    WHERE ef.geom IS NOT NULL
      AND ef.longitude BETWEEN p.longitude - 0.015 AND p.longitude + 0.015
      AND ef.latitude  BETWEEN p.latitude  - 0.015 AND p.latitude  + 0.015
);

-- Plume ↔ VNF site matches within 1km (excluding sites near non-upstream facilities)
CREATE OR REPLACE TABLE plume_site_matches AS
SELECT
    p.plume_id,
    p.date AS plume_date,
    fs.flare_id,
    fs.lat AS vnf_lat, fs.lon AS vnf_lon,
    ST_Distance_Sphere(p.geom, fs.geom) / 1000.0 AS distance_km,
    ROW_NUMBER() OVER (PARTITION BY p.plume_id ORDER BY ST_Distance_Sphere(p.geom, fs.geom)) AS rank
FROM raw.plumes p
JOIN flare_sites fs
    ON fs.lon BETWEEN p.longitude - 0.02 AND p.longitude + 0.02
    AND fs.lat BETWEEN p.latitude - 0.02 AND p.latitude + 0.02
    AND ST_Distance_Sphere(p.geom, fs.geom) < getvariable('plume_radius')
WHERE NOT fs.near_excluded_facility;
