LOAD spatial;

-- Distinct flare site locations (one row per flare_id)
CREATE OR REPLACE TEMP TABLE flare_sites AS
SELECT flare_id, AVG(lat) AS lat, AVG(lon) AS lon, ST_Point(AVG(lon), AVG(lat)) AS geom
FROM vnf WHERE detected GROUP BY flare_id;

-- Match each flare site to nearest well within ~750m
CREATE OR REPLACE TEMP TABLE site_well_match AS
WITH candidates AS (
    SELECT f.flare_id,
           w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number, w.operator_no,
           ST_Distance_Sphere(f.geom, w.geom) / 1000.0 AS distance_km
    FROM flare_sites f
    JOIN wells w ON w.geom IS NOT NULL
        AND w.longitude BETWEEN f.lon - 0.015 AND f.lon + 0.015
        AND w.latitude  BETWEEN f.lat - 0.015 AND f.lat + 0.015
        AND ST_DWithin(f.geom, w.geom, 0.0075)
), nearest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY flare_id ORDER BY distance_km) AS rn
    FROM candidates
)
SELECT * EXCLUDE (rn) FROM nearest WHERE rn = 1;

-- Match each flare site to nearest permitted flare location within ~750m
CREATE OR REPLACE TEMP TABLE site_flare_loc_match AS
SELECT DISTINCT f.flare_id, fl.filing_no
FROM flare_sites f
JOIN flare_locations fl ON fl.geom IS NOT NULL
    AND fl.longitude BETWEEN f.lon - 0.015 AND f.lon + 0.015
    AND fl.latitude  BETWEEN f.lat - 0.015 AND f.lat + 0.015
    AND ST_DWithin(f.geom, fl.geom, 0.0075);

-- Expand site matches to daily detections
CREATE OR REPLACE TABLE vnf_matched AS
SELECT v.flare_id, v.date, v.rh_mw, v.temp_k, v.lat AS vnf_lat, v.lon AS vnf_lon,
       sw.api, sw.oil_gas_code, sw.lease_district, sw.lease_number, sw.well_number,
       sw.operator_no, sw.distance_km
FROM vnf v
JOIN site_well_match sw USING (flare_id)
WHERE v.detected;

-- District mapping: wells use numeric (07,08), permits use alphanumeric (7C,8A,08)
CREATE OR REPLACE MACRO district_match(well_d, permit_d) AS
    permit_d = well_d
    OR (well_d = '08' AND permit_d IN ('08', '8A'))
    OR (well_d = '07' AND permit_d IN ('07', '7C', '7B'));

-- Pre-parse permit dates once
CREATE OR REPLACE TEMP TABLE permits_parsed AS
SELECT filing_no, property, lease_district, lease_number, operator_no,
    TRY_STRPTIME(effective_dt, '%m/%d/%Y')::DATE AS eff_date,
    TRY_STRPTIME(expiration_dt, '%m/%d/%Y')::DATE AS exp_date
FROM permits WHERE status = 'Approved';

-- Find valid permit coverage: lease match OR spatial match
CREATE OR REPLACE TABLE dark_flares AS
SELECT m.*,
    o.operator_name,
    COALESCE(p.filing_no, sp_p.filing_no) AS permit_filing_no,
    COALESCE(p.property, sp_p.property) AS permit_property,
    COALESCE(p.eff_date, sp_p.eff_date) AS permit_effective,
    COALESCE(p.exp_date, sp_p.exp_date) AS permit_expiration,
    p.filing_no IS NULL AND sp_p.filing_no IS NULL AS is_dark
FROM vnf_matched m
LEFT JOIN operators o
    ON LPAD(o.operator_number, 6, '0') = LPAD(m.operator_no, 6, '0')
LEFT JOIN permits_parsed p
    ON p.lease_number = m.lease_number
    AND district_match(m.lease_district, p.lease_district)
    AND p.eff_date <= m.date
    AND (p.exp_date IS NULL OR p.exp_date >= m.date)
LEFT JOIN (
    site_flare_loc_match sl
    JOIN permits_parsed sp_p ON sp_p.filing_no = sl.filing_no
) ON sl.flare_id = m.flare_id
    AND sp_p.eff_date <= m.date
    AND (sp_p.exp_date IS NULL OR sp_p.exp_date >= m.date);

CREATE OR REPLACE VIEW dark_flaring_summary AS
SELECT is_dark,
    count(*) AS detection_days, count(DISTINCT flare_id) AS flare_sites,
    round(avg(rh_mw), 2) AS avg_rh_mw, round(sum(rh_mw), 0) AS total_rh_mw,
    min(date) AS earliest, max(date) AS latest
FROM dark_flares GROUP BY is_dark;

CREATE OR REPLACE VIEW top_dark_flares AS
SELECT flare_id, lease_district, lease_number, operator_no,
    COALESCE(operator_name, 'OP#' || operator_no) AS operator_name,
    round(avg(vnf_lat), 4) AS vnf_lat, round(avg(vnf_lon), 4) AS vnf_lon,
    count(*) AS detection_days, round(sum(rh_mw), 1) AS total_rh_mw,
    min(date) AS first_seen, max(date) AS last_seen
FROM dark_flares WHERE is_dark
GROUP BY flare_id, lease_district, lease_number, operator_no, operator_name
ORDER BY total_rh_mw DESC;
