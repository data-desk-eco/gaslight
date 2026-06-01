-- export.sql — Project the canonical permian.* tables (built by publish.sql)
-- into the web app's parquet files.
--
-- The shareable database dist/gaslight.duckdb is the single source of truth;
-- these parquets are lightweight, flare-centric projections of it tuned for
-- in-browser loading (whole-Permian wells/gatherers are far too large to ship
-- to the client). Column names and types here must stay stable — the web app
-- (web/db.js, web/app.js) reads them by name.

LOAD spatial;

-- Wells within the VIIRS pixel bbox (±0.0034° ≈ 375m) of any flare site, and
-- the leases they belong to. Drives every flare-centric projection below.
CREATE OR REPLACE TEMP TABLE app_flare_lease_match AS
SELECT DISTINCT fs.flare_id, w.api, w.oil_gas_code, w.lease_district, w.lease_number
FROM permian.flare_sites fs
JOIN permian.wells w
    ON w.longitude BETWEEN fs.lon - 0.0034 AND fs.lon + 0.0034
    AND w.latitude  BETWEEN fs.lat - 0.0034 AND fs.lat + 0.0034;

CREATE OR REPLACE TEMP TABLE app_flare_leases AS
SELECT DISTINCT lease_district, lease_number FROM app_flare_lease_match;

-- Flares
COPY (
    SELECT flare_id, lat, lon, detection_days,
        CAST(first_detected AS VARCHAR) AS first_detected,
        CAST(last_detected AS VARCHAR) AS last_detected,
        total_rh_mw, avg_rh_mw
    FROM permian.flare_sites
) TO 'web/data/flares.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Detections (per-flare daily time series)
COPY (
    SELECT flare_id, CAST(date AS VARCHAR) AS date, rh_mw
    FROM permian.flare_detections
) TO 'web/data/detections.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Permits (one row per filing, gas plants excluded from the map)
COPY (
    SELECT filing_no, latitude, longitude, name, county, district,
        release_type, operator_name, status,
        CAST(effective_dt AS VARCHAR) AS effective_dt,
        CAST(expiration_dt AS VARCHAR) AS expiration_dt,
        release_rate_mcf_day, exception_reasons
    FROM permian.permits
    WHERE NOT is_gas_plant
) TO 'web/data/permits.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Plumes
COPY (
    SELECT plume_id, source, satellite, date, latitude, longitude,
        emission_rate, emission_uncertainty, sector
    FROM permian.plumes
) TO 'web/data/plumes.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Facilities (R-3 gas processing)
COPY (
    SELECT serial_number, facility_name, plant_type, latitude, longitude
    FROM permian.facilities
) TO 'web/data/facilities.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Wells (only those near a flare site)
COPY (
    SELECT w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number,
        w.operator_name, w.latitude, w.longitude,
        w.flared_mcf, w.produced_mcf, w.flaring_intensity_pct, w.lease_name
    FROM permian.wells w
    SEMI JOIN app_flare_lease_match m USING (api)
) TO 'web/data/wells.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Leases (flare ↔ lease matches via nearby wells)
COPY (
    SELECT m.flare_id, m.lease_district, m.lease_number, m.oil_gas_code,
        COALESCE(wc.well_count, 0) AS well_count,
        round(COALESCE(lz.total_flared_mcf, 0), 0) AS reported_flared_mcf,
        lz.operator_name AS lease_operator, lz.lease_name
    FROM (SELECT DISTINCT flare_id, oil_gas_code, lease_district, lease_number
          FROM app_flare_lease_match) m
    LEFT JOIN (
        SELECT oil_gas_code, lease_district, lease_number, count(*) AS well_count
        FROM permian.wells GROUP BY 1, 2, 3
    ) wc USING (oil_gas_code, lease_district, lease_number)
    LEFT JOIN permian.leases lz
        ON lz.lease_district = m.lease_district
        AND lz.lease_number = m.lease_number
) TO 'web/data/leases.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Lease monthly flaring/production time series (flare-linked leases only)
COPY (
    SELECT mf.lease_district, mf.lease_number,
        CAST(mf.date AS VARCHAR) AS date,
        mf.total_flared_mcf::INT AS flared_mcf,
        (CASE WHEN mf.total_gas_prod_mcf > 0 THEN mf.total_gas_prod_mcf
              ELSE mf.total_disposed_mcf END)::INT AS produced_mcf
    FROM permian.monthly_flaring mf
    SEMI JOIN app_flare_leases fl
        ON fl.lease_district = mf.lease_district
        AND fl.lease_number = mf.lease_number
    SEMI JOIN permian.leases lz
        ON lz.lease_district = mf.lease_district
        AND lz.lease_number = mf.lease_number
    ORDER BY mf.lease_district, mf.lease_number, mf.year, mf.month
) TO 'web/data/production.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Gatherers/purchasers/nominators (flare-linked leases only)
COPY (
    SELECT g.oil_gas_code, g.district, g.lease_number, g.type, g.percentage,
        g.gpn_number, g.gpn_name, g.is_current, g.first_date, g.last_date
    FROM permian.gatherers g
    SEMI JOIN app_flare_leases fl
        ON fl.lease_district = g.district
        AND fl.lease_number = g.lease_number
) TO 'web/data/gatherers.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
