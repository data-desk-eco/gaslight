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
-- the leases they belong to. Drives the wells/lease/production/gatherer
-- projections below — the map's well layer ships the same one-pixel net used
-- for attribution (the "flare detection area" box readers see).
CREATE OR REPLACE TEMP TABLE app_flare_lease_match AS
SELECT DISTINCT fs.flare_id, w.api, w.oil_gas_code, w.lease_district, w.lease_number
FROM permian.vnf_sites fs
JOIN permian.wells_tx w
    ON w.longitude BETWEEN fs.lon - 0.0034 AND fs.lon + 0.0034
    AND w.latitude  BETWEEN fs.lat - 0.0034 AND fs.lat + 0.0034;

CREATE OR REPLACE TEMP TABLE app_flare_leases AS
SELECT DISTINCT lease_district, lease_number FROM app_flare_lease_match;

-- VNF sites
COPY (
    SELECT flare_id, lat, lon, detection_days,
        CAST(first_detected AS VARCHAR) AS first_detected,
        CAST(last_detected AS VARCHAR) AS last_detected,
        total_rh_mw, avg_rh_mw, avg_flow_rate
    FROM permian.vnf_sites
) TO 'web/data/vnf.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- VNF detections (per-site daily time series)
COPY (
    SELECT flare_id, CAST(date AS VARCHAR) AS date, rh_mw, flow_rate
    FROM permian.vnf_detections
) TO 'web/data/vnf_detections.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

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

-- NM OCD notifications — flare/vent notices aggregated to sites (one point per
-- location), mirroring how permits collapse many filings to a marker. Vented
-- gas (Mcf) is summed so venting sites can be read against the plumes layer.
COPY (
    SELECT round(latitude, 5) AS latitude, round(longitude, 5) AS longitude,
        mode(operator) AS operator, mode(facility_name) AS facility_name,
        mode(county) AS county,
        count(*) AS n_events,
        count(*) FILTER (incident_type = 'Flare') AS n_flare,
        count(*) FILTER (incident_type IN ('Vent', 'Vent with Flaring')) AS n_vent,
        round(sum(volume_released) FILTER (
            incident_type IN ('Vent', 'Vent with Flaring') AND upper(volume_unit) = 'MCF'), 0) AS vented_mcf,
        CAST(min(incident_date) AS VARCHAR) AS first_date,
        CAST(max(incident_date) AS VARCHAR) AS last_date,
        max(incident_number) AS incident_number
    FROM permian.nm_notifications
    WHERE incident_type IN ('Flare', 'Vent', 'Vent with Flaring')
    GROUP BY round(latitude, 5), round(longitude, 5)
) TO 'web/data/nmocd.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Wells (those within a VIIRS pixel of a flare site; see app_flare_lease_match)
COPY (
    SELECT w.api, w.oil_gas_code, w.lease_district, w.lease_number, w.well_number,
        w.operator_name, w.latitude, w.longitude,
        w.flared_mcf, w.produced_mcf, w.flaring_intensity_pct, w.lease_name
    FROM permian.wells_tx w
    SEMI JOIN app_flare_lease_match m USING (api)
) TO 'web/data/wells_tx.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- NM OCD wells within a VIIRS pixel of any flare site (same net as the TX well
-- layer). No PDQ flaring metrics exist on the NM side, so these ship header
-- attributes only; the full NM well set lives in dist.wells_nm for the shareable DB.
COPY (
    SELECT DISTINCT w.api, w.well_name, w.well_number, w.well_type, w.status,
        w.operator, w.district, w.section, w.township, w.range, w.footages,
        w.measured_depth, w.true_vertical_depth,
        CAST(w.spud_date AS VARCHAR) AS spud_date,
        CAST(w.last_production AS VARCHAR) AS last_production,
        w.latitude, w.longitude
    FROM permian.wells_nm w
    JOIN permian.vnf_sites fs
        ON w.longitude BETWEEN fs.lon - 0.0034 AND fs.lon + 0.0034
        AND w.latitude  BETWEEN fs.lat - 0.0034 AND fs.lat + 0.0034
) TO 'web/data/wells_nm.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

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
        FROM permian.wells_tx GROUP BY 1, 2, 3
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

-- Sentinel-2 flare sites (whole top-scoring catalogue; site-level only). The
-- per-date `detections` are split into s2_detections.parquet below so the map
-- can load the site layer at boot without dragging the (much larger) time
-- series with it. The web map reads these columns by name (web/db.js
-- queryS2Precomputed, web/s2.js).
COPY (
    SELECT h3, lon, lat, n_detections, n_dates, n_clear_obs, persistence,
        CAST(first_date AS VARCHAR) AS first_date,
        CAST(last_date AS VARCHAR) AS last_date,
        max_b12, mean_max_b12, b12_b11_ratio, min_glint_score,
        total_score, corroborated, nearest_source
    FROM permian.s2_detections
) TO 'web/data/s2.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Sentinel-2 per-date detections (one row per site-date). Flattened out of the
-- site `detections` JSON array — columnar ZSTD on dates/floats is far smaller
-- than the embedded JSON, and the web app loads this lazily and queries it by
-- h3 only when an S2 card opens (mirrors vnf_detections). `pixels` is dropped:
-- the card renders only date + max_b12.
COPY (
    SELECT h3, d.date::VARCHAR AS date, d.max_b12
    FROM permian.s2_detections,
         UNNEST(CAST(detections AS STRUCT(date VARCHAR, max_b12 DOUBLE, pixels INT)[])) AS t(d)
    ORDER BY h3, date
) TO 'web/data/s2_detections.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Gatherers/purchasers/nominators (flare-linked leases only)
COPY (
    SELECT g.oil_gas_code, g.district, g.lease_number, g.type, g.percentage,
        g.gpn_number, g.gpn_name, g.is_current, g.first_date, g.last_date
    FROM permian.gatherers g
    SEMI JOIN app_flare_leases fl
        ON fl.lease_district = g.district
        AND fl.lease_number = g.lease_number
) TO 'web/data/gatherers.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
