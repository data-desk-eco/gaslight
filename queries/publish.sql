-- publish.sql — Build the canonical, shareable Permian dataset.
--
-- Produces a clean `permian.*` schema inside data.duckdb (whole-Permian,
-- geom-free, consistently named, 2021+ where temporal) and copies it into a
-- fresh standalone database `dist/gaslight.duckdb` whose `main` schema is the
-- single product shared with newsrooms AND consumed by the web map (via the
-- parquet projections built in export.sql).
--
-- No `raw`/`rrc` internals leak into the shared file; no GEOMETRY blobs
-- (lat/lon are retained). The in-DB `_dictionary` / `_sources` tables and the
-- markdown docs under docs/data-dictionary/ are added afterwards by
-- scripts/build_dictionary.py.

LOAD spatial;

-- Single window shared by every time-series table. Widen here to extend
-- coverage; everything keys off this one variable.
SET VARIABLE start_date = '2021-01-01'::DATE;
SET VARIABLE lat_min = 30.0;
SET VARIABLE lat_max = 33.5;
SET VARIABLE lon_min = -104.5;
SET VARIABLE lon_max = -100.0;
SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

CREATE OR REPLACE MACRO normalize_lease(n) AS LPAD(n, 6, '0');

CREATE OR REPLACE MACRO in_permian(lat, lon) AS
    lat BETWEEN getvariable('lat_min') AND getvariable('lat_max')
    AND lon BETWEEN getvariable('lon_min') AND getvariable('lon_max')
    AND (lat <= 32.0 OR lon >= getvariable('nm_border_lon'));

CREATE SCHEMA IF NOT EXISTS permian;

-- ---------------------------------------------------------------------------
-- Lease-level flaring rollup (shared by wells + leases). Flaring numerator
-- from disposition code 04 (months with flaring only); production denominator
-- from the full production table (all months). 2021+, Permian districts.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TEMP TABLE lease_flaring AS
WITH flared AS (
    SELECT district, lease_number,
        sum(total_flared_mcf) AS total_flared_mcf,
        mode(operator_name) AS operator_name,
        mode(lease_name) AS lease_name
    FROM rrc.production
    WHERE district IN ('6E','7B','7C','08','8A')
      AND make_date(year, month, 1) >= getvariable('start_date')
    GROUP BY 1, 2
),
produced AS (
    SELECT dm.rrc_district AS district, lp.lease_no AS lease_number,
        sum(lp.lease_gas_prod_vol + lp.lease_csgd_prod_vol) AS total_gas_prod_mcf
    FROM raw.lease_production lp
    JOIN rrc.district_map dm ON dm.pdq_district = lp.district_no
    WHERE dm.rrc_district IN ('6E','7B','7C','08','8A')
      AND make_date(lp.cycle_year::INT, lp.cycle_month::INT, 1) >= getvariable('start_date')
    GROUP BY 1, 2
)
SELECT f.district,
    normalize_lease(f.lease_number) AS lease_number,
    f.total_flared_mcf,
    COALESCE(p.total_gas_prod_mcf, f.total_flared_mcf) AS total_gas_prod_mcf,
    f.operator_name, f.lease_name
FROM flared f
LEFT JOIN produced p
    ON p.district = f.district
    AND normalize_lease(p.lease_number) = normalize_lease(f.lease_number);

-- Well count per lease (all Permian wells with valid coords)
CREATE OR REPLACE TEMP TABLE lease_well_count AS
SELECT lease_district AS district,
    normalize_lease(lease_number) AS lease_number,
    count(*) AS well_count
FROM raw.wells
WHERE latitude != 0 AND longitude != 0
  AND in_permian(latitude, longitude)
GROUP BY 1, 2;

-- ---------------------------------------------------------------------------
-- vnf_sites — one row per VIIRS Nightfire flare site (2021+, Permian)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.vnf_sites AS
WITH agg AS (
    SELECT flare_id,
        AVG(lat) AS lat, AVG(lon) AS lon,
        MIN(date) AS first_detected, MAX(date) AS last_detected,
        COUNT(*) AS detection_days,
        sum(rh_mw) AS total_rh_mw,
        avg(rh_mw) FILTER (WHERE rh_mw > 0) AS avg_rh_mw
    FROM raw.vnf
    WHERE detected AND date >= getvariable('start_date')
    GROUP BY flare_id
)
SELECT flare_id,
    round(lat, 6) AS lat, round(lon, 6) AS lon,
    detection_days,
    first_detected, last_detected,
    round(total_rh_mw, 1) AS total_rh_mw,
    round(avg_rh_mw, 2) AS avg_rh_mw
FROM agg
WHERE in_permian(lat, lon);

-- ---------------------------------------------------------------------------
-- vnf_detections — per-site daily detection time series (2021+)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.vnf_detections AS
SELECT v.flare_id, v.date, round(v.rh_mw, 2) AS rh_mw
FROM raw.vnf v
SEMI JOIN permian.vnf_sites fs USING (flare_id)
WHERE v.detected AND v.date >= getvariable('start_date');

-- ---------------------------------------------------------------------------
-- permits — one row per SWR 32 flare permit filing (Permian). Gas plants kept
-- and flagged (is_gas_plant) rather than dropped, so leads aren't hidden.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.permits AS
SELECT
    fl.filing_no,
    round(fl.latitude, 6) AS latitude,
    round(fl.longitude, 6) AS longitude,
    fl.name, fl.county, fl.district, fl.release_type,
    p.operator_no, p.operator_name, p.property_type, p.status,
    p.effective_dt, p.expiration_dt,
    round(COALESCE(plm.release_rate_mcf_day, 0), 0) AS release_rate_mcf_day,
    p.exception_reasons,
    (fl.filing_no IN (SELECT filing_no FROM raw.permits WHERE property_type = 'Gas Plant')
     OR COALESCE(fl.facility_type, '') ILIKE '%gas plant%') AS is_gas_plant
FROM raw.permit_locations fl
JOIN rrc.permits p ON p.filing_no = fl.filing_no
LEFT JOIN (
    SELECT filing_no, sum(TRY_CAST(requested_release_rate_mcf_day AS DOUBLE)) AS release_rate_mcf_day
    FROM rrc.permit_leases GROUP BY filing_no
) plm ON plm.filing_no = fl.filing_no
WHERE fl.latitude IS NOT NULL AND fl.longitude IS NOT NULL
  AND in_permian(fl.latitude, fl.longitude);

-- ---------------------------------------------------------------------------
-- permit_leases — each filing's underlying leases (commingle permits flattened)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.permit_leases AS
SELECT pl.filing_no, pl.property_type,
    pl.lease_district, normalize_lease(pl.lease_number) AS lease_number,
    pl.lease_name,
    TRY_CAST(pl.requested_release_rate_mcf_day AS DOUBLE) AS requested_release_rate_mcf_day
FROM rrc.permit_leases pl
SEMI JOIN permian.permits pp ON pp.filing_no = pl.filing_no;

-- ---------------------------------------------------------------------------
-- wells — every Permian oil/gas well with per-lease flaring metrics
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.wells AS
SELECT w.api, w.oil_gas_code, w.lease_district,
    normalize_lease(w.lease_number) AS lease_number, w.well_number,
    w.operator_no, COALESCE(o.operator_name, 'Unknown') AS operator_name,
    round(w.latitude, 6) AS latitude, round(w.longitude, 6) AS longitude,
    round(COALESCE(lf.total_flared_mcf, 0), 0) AS flared_mcf,
    round(COALESCE(lf.total_gas_prod_mcf, 0), 0) AS produced_mcf,
    CASE WHEN lf.total_gas_prod_mcf > 0
         THEN round(100.0 * lf.total_flared_mcf / lf.total_gas_prod_mcf, 1)
         ELSE NULL END AS flaring_intensity_pct,
    lf.lease_name
FROM raw.wells w
LEFT JOIN raw.operators o ON o.operator_number = w.operator_no
LEFT JOIN lease_flaring lf
    ON lf.district = w.lease_district
    AND lf.lease_number = normalize_lease(w.lease_number)
WHERE w.latitude != 0 AND w.longitude != 0
  AND in_permian(w.latitude, w.longitude);

-- ---------------------------------------------------------------------------
-- leases — lease-level flaring rollup (Permian leases that reported flaring)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.leases AS
SELECT lf.district AS lease_district, lf.lease_number,
    lf.lease_name, lf.operator_name,
    round(lf.total_flared_mcf, 0) AS total_flared_mcf,
    round(lf.total_gas_prod_mcf, 0) AS total_gas_prod_mcf,
    CASE WHEN lf.total_gas_prod_mcf > 0
         THEN round(100.0 * lf.total_flared_mcf / lf.total_gas_prod_mcf, 1)
         ELSE NULL END AS flaring_intensity_pct,
    COALESCE(wc.well_count, 0) AS well_count
FROM lease_flaring lf
LEFT JOIN lease_well_count wc
    ON wc.district = lf.district AND wc.lease_number = lf.lease_number;

-- ---------------------------------------------------------------------------
-- monthly_flaring — lease × month reported flaring/production (2021+, Permian)
-- (was `rrc.production`; renamed because it holds flaring months only)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.monthly_flaring AS
SELECT oil_gas_code, district AS lease_district,
    normalize_lease(lease_number) AS lease_number,
    lease_name, operator_no, operator_name, field_name,
    year, month, make_date(year, month, 1) AS date,
    round(gas_flared_mcf, 0) AS gas_flared_mcf,
    round(csgd_flared_mcf, 0) AS csgd_flared_mcf,
    round(total_flared_mcf, 0) AS total_flared_mcf,
    round(total_disposed_mcf, 0) AS total_disposed_mcf,
    round(total_gas_prod_mcf, 0) AS total_gas_prod_mcf
FROM rrc.production
WHERE district IN ('6E','7B','7C','08','8A')
  AND make_date(year, month, 1) >= getvariable('start_date');

-- ---------------------------------------------------------------------------
-- gatherers — gatherer/purchaser/nominator per Permian lease (P-4 records)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.gatherers AS
SELECT
    g.oil_gas_code, g.district,
    normalize_lease(g.lease_rrcid::VARCHAR) AS lease_number,
    CASE g.type_code WHEN 'G' THEN 'Gatherer' WHEN 'H' THEN 'Purchaser'
                     WHEN 'I' THEN 'Nominator' ELSE g.type_code END AS type,
    MAX(round(g.percentage * 100, 2)) AS percentage,
    g.gpn_number,
    COALESCE(o.operator_name, 'Unknown (' || g.gpn_number || ')') AS gpn_name,
    g.is_current::VARCHAR AS is_current,
    MIN(NULLIF(g.effective_date, '')) AS first_date,
    MAX(NULLIF(g.effective_date, '')) AS last_date
FROM raw.gatherers g
LEFT JOIN raw.operators o
    ON normalize_lease(o.operator_number::VARCHAR) = normalize_lease(g.gpn_number)
WHERE g.district IN ('6E','7B','7C','08','8A')
GROUP BY g.oil_gas_code, g.district, g.lease_rrcid, g.type_code,
    g.gpn_number, o.operator_name, g.is_current;

-- ---------------------------------------------------------------------------
-- plumes — methane plume detections (Carbon Mapper + UNEP IMEO), Permian
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.plumes AS
SELECT plume_id, source, satellite, date,
    round(latitude, 6) AS latitude, round(longitude, 6) AS longitude,
    round(emission_rate, 1) AS emission_rate,
    round(emission_uncertainty, 1) AS emission_uncertainty,
    sector
FROM raw.plumes
WHERE in_permian(latitude, longitude);

-- ---------------------------------------------------------------------------
-- facilities — RRC R-3 gas processing facilities (Permian)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.facilities AS
SELECT serial_number, facility_name, plant_type,
    round(latitude, 6) AS latitude, round(longitude, 6) AS longitude
FROM raw.excluded_facilities
WHERE in_permian(latitude, longitude);

-- ---------------------------------------------------------------------------
-- operators — RRC operator number → name reference lookup (statewide)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.operators AS
SELECT operator_number AS operator_no, operator_name, status
FROM raw.operators;

-- ---------------------------------------------------------------------------
-- s2_detections — Sentinel-2 SWIR flare sites from permian-flaring (top-scoring,
-- Permian-clipped). One row per H3 site; `detections` is a JSON array of the
-- site's per-date observations (date, max_b12, pixels). 2025-01..2026-05 window.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TABLE permian.s2_detections AS
SELECT
    h3, lat, lon,
    n_detections, n_dates,
    first_date::DATE AS first_date,
    last_date::DATE AS last_date,
    max_b12, mean_max_b12, b12_b11_ratio, min_glint_score,
    total_score, corroborated, nearest_source,
    detections
FROM raw.s2_catalogue;

-- ---------------------------------------------------------------------------
-- Copy the clean schema into the standalone shareable database.
-- ---------------------------------------------------------------------------
ATTACH 'dist/gaslight.duckdb' AS dist;

CREATE OR REPLACE TABLE dist.vnf_sites         AS SELECT * FROM permian.vnf_sites;
CREATE OR REPLACE TABLE dist.vnf_detections    AS SELECT * FROM permian.vnf_detections;
CREATE OR REPLACE TABLE dist.permits           AS SELECT * FROM permian.permits;
CREATE OR REPLACE TABLE dist.permit_leases     AS SELECT * FROM permian.permit_leases;
CREATE OR REPLACE TABLE dist.wells             AS SELECT * FROM permian.wells;
CREATE OR REPLACE TABLE dist.leases            AS SELECT * FROM permian.leases;
CREATE OR REPLACE TABLE dist.monthly_flaring   AS SELECT * FROM permian.monthly_flaring;
CREATE OR REPLACE TABLE dist.gatherers         AS SELECT * FROM permian.gatherers;
CREATE OR REPLACE TABLE dist.plumes            AS SELECT * FROM permian.plumes;
CREATE OR REPLACE TABLE dist.facilities        AS SELECT * FROM permian.facilities;
CREATE OR REPLACE TABLE dist.operators         AS SELECT * FROM permian.operators;
CREATE OR REPLACE TABLE dist.s2_detections     AS SELECT * FROM permian.s2_detections;

DETACH dist;
