LOAD spatial;

-- ============================================================
-- Core analysis table: dark_flares (site × day)
-- ============================================================
-- For each VNF detection-day at a matched upstream site, check
-- whether any nearby SWR 32 permit covers that date.
-- One row per (flare_id, date), preferring permitted if ambiguous.

CREATE OR REPLACE TABLE dark_flares AS
WITH matched AS (
    SELECT
        v.flare_id, v.date, v.rh_mw, v.temp_k, v.lat AS vnf_lat, v.lon AS vnf_lon,
        so.operator_name, so.operator_no,
        so.nearest_filing_no AS loc_permit, so.nearest_permit_km AS permit_distance_km,
        so.lease_district AS permit_lease_district,
        so.confidence,
        spc.filing_no IS NULL AS is_dark,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY (spc.filing_no IS NOT NULL) DESC
        ) AS rn
    FROM raw.vnf v
    JOIN flare_sites fs USING (flare_id)
    JOIN site_operators so USING (flare_id)
    LEFT JOIN (
        site_permit_matches sm
        JOIN site_permit_coverage spc ON spc.filing_no = sm.filing_no
    ) ON sm.flare_id = v.flare_id
        AND spc.effective_dt <= v.date
        AND (spc.expiration_dt IS NULL OR spc.expiration_dt >= v.date)
    WHERE v.detected
      AND NOT fs.near_excluded_facility
      AND v.date >= (SELECT MIN(TRY_STRPTIME(submittal_dt, '%m/%d/%Y'))::DATE FROM raw.permits)
)
SELECT * EXCLUDE (rn) FROM matched WHERE rn = 1;

-- ============================================================
-- Lease-level VNF allocation + reported flaring integration
-- ============================================================

-- Site-to-lease mapping: dual path (permit + spatial), deduplicated
CREATE OR REPLACE TABLE site_lease_map AS
WITH permit_path AS (
    SELECT DISTINCT
        so.flare_id,
        plm.lease_district, plm.lease_number, plm.lease_name,
        'permit' AS match_source
    FROM site_operators so
    JOIN permit_lease_map plm ON plm.filing_no = so.nearest_filing_no
),
spatial_path AS (
    SELECT DISTINCT
        slm.flare_id,
        slm.lease_district, slm.lease_number,
        COALESCE(pl.lease_name, rf.lease_name) AS lease_name,
        'spatial' AS match_source
    FROM site_lease_matches slm
    LEFT JOIN raw.pdq_leases pl
        ON pl.district_name = slm.lease_district
        AND LPAD(pl.lease_no, 6, '0') = LPAD(slm.lease_number, 6, '0')
    LEFT JOIN (
        SELECT DISTINCT district, lease_no, lease_name
        FROM reported_flaring
    ) rf ON rf.district = slm.lease_district
        AND LPAD(rf.lease_no, 6, '0') = LPAD(slm.lease_number, 6, '0')
    WHERE (slm.flare_id, slm.lease_district, slm.lease_number) NOT IN (
        SELECT flare_id, lease_district, lease_number FROM permit_path
    )
)
SELECT * FROM permit_path
UNION ALL
SELECT * FROM spatial_path;

-- VNF detection-days allocated to leases via site_lease_map
CREATE OR REPLACE TABLE lease_vnf_allocation AS
WITH detection_leases AS (
    SELECT
        v.flare_id, v.date, v.rh_mw,
        COALESCE(df.is_dark, TRUE) AS is_dark,
        COALESCE(df.operator_name, so.operator_name) AS operator_name,
        slm.lease_district, slm.lease_number, slm.lease_name,
        slm.match_source
    FROM raw.vnf v
    JOIN flare_sites fs USING (flare_id)
    JOIN site_lease_map slm ON slm.flare_id = v.flare_id
    LEFT JOIN dark_flares df ON df.flare_id = v.flare_id AND df.date = v.date
    LEFT JOIN site_operators so ON so.flare_id = v.flare_id
    WHERE v.detected
      AND NOT fs.near_excluded_facility
      AND v.date >= (SELECT MIN(TRY_STRPTIME(submittal_dt, '%m/%d/%Y'))::DATE FROM raw.permits)
),
with_weights AS (
    SELECT *,
        1.0 / COUNT(*) OVER (PARTITION BY flare_id, date) AS weight
    FROM detection_leases
)
SELECT
    flare_id, date, is_dark, operator_name,
    lease_district, lease_number, lease_name,
    rh_mw * weight AS allocated_rh_mw, weight,
    match_source
FROM with_weights;

CREATE OR REPLACE TABLE lease_flaring AS
WITH months AS (
    SELECT DISTINCT date_trunc('month', date)::DATE AS month
    FROM dark_flares WHERE date >= '2023-10-01'
),
reported AS (
    SELECT district AS lease_district, lease_no AS lease_number,
        (year::VARCHAR || '-' || LPAD(month::VARCHAR, 2, '0') || '-01')::DATE AS month,
        operator_name, lease_name, total_flared_mcf
    FROM reported_flaring
    WHERE district IN ('7B','7C','08','8A') AND year >= 2023
),
vnf AS (
    SELECT lease_district, lease_number,
        date_trunc('month', date)::DATE AS month,
        count(*) AS vnf_detection_days,
        round(sum(allocated_rh_mw), 2) AS vnf_rh_mw,
        sum(CASE WHEN is_dark THEN 1 ELSE 0 END) AS vnf_dark_days,
        round(sum(CASE WHEN is_dark THEN allocated_rh_mw ELSE 0 END), 2) AS vnf_dark_rh_mw
    FROM lease_vnf_allocation GROUP BY 1, 2, 3
),
permit_days AS (
    SELECT lease_district, lease_number, month, days_in_month,
        count(DISTINCT covered_day) AS covered_days
    FROM (
        SELECT
            plm.lease_district, plm.lease_number, m.month,
            EXTRACT(DAY FROM m.month + INTERVAL 1 MONTH - m.month)::INT AS days_in_month,
            UNNEST(generate_series(
                GREATEST(COALESCE(TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'), TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE, m.month),
                LEAST(COALESCE(TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'), TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE, (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE),
                INTERVAL 1 DAY
            ))::DATE AS covered_day
        FROM permit_lease_map plm
        JOIN raw.permits p ON p.filing_no = plm.filing_no
        LEFT JOIN raw.permit_details pd ON pd.filing_no = plm.filing_no
        CROSS JOIN months m
        WHERE COALESCE(pd.exception_status, p.status) IN ('Approved', 'Submitted', 'Hearing Pending', 'Resubmitted')
          AND COALESCE(TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'), TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE <= (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE
          AND COALESCE(TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'), TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE >= m.month
          AND GREATEST(COALESCE(TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'), TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE, m.month)
              <= LEAST(COALESCE(TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'), TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE, (m.month + INTERVAL 1 MONTH - INTERVAL 1 DAY)::DATE)
    ) GROUP BY 1, 2, 3, 4
)
SELECT
    COALESCE(r.lease_district, v.lease_district) AS lease_district,
    COALESCE(r.lease_number, v.lease_number) AS lease_number,
    COALESCE(r.month, v.month) AS month,
    r.operator_name, r.lease_name,
    COALESCE(r.total_flared_mcf, 0) AS reported_flared_mcf,
    COALESCE(v.vnf_detection_days, 0) AS vnf_detection_days,
    COALESCE(v.vnf_rh_mw, 0) AS vnf_rh_mw,
    COALESCE(v.vnf_dark_days, 0) AS vnf_dark_days,
    COALESCE(v.vnf_dark_rh_mw, 0) AS vnf_dark_rh_mw,
    COALESCE(pd.covered_days * 1.0 / pd.days_in_month, 0) AS permit_coverage,
    COALESCE(pd.covered_days, 0) AS permit_days,
    round(COALESCE(r.total_flared_mcf, 0) * (1 - COALESCE(pd.covered_days * 1.0 / pd.days_in_month, 0)), 0) AS unpermitted_flared_mcf
FROM reported r
FULL OUTER JOIN vnf v
    ON v.lease_district = r.lease_district
    AND LPAD(v.lease_number, 6, '0') = LPAD(r.lease_number, 6, '0')
    AND v.month = r.month
LEFT JOIN permit_days pd
    ON pd.lease_district = COALESCE(r.lease_district, v.lease_district)
    AND LPAD(pd.lease_number, 6, '0') = LPAD(COALESCE(r.lease_number, v.lease_number), 6, '0')
    AND pd.month = COALESCE(r.month, v.month);

-- ============================================================
-- Notebook views (all notebook SQL = SELECT * FROM view)
-- ============================================================

CREATE OR REPLACE VIEW headline AS
SELECT
    count(DISTINCT CASE WHEN is_dark THEN flare_id END) AS dark_sites,
    sum(CASE WHEN is_dark THEN 1 ELSE 0 END) AS dark_days,
    sum(1) AS total_days,
    round(100.0 * sum(CASE WHEN is_dark THEN 1 ELSE 0 END) / count(*), 0) AS pct_dark,
    round(sum(CASE WHEN is_dark THEN rh_mw ELSE 0 END), 0) AS dark_rh_mw,
    (SELECT round(sum(unpermitted_flared_mcf) / 1e6, 1) FROM lease_flaring) AS unpermitted_bcf,
    (SELECT round(100.0 * sum(unpermitted_flared_mcf) / NULLIF(sum(reported_flared_mcf), 0), 0) FROM lease_flaring WHERE reported_flared_mcf > 0) AS pct_reported_unpermitted
FROM dark_flares;

CREATE OR REPLACE VIEW dark_sites AS
SELECT flare_id,
    extract(year FROM date)::INTEGER AS year,
    COALESCE(operator_name, 'Unknown') AS operator,
    confidence,
    round(avg(vnf_lat), 4) AS lat, round(avg(vnf_lon), 4) AS lon,
    count(*) AS detection_days,
    round(sum(rh_mw), 1) AS total_rh_mw,
    round(avg(rh_mw), 2) AS avg_rh_mw
FROM dark_flares WHERE is_dark
GROUP BY flare_id, year, operator, confidence;

CREATE OR REPLACE VIEW dark_operators AS
SELECT
    COALESCE(operator_name, 'Unknown') AS operator,
    count(DISTINCT flare_id) AS sites,
    count(*) AS detection_days,
    round(sum(rh_mw), 0) AS total_rh_mw
FROM dark_flares
WHERE is_dark AND confidence IN ('sole', 'majority')
GROUP BY 1 ORDER BY total_rh_mw DESC
LIMIT 15;

CREATE OR REPLACE VIEW dark_quarterly AS
SELECT date_trunc('quarter', date) AS quarter,
    is_dark,
    count(*) AS detection_days,
    count(DISTINCT flare_id) AS sites,
    round(sum(rh_mw), 0) AS total_rh_mw
FROM dark_flares GROUP BY 1, 2 ORDER BY 1;

CREATE OR REPLACE VIEW top_leases AS
SELECT
    lease_district AS district, lease_number,
    max(operator_name) AS operator, max(lease_name) AS lease_name,
    count(*) AS months,
    round(sum(reported_flared_mcf), 0) AS reported_mcf,
    round(sum(unpermitted_flared_mcf), 0) AS unpermitted_mcf,
    sum(vnf_detection_days) AS vnf_days,
    round(sum(vnf_rh_mw), 1) AS vnf_rh_mw,
    round(avg(permit_coverage), 2) AS avg_coverage
FROM lease_flaring
WHERE reported_flared_mcf > 0 OR vnf_detection_days > 0
GROUP BY 1, 2
HAVING sum(unpermitted_flared_mcf) > 0
ORDER BY unpermitted_mcf DESC;

-- ============================================================
-- Plume attribution (separate analysis pipeline)
-- ============================================================

CREATE OR REPLACE TABLE plume_attributed AS
WITH plume_vnf AS (
    SELECT * FROM (
        SELECT
            psm.plume_id, psm.flare_id, psm.distance_km AS vnf_distance_km,
            v.rh_mw, CASE WHEN v.date IS NOT NULL THEN true ELSE false END AS flare_detected,
            ROW_NUMBER() OVER (PARTITION BY psm.plume_id ORDER BY (v.date IS NOT NULL) DESC, psm.distance_km) AS rn
        FROM plume_site_matches psm
        LEFT JOIN raw.vnf v ON v.flare_id = psm.flare_id AND v.detected
            AND v.date BETWEEN psm.plume_date - INTERVAL 1 DAY AND psm.plume_date + INTERVAL 1 DAY
        WHERE psm.rank = 1
    ) WHERE rn = 1
)
SELECT
    p.plume_id, p.source, p.satellite, p.date, p.latitude, p.longitude,
    p.emission_rate, p.emission_uncertainty, p.sector,
    pw.api, pw.oil_gas_code, pw.lease_district, pw.lease_number, pw.operator_no,
    pw.distance_km AS well_distance_km,
    o.operator_name,
    pv.flare_id AS vnf_flare_id, pv.vnf_distance_km,
    COALESCE(pv.flare_detected, false) AS flare_detected, pv.rh_mw AS vnf_rh_mw,
    CASE
        WHEN pv.flare_id IS NOT NULL AND NOT COALESCE(pv.flare_detected, false) THEN 'unlit'
        WHEN pv.flare_id IS NOT NULL AND COALESCE(pv.flare_detected, false) THEN 'flaring'
        WHEN pw.api IS NOT NULL THEN 'wellpad'
        ELSE 'unmatched'
    END AS classification
FROM raw.plumes p
LEFT JOIN (SELECT * FROM plume_well_matches WHERE rank = 1) pw USING (plume_id)
LEFT JOIN plume_vnf pv USING (plume_id)
LEFT JOIN raw.operators o ON LPAD(o.operator_number, 6, '0') = LPAD(pw.operator_no, 6, '0');

CREATE OR REPLACE VIEW plume_summary AS
SELECT classification, source,
    count(*) AS plume_count,
    round(avg(emission_rate), 1) AS avg_emission_rate,
    round(sum(emission_rate), 0) AS total_emission_rate,
    min(date) AS earliest, max(date) AS latest
FROM plume_attributed GROUP BY 1, 2 ORDER BY 1, 2;
