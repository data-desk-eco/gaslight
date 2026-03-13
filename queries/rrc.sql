LOAD spatial;

-- PDQ district_no (numeric) → RRC district ID (alphanumeric)
CREATE OR REPLACE TABLE rrc.district_map AS
SELECT * FROM (VALUES
    ('01','01'), ('02','02'), ('03','03'), ('04','04'), ('05','05'), ('06','06'),
    ('07','6E'), ('08','7B'), ('09','7C'), ('10','08'), ('11','8A'), ('12','8B'),
    ('13','09'), ('14','10')
) AS t(pdq_district, rrc_district);

-- Permits: merged filings + detail pages with parsed dates
CREATE OR REPLACE TABLE rrc.permits AS
SELECT
    p.filing_no, p.excep_seq, p.operator_no,
    COALESCE(pd.operator, p.operator_name) AS operator_name,
    p.property, p.property_type, p.lease_district, p.lease_number, p.fv_district,
    COALESCE(pd.exception_status, p.status) AS status,
    pd.filing_type, pd.site_name, pd.exception_reasons,
    COALESCE(TRY_STRPTIME(pd.requested_effective_date, '%m/%d/%Y'),
             TRY_STRPTIME(p.effective_dt, '%m/%d/%Y'))::DATE AS effective_dt,
    COALESCE(TRY_STRPTIME(pd.requested_expiration_date, '%m/%d/%Y'),
             TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y'))::DATE AS expiration_dt
FROM raw.permits p
LEFT JOIN raw.permit_details pd ON pd.filing_no = p.filing_no;

-- Permit → lease mapping (flattens commingle permits to underlying leases)
CREATE OR REPLACE TABLE rrc.permit_leases AS
SELECT pp.filing_no, pp.property_type,
    pp.district AS lease_district, pp.property_id AS lease_number,
    pp.lease_name, pp.requested_release_rate_mcf_day
FROM raw.permit_properties pp
WHERE pp.property_type IN ('Oil Lease', 'Gas Lease', 'Drilling Permit')
  AND pp.property_id IS NOT NULL AND pp.property_id != '';

-- Monthly reported flaring by lease (disposition code 04 = flared/vented)
-- Joined with actual production volumes from lease_production for proper denominator
CREATE OR REPLACE TABLE rrc.production AS
SELECT
    gd.oil_gas_code, dm.rrc_district AS district,
    gd.lease_no AS lease_number,
    gd.cycle_year::INT AS year, gd.cycle_month::INT AS month,
    gd.operator_no, gd.operator_name, gd.lease_name, gd.field_name,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) AS gas_flared_mcf,
    COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS csgd_flared_mcf,
    COALESCE(gd.lease_gas_dispcd04_vol, 0) + COALESCE(gd.lease_csgd_dispcde04_vol, 0) AS total_flared_mcf,
    COALESCE(gd.lease_gas_total_vol, 0) + COALESCE(gd.lease_csgd_total_vol, 0) AS total_disposed_mcf,
    COALESCE(lp.lease_gas_prod_vol, 0) + COALESCE(lp.lease_csgd_prod_vol, 0) AS total_gas_prod_mcf
FROM raw.gas_disposition gd
LEFT JOIN rrc.district_map dm ON dm.pdq_district = gd.district_no
LEFT JOIN raw.lease_production lp
    ON lp.oil_gas_code = gd.oil_gas_code
    AND lp.district_no = gd.district_no
    AND lp.lease_no = gd.lease_no
    AND lp.cycle_year = gd.cycle_year
    AND lp.cycle_month = gd.cycle_month;
