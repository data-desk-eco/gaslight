-- minimal lease-level texas flaring/venting dataset for the houston chronicle.
-- reads the raw rrc pdq dsv dumps directly (data/pdq/, statewide, all years) --
-- deliberately bypasses the permian-clipped pipeline so the path from raw data
-- is a single traceable statement.
--
-- grain: one row per lease-month reporting any disposition code 04 volume.
-- rrc code 04 = "vented or flared" -- the rrc does not separate the two.
-- production volumes joined on the same keys give the intensity denominator.
-- volumes are mcf; oil_gas_code 'O' leases flare casinghead gas, 'G' gas-well gas.
-- district is the rrc alphanumeric district (pdq's DISTRICT_NAME; its numeric
-- DISTRICT_NO is an internal code, dropped). district + lease_no + oil_gas_code
-- is the rrc lease identity usable in public rrc lookups.
COPY (
    SELECT
        d.OIL_GAS_CODE AS oil_gas_code,
        d.DISTRICT_NAME AS district,
        d.LEASE_NO AS lease_no,
        d.LEASE_NAME AS lease_name,
        d.OPERATOR_NO AS operator_no,
        d.OPERATOR_NAME AS operator_name,
        d.FIELD_NO AS field_no,
        d.FIELD_NAME AS field_name,
        d.CYCLE_YEAR::INT AS year,
        d.CYCLE_MONTH::INT AS month,
        TRY_CAST(d.LEASE_GAS_DISPCD04_VOL AS DOUBLE) AS gas_flared_vented_mcf,
        TRY_CAST(d.LEASE_CSGD_DISPCDE04_VOL AS DOUBLE) AS casinghead_flared_vented_mcf,
        TRY_CAST(p.LEASE_GAS_PROD_VOL AS DOUBLE) AS gas_produced_mcf,
        TRY_CAST(p.LEASE_CSGD_PROD_VOL AS DOUBLE) AS casinghead_produced_mcf
    FROM read_csv('data/pdq/OG_LEASE_CYCLE_DISP_DATA_TABLE.dsv',
            delim='}', header=true, all_varchar=true, ignore_errors=true) d
    LEFT JOIN read_csv('data/pdq/OG_LEASE_CYCLE_DATA_TABLE.dsv',
            delim='}', header=true, all_varchar=true, ignore_errors=true) p
        USING (OIL_GAS_CODE, DISTRICT_NO, LEASE_NO, CYCLE_YEAR, CYCLE_MONTH)
    WHERE COALESCE(TRY_CAST(d.LEASE_GAS_DISPCD04_VOL AS DOUBLE), 0) > 0
       OR COALESCE(TRY_CAST(d.LEASE_CSGD_DISPCDE04_VOL AS DOUBLE), 0) > 0
    ORDER BY year, month, district, lease_no
) TO 'dist/tx_lease_flaring.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
