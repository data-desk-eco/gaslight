COPY (
    WITH raw AS (
        SELECT
            flare_id::INTEGER AS flare_id,
            Date_Mscan::DATE AS date,
            Lat_GMTCO::DOUBLE AS lat,
            Lon_GMTCO::DOUBLE AS lon,
            Cloud_Mask::INTEGER AS cloud_mask,
            QF_Detect::INTEGER AS qf_detect,
            CASE WHEN RH < 999999 THEN RH::DOUBLE END AS rh,
            CASE WHEN Temp_BB < 999999 THEN Temp_BB::DOUBLE END AS temp_bb
        FROM read_csv('data/vnf_profiles/site_*.csv',
            header=true, auto_detect=true, filename=false)
        WHERE Sunlit = 0
    )
    SELECT flare_id, date,
        AVG(lat) AS lat, AVG(lon) AS lon,
        BOOL_OR(cloud_mask = 0) AS clear,
        BOOL_OR(qf_detect > 0 AND qf_detect < 999999) AS detected,
        MAX(CASE WHEN qf_detect > 0 AND qf_detect < 999999 THEN rh END) AS rh_mw,
        MAX(CASE WHEN qf_detect > 0 AND qf_detect < 999999 THEN temp_bb END) AS temp_k,
        COUNT(*) AS n_passes
    FROM raw
    GROUP BY flare_id, date
) TO 'data/vnf.parquet' (FORMAT PARQUET);
