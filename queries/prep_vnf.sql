COPY (
    SELECT flare_id::INTEGER AS flare_id,
        AVG(Lat_GMTCO::DOUBLE) AS lat, AVG(Lon_GMTCO::DOUBLE) AS lon,
        Date_Mscan::DATE AS date,
        BOOL_OR(Cloud_Mask::INT = 0) AS clear,
        BOOL_OR(QF_Detect::INT > 0 AND Sunlit::INT = 0) AS detected,
        MAX(CASE WHEN QF_Detect::INT > 0 AND Sunlit::INT = 0 AND RH != '999999'
            THEN RH::DOUBLE ELSE NULL END) / 1e6 AS rh_mw,
        MAX(CASE WHEN QF_Detect::INT > 0 AND Sunlit::INT = 0 AND Temp_BB != '999999'
            THEN Temp_BB::DOUBLE ELSE NULL END) AS temp_k,
        COUNT(*) AS n_passes
    FROM read_csv('data/vnf_profiles/site_*.csv',
        header=true, all_varchar=true, union_by_name=true)
    WHERE Sunlit = '0'
    GROUP BY flare_id::INTEGER, Date_Mscan::DATE
) TO 'data/vnf.parquet' (FORMAT PARQUET);
