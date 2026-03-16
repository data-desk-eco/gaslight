-- s2.sql — Post-process raw S2 CLI detections into web-ready parquet.
-- Clips to Texas (excludes NM corner), applies burnoff-compatible quality filters.

SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

COPY (
    SELECT
        cluster_id, date::VARCHAR AS date, max_b12, pixels,
        det_lon, det_lat,
        cluster_lon, cluster_lat,
        cluster_max_b12, cluster_avg_b12, cluster_date_count,
        cluster_persistence, cluster_seasonal
    FROM 'data/s2-raw.csv'
    WHERE -- Clip to Texas portion of Permian (exclude NM corner)
        (cluster_lat <= 32.0 OR cluster_lon >= getvariable('nm_border_lon'))
        -- Quality filters (match burnoff defaults)
        AND cluster_date_count >= 4
        AND cluster_avg_b12 >= 0.85
) TO 'web/data/s2.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
