-- s2.sql — Transform openflaring's unified detection parquet into the
-- web-ready S2 parquet the map consumes (web/precomputed.js).
--
-- openflaring writes one row per S2 detection with a rich unified schema
-- (sensor, lon/lat, max_b12, max_b11, b12_b11_ratio, sun_*, glint_*,
-- cluster_id, persistence, total_score, …). Here we keep sensor='s2',
-- aggregate to cluster level, apply the Permian/Texas clip and the
-- detection-quality score gate, and emit the cluster_*/det_* columns
-- precomputed.js expects. The score gate lives here (not in the
-- openflaring run, which writes --score-threshold 0) so it can be
-- re-tuned without re-running detection.
--
-- Caller sets two variables (see Makefile):
--   of_parquet  — path to the openflaring output parquet
--   score_gate  — minimum cluster total_score to keep

SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

COPY (
    WITH s2 AS (
        SELECT * FROM read_parquet(getvariable('of_parquet'))
        WHERE sensor = 's2'
    ),
    agg AS (
        SELECT cluster_id,
            sum(lon * max_b12) / nullif(sum(max_b12), 0) AS cluster_lon,
            sum(lat * max_b12) / nullif(sum(max_b12), 0) AS cluster_lat,
            max(max_b12) AS cluster_max_b12,
            avg(max_b12) AS cluster_avg_b12,
            count(DISTINCT date) AS cluster_date_count,
            max(persistence) AS cluster_persistence,
            median(b12_b11_ratio) AS cluster_median_b12_b11_ratio,
            min(sun_elevation) AS cluster_min_sun_elevation,
            max(total_score) AS cluster_score
        FROM s2
        GROUP BY cluster_id
    )
    SELECT
        d.cluster_id,
        d.date::VARCHAR AS date,
        round(d.max_b12, 4) AS max_b12,
        round(d.max_b11, 4) AS peak_b11,
        d.pixels,
        round(d.sun_elevation, 2) AS sun_elevation,
        round(d.sun_azimuth, 2) AS sun_azimuth,
        round(d.lon, 6) AS det_lon,
        round(d.lat, 6) AS det_lat,
        round(a.cluster_lon, 6) AS cluster_lon,
        round(a.cluster_lat, 6) AS cluster_lat,
        round(a.cluster_max_b12, 4) AS cluster_max_b12,
        round(a.cluster_avg_b12, 4) AS cluster_avg_b12,
        a.cluster_date_count,
        round(a.cluster_persistence, 3) AS cluster_persistence,
        NULL::BOOLEAN AS cluster_seasonal,
        round(a.cluster_median_b12_b11_ratio, 3) AS cluster_median_b12_b11_ratio,
        round(a.cluster_min_sun_elevation, 2) AS cluster_min_sun_elevation,
        NULL::BOOLEAN AS cluster_likely_glint
    FROM s2 d
    JOIN agg a USING (cluster_id)
    -- Keep clusters snapped to known flare/facility infrastructure (cluster_id
    -- prefixed osm-/ogim-) regardless of score — they are confirmed real — plus
    -- unanchored s2-* clusters whose detection-quality score clears the gate.
    WHERE (
            d.cluster_id LIKE 'osm-%'
            OR d.cluster_id LIKE 'ogim-%'
            OR a.cluster_score >= getvariable('score_gate')
        )
        -- Permian bbox + Texas-only (exclude NM corner), on the cluster centroid
        AND a.cluster_lat BETWEEN 30.0 AND 33.5
        AND a.cluster_lon BETWEEN -104.5 AND -100.0
        AND (a.cluster_lat <= 32.0 OR a.cluster_lon >= getvariable('nm_border_lon'))
    ORDER BY d.cluster_id, d.date
) TO 'web/data/s2.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
