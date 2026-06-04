-- s2.sql — Refresh gaslight's local copy of the Sentinel-2 flare catalogue from
-- permian-flaring. Run by `make s2`; the upstream is the sibling permian-flaring
-- repo (the S2 detection engine + paper), the ONE place S2 detection happens.
--
-- This is a *fetch* step, symmetric to `make plumes` / `make r3`: it writes a
-- committed source file (`data/s2_catalogue.parquet`) that the normal pipeline
-- (load → publish → export) then ingests like every other source. gaslight does
-- no S2 detection of its own and, once this file is committed, rebuilds without
-- permian-flaring present.
--
-- permian-flaring's export is ALREADY the curated product set (its sql/70 ships
-- exactly the top 3,000 sites by total_score, basin-wide), so gaslight applies NO
-- quality filtering of its own — no score cut, no ranking. The ONE filter kept is
-- geographic: gaslight is a Texas product (its RRC / permit / well layers are
-- TX-only), so the NM Delaware corner is excluded. We otherwise ingest every site
-- p-f publishes and only re-round the columns for a tidy committed file. Each site
-- carries its per-date observations as a `detections` JSON array (date, max_b12,
-- pixels), preserving the timeline.
--
-- Caller sets one variable (see Makefile):
--   pf_catalogue — path to permian-flaring's s2_catalogue_detail.parquet
--
-- Time-window caveat: permian-flaring's S2 window is 2025-01-01..2026-05-31, so
-- this layer covers only 2025–2026 (the other gaslight layers are 2021+).

SET VARIABLE nm_border_lon = -103.064;  -- TX-NM border longitude (above 32°N)

COPY (
    SELECT
        h3,
        round(lon, 6) AS lon,
        round(lat, 6) AS lat,
        n_detections,
        n_dates,
        n_clear_obs,
        round(persistence, 3) AS persistence,
        first_date::VARCHAR AS first_date,
        last_date::VARCHAR AS last_date,
        round(max_b12, 3) AS max_b12,
        round(mean_max_b12, 3) AS mean_max_b12,
        round(b12_b11_ratio, 3) AS b12_b11_ratio,
        round(min_glint_score, 3) AS min_glint_score,
        round(total_score, 4) AS total_score,
        corroborated,
        nearest_source,
        detections
    FROM read_parquet(getvariable('pf_catalogue'))
    -- Texas-only: exclude the NM Delaware corner (above 32°N, west of the border).
    WHERE lat <= 32.0 OR lon >= getvariable('nm_border_lon')
    ORDER BY total_score DESC
) TO 'data/s2_catalogue.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
