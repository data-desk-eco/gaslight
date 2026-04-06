// precomputed.js — Pre-computed S2 flare detections (from CLI bulk runs).
// Loaded as a parquet via DuckDB, queried with SQL like all other gaslight data.

import * as db from './db.js?v=9';

let _clusters = null; // Map<clusterId, clusterObject>
let _loaded = false;

export async function load() {
    try {
        await db.loadS2Precomputed();
        const all = await db.queryS2Precomputed();
        if (all.length === 0) return;

        _clusters = new Map();
        for (const row of all) {
            const cid = row.cluster_id;
            if (!_clusters.has(cid)) {
                _clusters.set(cid, {
                    id: cid,
                    lon: row.cluster_lon,
                    lat: row.cluster_lat,
                    max_b12: row.cluster_max_b12,
                    avg_b12: row.cluster_avg_b12,
                    date_count: row.cluster_date_count,
                    persistence: row.cluster_persistence,
                    seasonal: row.cluster_seasonal,
                    // Glint discriminator fields (null on parquets predating the b11/sun pipeline)
                    median_b12_b11_ratio: row.cluster_median_b12_b11_ratio ?? null,
                    min_sun_elevation: row.cluster_min_sun_elevation ?? null,
                    likely_glint: row.cluster_likely_glint ?? null,
                    detection_count: 0,
                    detections: [],
                });
            }
            const c = _clusters.get(cid);
            c.detections.push({
                date: row.date,
                max_b12: row.max_b12,
                peak_b11: row.peak_b11 ?? null,
                pixels: row.pixels,
                sun_elevation: row.sun_elevation ?? null,
                sun_azimuth: row.sun_azimuth ?? null,
                lon: row.det_lon,
                lat: row.det_lat,
            });
            c.detection_count = c.detections.length;
        }

        // Compute first/last date per cluster
        for (const c of _clusters.values()) {
            c.first_date = c.detections.reduce((a, d) => d.date < a ? d.date : a, c.detections[0].date);
            c.last_date = c.detections.reduce((a, d) => d.date > a ? d.date : a, c.detections[0].date);
        }

        _loaded = true;
        console.log(`[precomputed] ${_clusters.size} S2 clusters from ${all.length} detections`);
    } catch (e) {
        // No precomputed parquet available — that's fine
        if (!e.message?.includes('Could not open') && !e.message?.includes('404')) {
            console.warn('[precomputed]', e.message);
        }
    }
}

// Query clusters within a bbox [west, south, east, north]
export function query(bbox) {
    if (!_clusters) return null;
    const [west, south, east, north] = bbox;
    const results = [];
    for (const c of _clusters.values()) {
        if (c.lon >= west && c.lon <= east && c.lat >= south && c.lat <= north) {
            results.push(c);
        }
    }
    return results.length > 0 ? results : null;
}

export function getAll() {
    return _clusters ? [..._clusters.values()] : [];
}

export function isLoaded() { return _loaded; }
