// s2.js — permian-flaring's score-capped S2 flare catalogue (display-only).
// One row per H3 site, loaded as a parquet via DuckDB. Each site carries a
// `detections` array of per-date observations ({date, max_b12, pixels}) parsed
// from the embedded JSON column, used to draw the per-date timeline in the
// detail card. gaslight runs no S2 detection of its own — this just displays.

import * as db from './db.js?v=11';

let _sites = null; // Map<h3, site>
let _loaded = false;

export async function load() {
    try {
        await db.loadS2Precomputed();
        const all = await db.queryS2Precomputed();
        if (all.length === 0) return;

        _sites = new Map();
        for (const row of all) {
            let detections = [];
            try {
                detections = row.detections ? JSON.parse(row.detections) : [];
            } catch { /* leave empty on malformed JSON */ }
            _sites.set(row.h3, {
                id: row.h3,
                lon: row.lon,
                lat: row.lat,
                max_b12: row.max_b12,
                mean_max_b12: row.mean_max_b12,
                n_dates: row.n_dates,
                n_detections: row.n_detections,
                first_date: row.first_date,
                last_date: row.last_date,
                b12_b11_ratio: row.b12_b11_ratio ?? null,
                min_glint_score: row.min_glint_score ?? null,
                total_score: row.total_score ?? null,
                corroborated: row.corroborated ?? null,
                nearest_source: row.nearest_source ?? null,
                detections,
            });
        }

        _loaded = true;
        console.log(`[s2] ${_sites.size} S2 catalogue sites loaded`);
    } catch (e) {
        // No s2 parquet available — that's fine
        if (!e.message?.includes('Could not open') && !e.message?.includes('404')) {
            console.warn('[s2]', e.message);
        }
    }
}

// Look up a single site by its h3 id (for opening the detail card).
export function get(id) {
    return _sites ? _sites.get(id) : null;
}

// Query sites within a bbox [west, south, east, north].
export function query(bbox) {
    if (!_sites) return null;
    const [west, south, east, north] = bbox;
    const results = [];
    for (const s of _sites.values()) {
        if (s.lon >= west && s.lon <= east && s.lat >= south && s.lat <= north) {
            results.push(s);
        }
    }
    return results.length > 0 ? results : null;
}

export function getAll() {
    return _sites ? [..._sites.values()] : [];
}

export function isLoaded() { return _loaded; }
