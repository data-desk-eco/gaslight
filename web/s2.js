// s2.js — permian-flaring's score-capped S2 flare catalogue (display-only).
// One row per H3 site, loaded as a parquet via DuckDB (site-level metadata only).
// Per-date detections live in s2_detections.parquet and are fetched lazily by h3
// (db.queryS2Detections) when a detail card opens, to draw the per-date timeline.
// gaslight runs no S2 detection of its own — this just displays.

import * as db from './db.js?v=13';

let _sites = null; // Map<h3, site>
let _loaded = false;

export async function load() {
    try {
        await db.loadS2Precomputed();
        const all = await db.queryS2Precomputed();
        if (all.length === 0) return;

        _sites = new Map();
        for (const row of all) {
            _sites.set(row.h3, {
                id: row.h3,
                lon: row.lon,
                lat: row.lat,
                max_b12: row.max_b12,
                mean_max_b12: row.mean_max_b12,
                n_dates: row.n_dates,
                n_detections: row.n_detections,
                persistence_pct: row.persistence_pct ?? null,
                first_date: row.first_date,
                last_date: row.last_date,
                b12_b11_ratio: row.b12_b11_ratio ?? null,
                min_glint_score: row.min_glint_score ?? null,
                total_score: row.total_score ?? null,
                corroborated: row.corroborated ?? null,
                nearest_source: row.nearest_source ?? null,
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
