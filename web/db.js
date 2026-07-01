let db = null;
let conn = null;
let _initPromise = null;
const _loaded = new Set();
const _loading = new Map();
let _log = () => {}; // boot log callback
let _status = () => {}; // boot status callback
export function onLog(fn) { _log = fn; }
export function onStatus(fn) { _status = fn; }

const MATCH_RADIUS_KM = 0.375;
const BBOX_DELTA = 0.0034; // ~375m in degrees latitude

// Reusable SQL fragment for great-circle distance (km) between two lat/lon pairs
function distSql(latA, lonA, latB, lonB) {
    return `111.32 * sqrt(power((${latA} - ${latB}) * cos(radians(${latB})), 2) + power(${lonA} - ${lonB}, 2))`;
}

// Reusable SQL fragment for bounding-box pre-filter
function bboxSql(latCol, lonCol, lat, lon) {
    return `${latCol} BETWEEN ${lat} - ${BBOX_DELTA} AND ${lat} + ${BBOX_DELTA}
             AND ${lonCol} BETWEEN ${lon} - (${BBOX_DELTA} / cos(radians(${lat}))) AND ${lon} + (${BBOX_DELTA} / cos(radians(${lat})))`;
}

// Tier 0: needed for first paint (37K VNF sites)
// Tier 1: visible layers loaded right after first paint (~750K total)
// Tier 2: deferred until first query (vnf_detections 667K, gatherers 308K, production 288K, leases 44K)
const TIER0 = ['vnf'];
const TIER1 = ['permits', 'nmocd', 'plumes', 'facilities', 'wells'];

function _fmtSize(bytes) {
    return bytes < 1024 ? bytes + ' B'
        : bytes < 1048576 ? (bytes / 1024).toFixed(0) + ' KB'
        : (bytes / 1048576).toFixed(1) + ' MB';
}

async function _loadParquet(name) {
    if (_loaded.has(name)) return;
    if (_loading.has(name)) return _loading.get(name);
    const p = (async () => {
        _log(`fetch  data/${name}.parquet`);
        const resp = await fetch(`data/${name}.parquet`);
        const buf = await resp.arrayBuffer();
        _log(`load   ${name}.parquet (${_fmtSize(buf.byteLength)})`);
        await db.registerFileBuffer(`${name}.parquet`, new Uint8Array(buf));
        _loaded.add(name);
    })();
    _loading.set(name, p);
    return p;
}

// Ensure a parquet is loaded before querying it — no-ops if already loaded
async function need(...names) {
    await Promise.all(names.map(n => _loadParquet(n)));
}

export async function init() {
    if (_initPromise) return _initPromise;
    _initPromise = _init();
    return _initPromise;
}

// Start fetching tier 0 parquets immediately (before WASM is even requested)
const _prefetched = new Map();
for (const name of TIER0) {
    _prefetched.set(name, fetch(`data/${name}.parquet`).then(r => r.arrayBuffer()));
}

async function _init() {
    _status('importing duckdb module...');
    _log('import duckdb-browser.mjs');
    const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm');
    _log('import duckdb-browser.mjs done');
    const mainModule = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-eh.wasm';
    const mainWorker = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-eh.worker.js';
    _log('spawn  duckdb worker');
    const workerBlob = new Blob([`importScripts("${mainWorker}");`], { type: 'text/javascript' });
    const worker = new Worker(URL.createObjectURL(workerBlob));
    db = new duckdb.AsyncDuckDB({ log: () => {} }, worker);
    _status('downloading duckdb wasm (34 MB)...');
    _log('fetch  duckdb-eh.wasm (34 MB)');
    await db.instantiate(mainModule);
    _status('starting wasm runtime...');
    _log('instantiate wasm runtime');
    conn = await db.connect();
    _log('connect to duckdb');
    _status('loading flare data...');
    // Register prefetched tier 0 parquets (fetches started at module load)
    await Promise.all(TIER0.map(async n => {
        const buf = await _prefetched.get(n);
        _log(`load   ${n}.parquet (${_fmtSize(buf.byteLength)})`);
        await db.registerFileBuffer(`${n}.parquet`, new Uint8Array(buf));
        _loaded.add(n);
    }));
}

// Load tier 1 files in background (call after first paint)
export function loadTier1() {
    TIER1.forEach(n => _loadParquet(n));
}

// Load the S2 catalogue parquet (permian-flaring's score-capped sites; optional —
// may not exist if `make s2` hasn't run)
export async function loadS2Precomputed() {
    await _loadParquet('s2');
}

// One row per H3 site, ordered by score. Site-level metadata only — the per-date
// detections live in s2_detections.parquet, fetched lazily per site on card open.
export async function queryS2Precomputed() {
    const result = await query(`
        SELECT h3, lon, lat, n_detections, n_dates, n_clear_obs, persistence,
               first_date, last_date,
               max_b12, mean_max_b12, b12_b11_ratio, min_glint_score,
               total_score, corroborated, nearest_source
        FROM 's2.parquet' ORDER BY total_score DESC
    `);
    return rows(result);
}

// Per-date detections for one S2 site (date, max_b12), loaded lazily and queried
// by h3 — mirrors queryDetections for VNF. Drives the event list + timeline chart.
export async function queryS2Detections(h3) {
    await need('s2_detections');
    const id = String(h3).replace(/'/g, "''");
    const result = await query(`
        SELECT date, max_b12
        FROM 's2_detections.parquet'
        WHERE h3 = '${id}'
        ORDER BY date
    `);
    return rows(result);
}

function bboxDeltas(lat, radiusKm) {
    return {
        dLat: radiusKm / 110.54,
        dLon: radiusKm / (111.32 * Math.cos(lat * Math.PI / 180)),
    };
}

async function query(sql) {
    if (!conn) throw new Error('DB not initialized');
    return conn.query(sql);
}

function rows(result) {
    const n = result.numRows;
    if (n === 0) return [];
    const fields = result.schema.fields;
    // Column-based extraction — reads each column array once instead of per-row proxy access.
    // `col.toArray()` returns the raw (typed) value buffer and does NOT apply Arrow's null
    // bitmap, so null slots in numeric columns surface as uninitialized garbage (e.g. 1e+236).
    // That garbage poisons MapLibre's GeoJSON tile encoder ("Given varint doesn't fit into
    // 10 bytes"), blanking whole tiles. Carry the Vector for nullable columns so we can emit
    // a real null via isValid().
    const columns = fields.map(f => {
        const col = result.getChild(f.name);
        const arr = col.toArray();
        return {
            name: f.name,
            arr,
            bigint: arr.length > 0 && typeof arr[0] === 'bigint',
            nullable: col.nullCount > 0 ? col : null,
        };
    });
    const out = new Array(n);
    for (let i = 0; i < n; i++) {
        const obj = {};
        for (const { name, arr, bigint, nullable } of columns) {
            if (nullable && !nullable.isValid(i)) { obj[name] = null; continue; }
            const v = arr[i];
            obj[name] = bigint ? Number(v) : v;
        }
        out[i] = obj;
    }
    return out;
}

export async function queryFlares() {
    const result = await query(`
        SELECT f.flare_id, f.lat AS _lat, f.lon AS _lon,
            round(f.lat, 2) AS lat, round(f.lon, 2) AS lon,
            f.detection_days, f.total_rh_mw, f.avg_rh_mw, f.avg_flow_rate,
            f.first_detected, f.last_detected
        FROM 'vnf.parquet' f
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryDetections(flareId) {
    await need('vnf_detections');
    const result = await query(`
        SELECT date, rh_mw, flow_rate
        FROM 'vnf_detections.parquet'
        WHERE flare_id = ${Number(flareId)}
        ORDER BY date
    `);
    return rows(result);
}

export async function queryLeases(flareId) {
    await need('leases');
    const result = await query(`
        SELECT lease_district, lease_number, oil_gas_code, well_count,
            reported_flared_mcf, lease_operator, lease_name
        FROM 'leases.parquet'
        WHERE flare_id = ${Number(flareId)}
    `);
    return rows(result);
}

export async function queryLeaseMonthly(leaseDistrict, leaseNumber) {
    await need('production');
    const ld = leaseDistrict.replace(/'/g, "''");
    const ln = String(leaseNumber).replace(/'/g, "''");
    const result = await query(`
        SELECT date, flared_mcf, produced_mcf
        FROM 'production.parquet'
        WHERE lease_district = '${ld}'
          AND lease_number = '${ln}'
        ORDER BY date
    `);
    return rows(result);
}

export async function queryPermits() {
    await need('permits');
    const where = 'WHERE latitude IS NOT NULL AND longitude IS NOT NULL';
    const result = await query(`
        SELECT latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            name, county, district,
            release_type, operator_name,
            count(*) AS n_filings,
            MIN(effective_dt) AS earliest_effective,
            MAX(expiration_dt) AS latest_expiration,
            MAX(release_rate_mcf_day) AS max_release_rate_mcf_day
        FROM 'permits.parquet'
        ${where}
        GROUP BY _lat, _lon, name, county, district,
            release_type, operator_name
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryPermitFilings(lat, lon, { radiusKm = MATCH_RADIUS_KM, name, operator } = {}) {
    await need('permits');
    const dist = distSql('latitude', 'longitude', lat, lon);
    const bbox = bboxSql('latitude', 'longitude', lat, lon);
    let where = `${bbox} AND ${dist} <= ${radiusKm}`;
    if (name) where += ` AND name = '${name.replace(/'/g, "''")}'`;
    if (operator) where += ` AND operator_name = '${operator.replace(/'/g, "''")}'`;
    const result = await query(`
        SELECT filing_no, name, operator_name, district, county, release_type,
            status, effective_dt, expiration_dt, release_rate_mcf_day,
            exception_reasons, latitude, longitude,
            ${dist} AS distance_km
        FROM 'permits.parquet'
        WHERE ${where}
        ORDER BY effective_dt
    `);
    return rows(result);
}

export async function queryNearbyPermits(lat, lon, radiusKm = 0.75) {
    await need('permits');
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    const result = await query(`
        SELECT name, operator_name, district, county, release_type,
            effective_dt, expiration_dt, latitude, longitude
        FROM 'permits.parquet'
        WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
    `);
    return rows(result);
}

export async function queryNearestPermit(lat, lon, radiusKm = MATCH_RADIUS_KM) {
    await need('permits');
    const dist = distSql('latitude', 'longitude', lat, lon);
    const bbox = bboxSql('latitude', 'longitude', lat, lon);
    const result = await query(`
        SELECT name, operator_name, district, county, release_type,
            effective_dt, expiration_dt,
            latitude, longitude,
            ${dist} AS distance_km
        FROM 'permits.parquet'
        WHERE ${bbox}
        ORDER BY distance_km
        LIMIT 1
    `);
    const r = rows(result);
    return r.length > 0 ? r[0] : null;
}

export async function queryFacilities() {
    await need('facilities');
    const result = await query(`
        SELECT serial_number, facility_name, plant_type,
            latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude
        FROM 'facilities.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

export async function queryPlumes() {
    await need('plumes');
    const result = await query(`
        SELECT plume_id, latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            source, satellite,
            CAST(date AS VARCHAR) AS date,
            emission_rate, emission_uncertainty, sector
        FROM 'plumes.parquet'
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}

// NM OCD flare/vent notifications, already aggregated to sites in export.sql.
export async function queryNmocd() {
    await need('nmocd');
    const result = await query(`
        SELECT latitude AS _lat, longitude AS _lon,
            round(latitude, 4) AS latitude, round(longitude, 4) AS longitude,
            operator, facility_name, county,
            n_events, n_flare, n_vent, vented_mcf,
            CAST(first_date AS VARCHAR) AS first_date,
            CAST(last_date AS VARCHAR) AS last_date, incident_number
        FROM 'nmocd.parquet'
    `);
    return {
        type: 'FeatureCollection',
        features: rows(result).map(r => {
            const { _lat, _lon, ...props } = r;
            return { type: 'Feature', geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] }, properties: props };
        })
    };
}

export async function queryNearbyFacilities(lat, lon, radiusKm = 5) {
    await need('facilities');
    const { dLat, dLon } = bboxDeltas(lat, radiusKm);
    const dist = distSql('latitude', 'longitude', lat, lon);
    const result = await query(`
        SELECT serial_number, facility_name, plant_type, latitude, longitude,
            ${dist} AS distance_km
        FROM 'facilities.parquet'
        WHERE latitude BETWEEN ${lat - dLat} AND ${lat + dLat}
          AND longitude BETWEEN ${lon - dLon} AND ${lon + dLon}
          AND ${dist} <= ${radiusKm}
        ORDER BY distance_km
    `);
    return rows(result);
}

export async function queryGatherers(leaseDistrict, leaseNumber) {
    await need('gatherers');
    const ld = leaseDistrict.replace(/'/g, "''");
    const ln = String(leaseNumber).replace(/'/g, "''");
    const result = await query(`
        SELECT type, gpn_name, percentage, is_current, first_date, last_date
        FROM 'gatherers.parquet'
        WHERE district = '${ld}'
          AND lease_number = LPAD('${ln}', 6, '0')
        ORDER BY is_current DESC, last_date DESC NULLS LAST, type, percentage DESC
    `);
    return rows(result);
}

function _searchTerm(search) {
    if (!search) return null;
    const s = search.replace(/'/g, "''").replace(/%/g, '\\%').replace(/_/g, '\\_');
    return `'%${s}%' ESCAPE '\\'`;
}

function _searchWhere(layer, term) {
    if (!term) return null;
    switch (layer) {
        case 'flares':
            return `(CAST(first_detected AS VARCHAR) ILIKE ${term} OR CAST(last_detected AS VARCHAR) ILIKE ${term})`;
        case 'permits':
            return `(name ILIKE ${term} OR county ILIKE ${term} OR district ILIKE ${term} OR release_type ILIKE ${term} OR operator_name ILIKE ${term})`;
        case 'nmocd':
            return `(operator ILIKE ${term} OR facility_name ILIKE ${term} OR county ILIKE ${term})`;
        case 'plumes':
            return `(CAST(plume_id AS VARCHAR) ILIKE ${term} OR source ILIKE ${term} OR satellite ILIKE ${term} OR CAST(date AS VARCHAR) ILIKE ${term} OR sector ILIKE ${term})`;
        case 'wells':
            return `(CAST(api AS VARCHAR) ILIKE ${term} OR oil_gas_code ILIKE ${term} OR lease_district ILIKE ${term} OR CAST(lease_number AS VARCHAR) ILIKE ${term} OR CAST(well_number AS VARCHAR) ILIKE ${term} OR operator_name ILIKE ${term} OR lease_name ILIKE ${term})`;
        case 'infra':
            return `(CAST(serial_number AS VARCHAR) ILIKE ${term} OR facility_name ILIKE ${term} OR plant_type ILIKE ${term})`;
    }
    return null;
}

function _layerBaseSql(layer, where) {
    const w = where.length ? 'WHERE ' + where.join(' AND ') : '';
    switch (layer) {
        case 'flares':
            return `SELECT flare_id, lat, lon, round(lat, 2) AS lat_r, round(lon, 2) AS lon_r,
                detection_days, total_rh_mw, avg_rh_mw, first_detected, last_detected
                FROM 'vnf.parquet' ${w}`;
        case 'permits':
            return `SELECT latitude, longitude,
                round(latitude, 2) AS lat_r, round(longitude, 2) AS lon_r,
                name, county, district, release_type, operator_name,
                count(*) AS n_filings, MIN(effective_dt) AS earliest_effective,
                MAX(expiration_dt) AS latest_expiration, MAX(release_rate_mcf_day) AS max_release_rate_mcf_day
                FROM 'permits.parquet' ${w}
                GROUP BY latitude, longitude, name, county, district, release_type, operator_name`;
        case 'nmocd':
            return `SELECT latitude, longitude,
                round(latitude, 2) AS lat_r, round(longitude, 2) AS lon_r,
                operator, facility_name, county, n_events, n_flare, n_vent, vented_mcf,
                CAST(last_date AS VARCHAR) AS last_date
                FROM 'nmocd.parquet' ${w}`;
        case 'plumes':
            return `SELECT plume_id, latitude, longitude,
                round(latitude, 2) AS lat_r, round(longitude, 2) AS lon_r,
                source, satellite, CAST(date AS VARCHAR) AS date, emission_rate, emission_uncertainty, sector
                FROM 'plumes.parquet' ${w}`;
        case 'wells':
            return `SELECT api, oil_gas_code, lease_district, lease_number, well_number,
                operator_name, latitude, longitude,
                round(latitude, 2) AS lat_r, round(longitude, 2) AS lon_r,
                flared_mcf, produced_mcf, flaring_intensity_pct, lease_name
                FROM 'wells.parquet' ${w}`;
        case 'infra':
            return `SELECT serial_number, facility_name, plant_type,
                latitude, longitude,
                round(latitude, 2) AS lat_r, round(longitude, 2) AS lon_r
                FROM 'facilities.parquet' ${w}`;
    }
    return null;
}

const _latCol = { flares: 'lat', permits: 'latitude', nmocd: 'latitude', plumes: 'latitude', wells: 'latitude', infra: 'latitude' };
const _lonCol = { flares: 'lon', permits: 'longitude', nmocd: 'longitude', plumes: 'longitude', wells: 'longitude', infra: 'longitude' };

export async function queryDrawerRows(layer, bounds, search, sortCol, sortDir, limit = 1000) {
    const { south, north, west, east } = bounds;
    const term = _searchTerm(search);
    const lat = _latCol[layer], lon = _lonCol[layer];

    const where = [`${lat} BETWEEN ${south} AND ${north}`, `${lon} BETWEEN ${west} AND ${east}`];
    if (layer === 'permits') where.push('latitude IS NOT NULL', 'longitude IS NOT NULL');
    const sw = _searchWhere(layer, term);
    if (sw) where.push(sw);

    const baseSql = _layerBaseSql(layer, where);
    if (!baseSql) return { rows: [], total: 0 };

    const order = sortCol ? `ORDER BY "${sortCol}" ${sortDir === 'DESC' ? 'DESC' : 'ASC'}` : '';
    const sql = `WITH base AS (${baseSql})
        SELECT * EXCLUDE (${lat}, ${lon}, lat_r, lon_r),
            lat_r AS ${lat === 'lat' ? 'lat' : 'latitude'},
            lon_r AS ${lon === 'lon' ? 'lon' : 'longitude'},
            count(*) OVER() AS _total
        FROM base ${order} LIMIT ${limit}`;
    const result = await query(sql);
    const data = rows(result);
    const total = data.length > 0 ? data[0]._total : 0;
    for (const r of data) delete r._total;
    return { rows: data, total };
}

export async function queryMapSearch(layer, search) {
    const term = _searchTerm(search);
    const sw = _searchWhere(layer, term);
    if (!sw) return null;

    const lat = _latCol[layer], lon = _lonCol[layer];
    const where = [sw];
    if (layer === 'permits') where.push('latitude IS NOT NULL', 'longitude IS NOT NULL');

    const baseSql = _layerBaseSql(layer, where);
    if (!baseSql) return null;

    const sql = `WITH base AS (${baseSql})
        SELECT * EXCLUDE (lat_r, lon_r) FROM base`;
    const result = await query(sql);
    const data = rows(result);

    const latKey = lat, lonKey = lon;
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const rlat = Number(r[latKey]), rlon = Number(r[lonKey]);
            delete r[latKey]; delete r[lonKey];
            // Add rounded display coords as properties
            r[latKey === 'lat' ? 'lat' : 'latitude'] = Math.round(rlat * 100) / 100;
            r[lonKey === 'lon' ? 'lon' : 'longitude'] = Math.round(rlon * 100) / 100;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [rlon, rlat] },
                properties: r,
            };
        })
    };
}

export async function queryWells({ bounds } = {}) {
    await need('wells');
    const conditions = [];
    if (bounds) {
        conditions.push(`latitude BETWEEN ${bounds.south} AND ${bounds.north}`);
        conditions.push(`longitude BETWEEN ${bounds.west} AND ${bounds.east}`);
    }
    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
    const result = await query(`
        SELECT api, oil_gas_code, lease_district, lease_number, well_number,
            operator_name, latitude AS _lat, longitude AS _lon,
            round(latitude, 2) AS latitude, round(longitude, 2) AS longitude,
            flared_mcf, produced_mcf, flaring_intensity_pct, lease_name
        FROM 'wells.parquet'
        ${where}
    `);
    const data = rows(result);
    return {
        type: 'FeatureCollection',
        features: data.map(r => {
            const { _lat, _lon, ...props } = r;
            return {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [Number(_lon), Number(_lat)] },
                properties: props
            };
        })
    };
}
