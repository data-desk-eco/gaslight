import * as db from './db.js?v=13';
import * as s2 from './s2.js?v=3';
import * as drawer from './drawer.js?v=5';

// Boot screen log
const _bootLog = document.getElementById('boot-log');
const _bootScreen = document.getElementById('boot-screen');
const _bootStatus = document.getElementById('boot-status');
const _t0 = performance.now();

function bootLog(msg) {
    if (!_bootLog) return;
    const el = document.createElement('span');
    const ts = ((performance.now() - _t0) / 1000).toFixed(2).padStart(6);
    // Justify first word so subsequent text aligns (widest verb is "instantiate")
    const i = msg.indexOf(' ');
    const fmt = i > 0 ? (msg.slice(0, i) + ' ').padEnd(12) + msg.slice(i + 1).trimStart() : msg;
    el.textContent = `[${ts}s] ${fmt}`;
    _bootLog.appendChild(el);
    el.scrollIntoView({ block: 'end' });
}

let _statusTimer = 0;
function bootStatus(msg) {
    if (!_bootStatus) return;
    clearInterval(_statusTimer);
    if (!msg) { _bootStatus.textContent = ''; return; }
    const t0status = performance.now();
    const update = () => {
        const ts = ((performance.now() - _t0) / 1000).toFixed(2).padStart(6);
        const j = msg.indexOf(' ');
        const sfmt = j > 0 ? (msg.slice(0, j) + ' ').padEnd(12) + msg.slice(j + 1).trimStart() : msg;
        _bootStatus.textContent = `[${ts}s] ${sfmt}`;
    };
    update();
    _statusTimer = setInterval(update, 50);
}

function bootDone() {
    clearInterval(_statusTimer);
    bootLog('READY');
    _bootScreen.remove();
}

db.onLog(bootLog);
db.onStatus(bootStatus);
bootLog('gaslight / upstream flaring in the permian');
bootLog('');
bootLog('fetch  vnf.parquet (prefetch)');

const _css = k => getComputedStyle(document.documentElement).getPropertyValue(k).trim();
const COLORS = {
    flare: _css('--color-flare'),
    permit: _css('--color-permit'),
    plume: _css('--color-plume'),
    well: _css('--color-well'),
    infra: _css('--color-infra'),
};

// Geo constants
const LAT_PER_M = 1 / 110540;
const LON_PER_M = lat => 1 / (111320 * Math.cos(lat * Math.PI / 180));

const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

// Color ramps
function b12Color(b) {
    return b < 0.3 ? '#660800' : b < 0.5 ? '#991100' : b < 0.7 ? '#cc2200' : b < 0.9 ? '#ff4422' : b < 1.2 ? '#ff8844' : '#ffcc44';
}
function mwColor(mw) {
    return mw < 0.3 ? '#660800' : mw < 0.6 ? '#991100' : mw < 0.9 ? '#cc3300' : mw < 1.3 ? '#ff5522' : mw < 2 ? '#ff8844' : mw < 4 ? '#ffcc66' : '#ffeeaa';
}

function fmtCoords(lat, lon) {
    return `${Number(lat).toFixed(4)}, ${Number(lon).toFixed(4)}`;
}

// DOM cache for detail panel
const $ = id => document.getElementById(id);
function openDetail(title, lat, lon, body) {
    $('detail-title').textContent = title;
    $('detail-coords').textContent = fmtCoords(lat, lon);
    $('intensity-chart').innerHTML = '';
    removeS2Badge();
    $('detail-body').innerHTML = Array.isArray(body)
        ? body.flat(Infinity).filter(Boolean).join('')
        : body;
    const panel = $('detail-panel');
    panel.classList.remove('hidden');
    panel.scrollTop = 0;
}

let layerState = { flares: true, s2: true, permits: true, plumes: false, wells: false, infra: false };
let overlappingFeatures = [];
let overlapIndex = 0;
let flareFeatures = [];
const _originalSourceData = {}; // stashed per-layer source data for restoring after search

const map = new maplibregl.Map({
    container: 'map',
    style: {
        version: 8,
        glyphs: 'https://tiles.openfreemap.org/fonts/{fontstack}/{range}.pbf',
        sources: {
            satellite: {
                type: 'raster',
                tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
                tileSize: 256
            },
            labels: {
                type: 'vector',
                url: 'https://tiles.openfreemap.org/planet'
            }
        },
        layers: [
            { id: 'basemap', type: 'raster', source: 'satellite', paint: { 'raster-saturation': -1, 'raster-brightness-max': 0.65 } },
            {
                id: 'state-borders', type: 'line', source: 'labels', 'source-layer': 'boundary',
                filter: ['==', ['get', 'admin_level'], 4],
                paint: { 'line-color': 'rgba(255,255,255,0.15)', 'line-width': 0.5 }
            },
            {
                id: 'place-labels', type: 'symbol', source: 'labels', 'source-layer': 'place',
                filter: ['in', ['get', 'class'], ['literal', ['city', 'town']]],
                layout: { 'text-field': ['get', 'name:en'], 'text-font': ['Noto Sans Regular'], 'text-size': 11, 'text-anchor': 'center' },
                paint: { 'text-color': 'rgba(255,255,255,0.5)', 'text-halo-color': 'rgba(0,0,0,0.7)', 'text-halo-width': 1 }
            }
        ]
    },
    center: [-102.5, 31.8],
    zoom: 7,
    minZoom: 7,
    maxBounds: [[-107, 29], [-98, 35]],
    projection: 'globe',
    hash: 'map'
});


// Start DuckDB init immediately — runs in parallel with map tile loading
bootLog('init   duckdb wasm');
bootStatus('loading duckdb wasm runtime...');
const dbReady = db.init();
bootLog('init   maplibre gl');
bootStatus('waiting for map tiles...');

map.on('load', async () => {
    bootLog('map    tiles loaded');
    bootStatus('waiting for duckdb...');
    $('stat-sites').textContent = 'Loading...';

    await dbReady;
    bootLog('duckdb ready');

    bootLog('create map sources');
    addEmptySources();
    bootLog('create map layers');
    addLayers();
    bootLog('bindui event listeners');
    bindUI();
    restoreLayerHash();
    bootLog('query  vnf.parquet');
    bootStatus('querying VNF sites...');
    await refreshFlares();
    bootLog(`render ${flareFeatures.length.toLocaleString()} flare sites`);
    bootStatus('loading tier 1 data...');
    // Tier 1: start loading permits + plumes in background
    bootLog('tier1  fetch permits, plumes, wells, facilities');
    db.loadTier1();
    loadPermits();
    // Load permian-flaring's score-capped S2 catalogue before handling deep links
    await s2.load();
    if (s2.isLoaded()) {
        bootLog(`s2 ${s2.getAll().length} catalogue sites loaded`);
        loadS2Sites();
    }
    updateMapCentre();
    bootLog('init   data drawer');
    bootDone();
    // Stats use queryRenderedFeatures — wait for first idle after data loads
    map.once('idle', updateStats);

    drawer.init(map, {
        onQuery: (layer, bounds, search, sortCol, sortDir) =>
            db.queryDrawerRows(layer, bounds, search, sortCol, sortDir),
        onSearch: async (layer, term) => {
            const source = map.getSource(layer);
            if (!source) return;
            if (!term) {
                // Restore original data
                if (_originalSourceData[layer]) source.setData(_originalSourceData[layer]);
                return;
            }
            const fc = await db.queryMapSearch(layer, term);
            if (fc) source.setData(fc);
        },
        onState: ({ tab, q }) => {
            updateHash({ data: tab || undefined, q: q || undefined });
        },
        onSelect: (layer, row) => {
            const info = {
                flares: { latCol: 'lat', lonCol: 'lon' },
                permits: { latCol: 'latitude', lonCol: 'longitude' },
                plumes: { latCol: 'latitude', lonCol: 'longitude' },
                wells: { latCol: 'latitude', lonCol: 'longitude' },
                infra: { latCol: 'latitude', lonCol: 'longitude' },
            }[layer];
            if (!info) return;

            // Build a mock feature to reuse existing detail functions
            const lat = Number(row[info.latCol]);
            const lon = Number(row[info.lonCol]);
            const feature = {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [lon, lat] },
                properties: row,
                layer: { id: `${layer}-layer` },
            };
            overlappingFeatures = [feature];
            overlapIndex = 0;
            showFeatureDetail(feature);
        }
    });

    handleDeepLink();
});

function syncDrawer(f) {
    const lid = f.layer.id;
    const p = f.properties;
    if (lid.startsWith('flare')) drawer.highlight('flares-layer', String(p.flare_id));
    else if (lid.startsWith('permits')) drawer.highlight('permits-layer', `${p.latitude}_${p.longitude}_${p.name}`);
    else if (lid.startsWith('plumes')) drawer.highlight('plumes-layer', String(p.plume_id));
    else if (lid.startsWith('wells')) drawer.highlight('wells-layer', String(p.api));
    else if (lid.startsWith('infra')) drawer.highlight('infra-layer', String(p.serial_number));
}

function featureKey(f) {
    const p = f.properties;
    if (f.layer.id === 's2-points' && p.id) return `s2:${p.id}`;
    if (p.flare_id != null) return `flare:${p.flare_id}`;
    if (p.plume_id != null) return `plume:${p.plume_id}`;
    if (p.api != null) return `well:${p.api}`;
    if (p.serial_number != null && f.layer.id.startsWith('infra')) return `infra:${p.serial_number}`;
    if (p.name != null && p.latitude != null) return `permit:${p.latitude}_${p.longitude}_${p.name}`;
    return `${f.layer.id}:${f.id}`;
}

function addEmptySources() {
    const empty = { type: 'FeatureCollection', features: [] };
    map.addSource('texas', { type: 'geojson', data: 'data/texas.geojson' });
    map.addSource('flares', { type: 'geojson', data: empty });
    map.addSource('permits', { type: 'geojson', data: empty });
    map.addSource('plumes', { type: 'geojson', data: empty });
    map.addSource('wells', { type: 'geojson', data: empty });
    map.addSource('infra', { type: 'geojson', data: empty });
    map.addSource('flare-pixels', { type: 'geojson', data: empty });
    map.addSource('s2-detections', { type: 'geojson', data: empty });
}

function addWellImage() {
    const size = 24;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 5;
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(4, 4); ctx.lineTo(size - 4, size - 4);
    ctx.moveTo(size - 4, 4); ctx.lineTo(4, size - 4);
    ctx.stroke();
    const imgData = ctx.getImageData(0, 0, size, size);
    map.addImage('well-x', { width: size, height: size, data: imgData.data }, { sdf: true });
}

function addInfraImage() {
    const size = 24;
    const canvas = document.createElement('canvas');
    canvas.width = size;
    canvas.height = size;
    const ctx = canvas.getContext('2d');
    ctx.strokeStyle = '#fff';
    ctx.lineWidth = 4;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(size / 2, 4);
    ctx.lineTo(size - 4, size - 4);
    ctx.lineTo(4, size - 4);
    ctx.closePath();
    ctx.stroke();
    const imgData = ctx.getImageData(0, 0, size, size);
    map.addImage('infra-triangle', { width: size, height: size, data: imgData.data }, { sdf: true });
}

// Shrink a data-driven size expression at low zoom, ramping to full size by z12.
// MapLibre requires the zoom interpolation at top level, with the data expression
// (scaled per stop) embedded inside each stop — hence the wrapper rather than a multiply.
function zoomScale(sizeExpr, minScale = 0.45, z0 = 7, z1 = 12) {
    return ['interpolate', ['linear'], ['zoom'],
        z0, ['*', sizeExpr, minScale],
        z1, sizeExpr];
}

function addLayers() {
    // Flare radius: scale on total_rh_mw (MW)
    const flareRadius = zoomScale([
        'interpolate', ['linear'],
        ['coalesce', ['get', 'total_rh_mw'], 0],
        0, 2, 10, 4, 50, 7, 200, 12, 1000, 20, 5000, 32
    ]);

    map.addLayer({
        id: 'texas-border', type: 'line', source: 'texas',
        paint: { 'line-color': 'rgba(255,255,255,0.2)', 'line-width': 1 }
    });

    // Permit radius: sqrt-ish scale on max_release_rate_mcf_day (huge range, 3–680K)
    const permitRadius = zoomScale([
        'interpolate', ['linear'],
        ['coalesce', ['get', 'max_release_rate_mcf_day'], 0],
        0, 1.5, 100, 2, 1000, 3.5, 5000, 6, 25000, 10, 100000, 16
    ]);

    // Wells: fixed-size X markers, visible at z10+
    // Color by combined score: sqrt(intensity% × ln(1 + flared_mcf))
    // Weights both how wasteful (intensity) and how much gas (volume)
    addWellImage();
    const wellScore = ['sqrt', ['*',
        ['coalesce', ['get', 'flaring_intensity_pct'], 0],
        ['ln', ['+', 1, ['coalesce', ['get', 'flared_mcf'], 0]]]
    ]];
    const wellColor = [
        'interpolate', ['linear'], wellScore,
        0, '#776655',
        1, '#cc5522',
        4, '#e06628',
        8, '#ee7733',
        12, '#ff8844',
        16, '#ffaa55',
        22, '#ffcc44',
        30, '#ffeeaa'
    ];
    map.addLayer({
        id: 'wells-layer', type: 'symbol', source: 'wells',
        layout: {
            visibility: 'none',
            'icon-image': 'well-x',
            'icon-size': zoomScale(0.4),
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': wellColor,
            'icon-opacity': 0.85,
        }
    });

    // Infrastructure: R-3 gas processing facilities (triangle marker)
    addInfraImage();
    map.addLayer({
        id: 'infra-layer', type: 'symbol', source: 'infra',
        layout: {
            visibility: 'none',
            'icon-image': 'infra-triangle',
            'icon-size': zoomScale(0.45),
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': COLORS.infra,
            'icon-opacity': 0.85,
        }
    });

    map.addLayer({
        id: 'permits-layer', type: 'circle', source: 'permits',
        paint: {
            'circle-radius': permitRadius,
            'circle-color': COLORS.permit,
            'circle-opacity': 0.25,
            'circle-stroke-width': 1,
            'circle-stroke-color': COLORS.permit
        }
    });

    map.addLayer({
        id: 'plumes-layer', type: 'circle', source: 'plumes',
        layout: { visibility: 'none' },
        paint: { 'circle-radius': PLUME_RADIUS, 'circle-color': COLORS.plume, 'circle-opacity': 0.25, 'circle-stroke-width': 1, 'circle-stroke-color': COLORS.plume }
    });

    // Flare stroke color ramp by avg_rh_mw (p25=0.5, p50=0.8, p75=1.3, p90=2.1)
    const flareColorRamp = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'avg_rh_mw'], 0],
        0, '#660800', 0.3, '#991100', 0.6, '#cc2200', 0.9, '#ff4422', 1.3, '#ff8844', 2, '#ffcc44', 4, '#ffeeaa'
    ];

    // VIIRS M-band pixel footprint (750m square) — invisible fill for click target
    map.addLayer({
        id: 'flare-pixels-fill', type: 'fill', source: 'flare-pixels',
        paint: { 'fill-color': 'transparent' }
    });

    // Dashed outline — fades in as the flare dot fades out (zoom 13→15)
    map.addLayer({
        id: 'flare-pixels-layer', type: 'line', source: 'flare-pixels',
        paint: {
            'line-color': 'rgba(255,255,255,0.8)',
            'line-width': 1,
            'line-dasharray': [3, 2],
            'line-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1]
        }
    });

    // Label above pixel square (positioned at top-left corner)
    // Text scales with zoom so it stays proportional to the 750m box on the ground.
    // At z15 750m ≈ 200px, we want ~11px text. Doubling per zoom: 11 / 2^(15-z).
    map.addSource('flare-pixel-labels', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
    map.addLayer({
        id: 'flare-pixels-label', type: 'symbol', source: 'flare-pixel-labels',
        layout: {
            'text-field': 'FLARE DETECTION AREA',
            'text-font': ['Noto Sans Regular'],
            'text-size': ['interpolate', ['exponential', 2], ['zoom'], 13, 2.75, 15, 11, 17, 44],
            'text-anchor': 'bottom-left',
            'text-max-width': 999,
            'text-offset': [-0.1, -0.3]
        },
        minzoom: 13,
        paint: {
            'text-color': 'rgba(255,255,255,0.8)',
            'text-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1]
        }
    });

    map.addLayer({
        id: 'flares-layer', type: 'circle', source: 'flares',
        paint: {
            'circle-radius': flareRadius,
            'circle-color': flareColorRamp,
            'circle-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0.25, 15, 0],
            'circle-stroke-width': 1.5,
            'circle-stroke-color': flareColorRamp,
            'circle-stroke-opacity': ['interpolate', ['linear'], ['zoom'], 13, 1, 15, 0]
        }
    });

    // Sentinel-2 detection points — square markers, same color ramp as VNF
    // Two SDF images: solid fill + thin border, rendered as stacked symbol layers
    const sz = 24, bw = 2;
    const canvas = document.createElement('canvas');
    canvas.width = sz; canvas.height = sz;
    const ctx = canvas.getContext('2d');
    // Solid square
    ctx.fillStyle = '#fff';
    ctx.fillRect(0, 0, sz, sz);
    map.addImage('s2-square-fill', { width: sz, height: sz, data: new Uint8Array(ctx.getImageData(0, 0, sz, sz).data) }, { sdf: true });
    // Hollow square (thin border)
    ctx.clearRect(0, 0, sz, sz);
    ctx.fillStyle = '#fff';
    ctx.fillRect(0, 0, sz, sz);
    ctx.clearRect(bw, bw, sz - 2 * bw, sz - 2 * bw);
    map.addImage('s2-square-stroke', { width: sz, height: sz, data: new Uint8Array(ctx.getImageData(0, 0, sz, sz).data) }, { sdf: true });

    const s2ColorRamp = [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'max_b12'], 0],
        0.3, '#660800', 0.5, '#991100', 0.7, '#cc2200', 0.9, '#ff4422', 1.2, '#ff8844', 1.5, '#ffcc44'
    ];
    const s2IconSize = zoomScale(['interpolate', ['linear'],
        ['coalesce', ['get', 'max_b12'], 0],
        0.3, 0.35, 0.6, 0.55, 1.0, 0.8, 1.5, 1.1]);
    // Fill layer (semi-transparent)
    map.addLayer({
        id: 's2-points-fill',
        type: 'symbol',
        source: 's2-detections',
        layout: {
            'icon-image': 's2-square-fill',
            'icon-size': s2IconSize,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': s2ColorRamp,
            'icon-opacity': 0.25,
        },
    });
    // Stroke layer (thin border)
    map.addLayer({
        id: 's2-points',
        type: 'symbol',
        source: 's2-detections',
        layout: {
            'icon-image': 's2-square-stroke',
            'icon-size': s2IconSize,
            'icon-allow-overlap': true,
            'icon-ignore-placement': true,
        },
        paint: {
            'icon-color': s2ColorRamp,
            'icon-opacity': 0.9,
        },
    });

}

const PLUME_RADIUS = zoomScale(['interpolate', ['linear'], ['coalesce', ['get', 'emission_rate'], 100], 10, 3, 500, 8, 5000, 18]);

// Generate 750m square polygons and top-left label points from flare data
function flarePixelData(flareGeoJson) {
    const HALF_M = 375; // half of 750m pixel
    const squares = [];
    const labels = [];
    for (const f of flareGeoJson.features) {
        const [lon, lat] = f.geometry.coordinates;
        const dLat = HALF_M * LAT_PER_M;
        const dLon = HALF_M * LON_PER_M(lat);
        squares.push({
            type: 'Feature',
            geometry: {
                type: 'Polygon',
                coordinates: [[
                    [lon - dLon, lat - dLat],
                    [lon + dLon, lat - dLat],
                    [lon + dLon, lat + dLat],
                    [lon - dLon, lat + dLat],
                    [lon - dLon, lat - dLat]
                ]]
            },
            properties: f.properties
        });
        labels.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [lon - dLon, lat + dLat] },
            properties: f.properties
        });
    }
    return {
        squares: { type: 'FeatureCollection', features: squares },
        labels: { type: 'FeatureCollection', features: labels }
    };
}

let _pixelsBuilt = false;
let _latestFlareData = null;

function ensureFlarePixels() {
    if (_pixelsBuilt || !_latestFlareData) return;
    _pixelsBuilt = true;
    const px = flarePixelData(_latestFlareData);
    map.getSource('flare-pixels').setData(px.squares);
    map.getSource('flare-pixel-labels').setData(px.labels);
}

async function refreshFlares() {
    const data = await db.queryFlares();
    flareFeatures = data.features;
    _latestFlareData = data;
    _pixelsBuilt = false;
    _originalSourceData.flares = data;
    map.getSource('flares').setData(data);
    // Pixel squares invisible until z13 — build lazily
    if (map.getZoom() >= 12) ensureFlarePixels();
    // Drawer population doesn't affect map paint — defer
    setTimeout(() => drawer.setData('flares', data.features), 0);
}

async function loadPermits() {
    if (!layerState.permits) return;
    const data = await db.queryPermits();
    _originalSourceData.permits = data;
    map.getSource('permits').setData(data);
    setTimeout(() => drawer.setData('permits', data.features), 0);
}

async function loadPlumes() {
    if (!layerState.plumes) return;
    const data = await db.queryPlumes();
    _originalSourceData.plumes = data;
    map.getSource('plumes').setData(data);
    drawer.setData('plumes', data.features);
}

async function loadInfra() {
    if (!layerState.infra) return;
    const data = await db.queryFacilities();
    _originalSourceData.infra = data;
    map.getSource('infra').setData(data);
    drawer.setData('infra', data.features);
}

async function loadWells() {
    if (!layerState.wells) return;
    const b = map.getBounds();
    const bounds = { south: b.getSouth(), north: b.getNorth(), west: b.getWest(), east: b.getEast() };
    const data = await db.queryWells({ bounds });
    _originalSourceData.wells = data;
    map.getSource('wells').setData(data);
    drawer.setData('wells', data.features);
}


// Populate the S2 map source from the catalogue. The per-date `detections`
// array is deliberately omitted from the GeoJSON properties (kept light) — it's
// looked up by id via s2.get() when a site's detail card opens.
function loadS2Sites() {
    const sites = s2.getAll();
    if (sites.length === 0) return 0;
    const fc = {
        type: 'FeatureCollection',
        features: sites.map(d => ({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: [d.lon, d.lat] },
            properties: {
                id: d.id, lon: d.lon, lat: d.lat,
                max_b12: d.max_b12, mean_max_b12: d.mean_max_b12,
                n_detections: d.n_detections, n_dates: d.n_dates,
                first_date: d.first_date, last_date: d.last_date,
                total_score: d.total_score, corroborated: d.corroborated,
            },
        })),
    };
    map.getSource('s2-detections').setData(fc);
    drawer.setData('s2', fc.features);
    return sites.length;
}

function updateMapCentre() {
    const c = map.getCenter();
    $('map-centre').textContent = `${c.lat.toFixed(3)}, ${c.lng.toFixed(3)}`;
}

function updateStats() {
    const sites = map.queryRenderedFeatures({ layers: ['flares-layer'] }).length;
    const s2Sites = map.queryRenderedFeatures({ layers: ['s2-points'] }).length;
    $('stat-sites').textContent = sites.toLocaleString();
    $('stat-s2').textContent = s2Sites.toLocaleString();
}

const LAYER_MAP = {
    flares: ['flares-layer', 'flare-pixels-fill', 'flare-pixels-layer', 'flare-pixels-label'],
    s2: ['s2-points', 's2-points-fill'],
    permits: ['permits-layer'],
    plumes: ['plumes-layer'],
    wells: ['wells-layer'],
    infra: ['infra-layer']
};

// Sync map layer z-order to match the legend DOM order (top of list = rendered on top)
function syncLayerOrder() {
    const rows = [...document.querySelectorAll('.toggle-row[data-layer]')];
    // Reverse: first in DOM should render on top, so move it last (MapLibre draws later layers on top)
    const layerOrder = rows.map(r => r.dataset.layer).reverse();
    // Move each group's map layers in order, stacking them above the previous group
    let beforeId = undefined; // undefined = move to top of layer stack
    for (const group of layerOrder) {
        const mapLayers = LAYER_MAP[group];
        if (!mapLayers) continue;
        for (const id of mapLayers) {
            try { map.moveLayer(id, beforeId); } catch {}
        }
        beforeId = mapLayers[0];
    }
    saveLayerHash();
}

// Compact layer hash: l=FSpmIW — all layers in order, UPPERCASE=visible, lowercase=hidden
// Codes: f=flares s=s2 p=permits m=plumes i=infra w=wells
const _L = { f: 'flares', s: 's2', p: 'permits', m: 'plumes', i: 'infra', w: 'wells' };
const _Linv = Object.fromEntries(Object.entries(_L).map(([k, v]) => [v, k]));

function saveLayerHash() {
    const rows = [...document.querySelectorAll('.toggle-row[data-layer]')];
    const code = rows.map(r => {
        const c = _Linv[r.dataset.layer];
        return layerState[r.dataset.layer] ? c.toUpperCase() : c;
    }).join('');
    updateHash({ l: code });
}

function restoreLayerHash() {
    const hash = location.hash.replace(/^#/, '');
    const match = hash.split('&').find(p => p.startsWith('l='));
    if (!match) return;
    const code = decodeURIComponent(match.split('=')[1]);
    if (!code) return;
    const entries = [...code].map(c => ({ layer: _L[c.toLowerCase()], visible: c === c.toUpperCase() })).filter(e => e.layer);
    if (entries.length === 0) return;

    // Apply visibility
    for (const { layer, visible } of entries) {
        layerState[layer] = visible;
        for (const id of (LAYER_MAP[layer] || [])) {
            try { map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none'); } catch {}
        }
    }
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        row.querySelector('input').checked = layerState[row.dataset.layer];
    }

    // Reorder DOM to match hash order
    const group = document.querySelector('.filter-group');
    const rowMap = new Map();
    for (const row of group.querySelectorAll('.toggle-row[data-layer]')) {
        rowMap.set(row.dataset.layer, row);
    }
    for (const { layer } of entries) {
        const row = rowMap.get(layer);
        if (row) group.appendChild(row);
    }
    syncLayerOrder();
}

function setLayerVisibility(layer, visible) {
    layerState[layer] = visible;
    const vis = visible ? 'visible' : 'none';
    for (const id of LAYER_MAP[layer]) {
        map.setLayoutProperty(id, 'visibility', vis);
    }
    if (visible) {
        if (layer === 'permits') loadPermits();
        if (layer === 'plumes') loadPlumes();
        if (layer === 'wells') loadWells();
        if (layer === 'infra') loadInfra();
    }
}

const ALL_CLICK_LAYERS = [
    'flares-layer',
    'flare-pixels-fill',
    'flare-pixels-layer',
    's2-points',
    'permits-layer',
    'plumes-layer',
    'wells-layer',
    'infra-layer'
];

function bindUI() {
    // Build flare pixel squares lazily when user zooms in past z12
    map.on('zoom', () => { if (map.getZoom() >= 12) ensureFlarePixels(); });

    $('collapse-toggle').addEventListener('click', () => {
        $('left-panel').classList.toggle('collapsed');
    });
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const layer = row.dataset.layer;
        const cb = row.querySelector('input');
        cb.addEventListener('change', () => { setLayerVisibility(layer, cb.checked); saveLayerHash(); });
        row.querySelector('.filter-label').addEventListener('click', () => {
            cb.checked = !cb.checked;
            cb.dispatchEvent(new Event('change'));
        });
        // Drag-to-reorder
        row.draggable = true;
        row.addEventListener('dragstart', e => {
            e.dataTransfer.effectAllowed = 'move';
            row.classList.add('dragging');
        });
        row.addEventListener('dragend', () => {
            row.classList.remove('dragging');
            syncLayerOrder();
        });
        row.addEventListener('dragover', e => {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
            const dragging = document.querySelector('.toggle-row.dragging');
            if (!dragging || dragging === row) return;
            const rect = row.getBoundingClientRect();
            const after = e.clientY > rect.top + rect.height / 2;
            row.parentNode.insertBefore(dragging, after ? row.nextSibling : row);
        });
    }

    $('detail-close').addEventListener('click', closeDetail);
    document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDetail(); });

    // Overlap navigation
    $('overlap-prev').addEventListener('click', () => {
        if (overlappingFeatures.length < 2) return;
        overlapIndex = (overlapIndex - 1 + overlappingFeatures.length) % overlappingFeatures.length;
        showFeatureDetail(overlappingFeatures[overlapIndex]);
    });
    $('overlap-next').addEventListener('click', () => {
        if (overlappingFeatures.length < 2) return;
        overlapIndex = (overlapIndex + 1) % overlappingFeatures.length;
        showFeatureDetail(overlappingFeatures[overlapIndex]);
    });

    // Click-through: query all visible layers at click point
    map.on('click', e => {
        const tolerance = 10;
        const bbox = [
            [e.point.x - tolerance, e.point.y - tolerance],
            [e.point.x + tolerance, e.point.y + tolerance]
        ];
        const activeLayers = ALL_CLICK_LAYERS.filter(l =>
            map.getLayer(l) && map.getLayoutProperty(l, 'visibility') !== 'none'
        );
        const raw = map.queryRenderedFeatures(bbox, { layers: activeLayers });

        // If a feature is already selected, any click deselects — even on another feature
        if (overlappingFeatures.length > 0) {
            closeDetail();
            return;
        }

        if (raw.length === 0) {
            closeDetail();
            return;
        }

        // Deduplicate: pixel squares share flare_id with point layer — keep point, drop pixel dupes
        const PIXEL_LAYERS = new Set(['flare-pixels-fill', 'flare-pixels-layer']);
        const seen = new Set();
        const features = [];
        // Prefer point features: process non-pixel layers first
        const sorted = [...raw].sort((a, b) => (PIXEL_LAYERS.has(a.layer.id) ? 1 : 0) - (PIXEL_LAYERS.has(b.layer.id) ? 1 : 0));
        for (const f of sorted) {
            const key = featureKey(f);
            if (seen.has(key)) continue;
            seen.add(key);
            features.push(f);
        }

        if (features.length === 0) {
            closeDetail();
            return;
        }

        // Sort by distance to click (use properties for polygons)
        const featureCenter = f => {
            if (f.geometry.type === 'Point') return f.geometry.coordinates;
            return [Number(f.properties.lon), Number(f.properties.lat)];
        };
        features.sort((a, b) => {
            const [aLng, aLat] = featureCenter(a);
            const [bLng, bLat] = featureCenter(b);
            return Math.hypot(aLng - e.lngLat.lng, aLat - e.lngLat.lat)
                 - Math.hypot(bLng - e.lngLat.lng, bLat - e.lngLat.lat);
        });

        overlappingFeatures = features;
        overlapIndex = 0;
        showFeatureDetail(features[0]);

        syncDrawer(features[0]);
    });

    // Cursor changes for interactive layers
    for (const id of ALL_CLICK_LAYERS) {
        map.on('mouseenter', id, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', id, () => { map.getCanvas().style.cursor = ''; });
    }

    map.on('move', updateMapCentre);
    map.on('moveend', () => {
        updateStats();
        loadWells();
    });
}

// Hash param helpers — coexist with MapLibre's #map=zoom/lat/lon.
// Merge semantics: keys in `params` are overwritten (null/undefined removes),
// any existing keys not mentioned are preserved.
function updateHash(params) {
    const hash = location.hash.replace(/^#/, '');
    const existing = {};
    const order = [];
    for (const part of hash.split('&')) {
        const eq = part.indexOf('=');
        if (eq <= 0) continue;
        const k = part.slice(0, eq);
        if (!(k in existing)) order.push(k);
        existing[k] = part.slice(eq + 1); // keep raw (already encoded)
    }
    for (const [k, v] of Object.entries(params)) {
        if (v == null) {
            delete existing[k];
        } else {
            if (!(k in existing)) order.push(k);
            existing[k] = encodeURIComponent(v);
        }
    }
    const parts = order.filter(k => k in existing).map(k => `${k}=${existing[k]}`);
    history.replaceState(null, '', location.pathname + location.search + '#' + parts.join('&'));
}

// Explicitly clear the feature-selection params (keep drawer/q/l/map).
const SELECTION_KEYS = { vnf: null, plume: null, s2: null };

async function handleDeepLink() {
    const hash = location.hash.replace(/^#/, '');
    const params = {};
    for (const part of hash.split('&')) {
        const eq = part.indexOf('=');
        if (eq > 0) params[part.slice(0, eq)] = decodeURIComponent(part.slice(eq + 1));
    }

    // Drawer state — independent of feature selection, honour alongside
    if (params.data) {
        const layer = params.data;
        const cb = document.querySelector(`.toggle-row[data-layer="${layer}"] input`);
        if (cb && !cb.checked) {
            cb.checked = true;
            cb.dispatchEvent(new Event('change'));
        }
        drawer.openAt({ tab: layer, q: params.q || '', width: 480 });
    }

    if (params.s2) {
        const site = s2.get(params.s2);
        if (site) {
            updateHash({ s2: site.id });
            map.flyTo({ center: [site.lon, site.lat], zoom: 16 });
            showS2Detail(site);
        }
        return;
    }

    if (params.plume) {
        const cb = document.querySelector('.toggle-row[data-layer="plumes"] input');
        if (cb && !cb.checked) {
            cb.checked = true;
            cb.dispatchEvent(new Event('change'));
        }
        const data = await db.queryPlumes();
        _originalSourceData.plumes = data;
        map.getSource('plumes').setData(data);
        drawer.setData('plumes', data.features);
        const feature = data.features.find(f => String(f.properties.plume_id) === params.plume);
        if (!feature) return;
        const [lon, lat] = feature.geometry.coordinates;
        updateHash({ plume: params.plume });
        map.flyTo({ center: [lon, lat], zoom: 14 });
        feature.layer = { id: 'plumes-layer' };
        overlappingFeatures = [feature];
        overlapIndex = 0;
        showFeatureDetail(feature);
        return;
    }

    const flareId = params.vnf;
    if (!flareId) return;

    const feature = flareFeatures.find(f => String(f.properties.flare_id) === flareId);
    if (!feature) return;

    const [lon, lat] = feature.geometry.coordinates;
    updateHash({ vnf: flareId });
    map.flyTo({ center: [lon, lat], zoom: 14 });

    feature.layer = { id: 'flares-layer' };
    showFeatureDetail(feature);
    syncDrawer(feature);
}

function removeS2Badge() {
    const badge = $('s2-badge') || $('detail-badge');
    if (badge) { badge.id = 'detail-badge'; badge.classList.add('hidden'); }
}

// ---------------------------------------------------------------------------
// Selection visual state — dims map, highlights selected + associated features
// ---------------------------------------------------------------------------

// Default opacity values for each layer (must match addLayers paint values).
// Used by activateSelection/deactivateSelection to dim/restore layers.
const LAYER_DEFAULTS = {
    'flares-layer': {
        'circle-stroke-opacity': ['interpolate', ['linear'], ['zoom'], 13, 1, 15, 0],
        'circle-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0.25, 15, 0],
    },
    'flare-pixels-layer': {
        'line-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1],
    },
    'flare-pixels-label': {
        'text-opacity': ['interpolate', ['linear'], ['zoom'], 13, 0, 15, 1],
    },
    'permits-layer': { 'circle-stroke-opacity': 1, 'circle-opacity': 0.25 },
    'plumes-layer': { 'circle-stroke-opacity': 1, 'circle-opacity': 0.25 },
    'wells-layer': { 'icon-opacity': 0.85 },
    'infra-layer': { 'icon-opacity': 0.85 },
    's2-points': { 'icon-opacity': 1 },
};

// How much to scale each property type when dimmed (unselected).
// Pixel boxes/labels use 0 — only the selected flare's detection area is shown.
const DIM_RATIOS = {
    'circle-stroke-opacity': 0.15, 'circle-opacity': 0.2,
    'icon-opacity': 0.18,
    'line-opacity': 0, 'text-opacity': 0,
};

// Build a paint expression: matched features stay at base, rest are dimmed.
// For zoom-based interpolations, data-driven case is embedded inside each stop
// (MapLibre requires data expressions inside zoom interpolations, not outside).
function dimPaint(base, ratio, match) {
    if (typeof base === 'number') {
        if (!match) return base * ratio;
        return ['case', match, base, base * ratio];
    }
    // Zoom interpolation: ['interpolate', ['linear'], ['zoom'], z0, v0, z1, v1, ...]
    const result = [base[0], base[1], base[2]];
    for (let i = 3; i < base.length; i += 2) {
        const z = base[i], v = base[i + 1];
        result.push(z);
        result.push(!match ? v * ratio : ['case', match, v, v * ratio]);
    }
    return result;
}

function activateSelection({ flareId, permitProps, plumeId, wellApi, infraSerial, s2Id } = {}) {
    $('map-dim-overlay').classList.add('active');

    const flareMatch = flareId != null ? ['==', ['get', 'flare_id'], flareId] : null;
    const matches = {
        'flares-layer': flareMatch,
        'flare-pixels-layer': flareMatch,
        'flare-pixels-label': flareMatch,
        'permits-layer': permitProps ? ['all',
            ['==', ['get', 'latitude'], Number(permitProps.latitude)],
            ['==', ['get', 'longitude'], Number(permitProps.longitude)],
            ['==', ['get', 'name'], permitProps.name]] : null,
        'plumes-layer': plumeId != null ? ['==', ['get', 'plume_id'], plumeId] : null,
        'wells-layer': wellApi != null ? ['==', ['get', 'api'], wellApi] : null,
        'infra-layer': infraSerial != null ? ['==', ['get', 'serial_number'], infraSerial] : null,
        's2-points': s2Id != null ? ['==', ['get', 'id'], s2Id] : null,
    };

    for (const [layerId, defaults] of Object.entries(LAYER_DEFAULTS)) {
        if (!map.getLayer(layerId)) continue;
        const match = matches[layerId];
        for (const [prop, base] of Object.entries(defaults)) {
            map.setPaintProperty(layerId, prop, dimPaint(base, DIM_RATIOS[prop], match));
        }
    }
}

function deactivateSelection() {
    $('map-dim-overlay').classList.remove('active');
    for (const [layerId, defaults] of Object.entries(LAYER_DEFAULTS)) {
        if (!map.getLayer(layerId)) continue;
        for (const [prop, base] of Object.entries(defaults)) {
            map.setPaintProperty(layerId, prop, base);
        }
    }
}

function closeDetail() {
    removeS2Badge();
    updateHash(SELECTION_KEYS);
    $('detail-panel').classList.add('hidden');
    deactivateSelection();
    overlappingFeatures = [];
    overlapIndex = 0;
    drawer.highlight(null, null);
}

function showFeatureDetail(feature) {
    removeS2Badge();
    const layer = feature.layer.id;
    const p = feature.properties;

    // Build selection opts: each layer type identifies its selected feature differently
    const selOpts = {};
    if (p.flare_id != null) selOpts.flareId = p.flare_id;
    if (layer.startsWith('permits-')) selOpts.permitProps = { latitude: p.latitude, longitude: p.longitude, name: p.name };
    if (layer.startsWith('plumes-')) selOpts.plumeId = p.plume_id;
    if (layer.startsWith('wells-')) selOpts.wellApi = p.api;
    if (layer.startsWith('infra-')) selOpts.infraSerial = p.serial_number;
    activateSelection(selOpts);

    if (layer === 's2-points') {
        const site = s2.get(feature.properties.id);
        if (site) showS2Detail(site);
    } else {
        if (layer.startsWith('flare')) showFlareDetail(feature);
        else if (layer.startsWith('plumes-')) {
            updateHash({ plume: p.plume_id });
            showPlumeDetail(feature);
        }
        else {
            updateHash(SELECTION_KEYS);
            if (layer.startsWith('permits-')) showPermitDetail(feature);
            else if (layer.startsWith('wells-')) showWellDetail(feature);
            else if (layer.startsWith('infra-')) showInfraDetail(feature);
        }
    }

    updateOverlapNav();
}

function updateOverlapNav() {
    const layer0 = overlappingFeatures[0]?.layer?.id || '';
    const group = layer0.startsWith('permits-') ? 'permits-' : layer0.startsWith('plumes-') ? 'plumes-' : null;
    const nav = $('overlap-nav');
    if (group) {
        const grouped = overlappingFeatures.filter(f => f.layer.id.startsWith(group));
        if (grouped.length > 1) {
            overlappingFeatures = grouped;
            nav.classList.remove('hidden');
            $('overlap-count').textContent = `${overlapIndex + 1} / ${overlappingFeatures.length}`;
            return;
        }
    }
    nav.classList.add('hidden');
}

// ---------------------------------------------------------------------------
// Card building blocks — return HTML strings, compose via arrays in openDetail
// ---------------------------------------------------------------------------

function field(label, value) {
    return `<div class="detail-field"><span class="detail-field-label">${label}</span><span class="detail-field-value">${value}</span></div>`;
}

const card = {
    // Stats grid: [{value, unit, id?}, ...]
    stats: (items) => `<div class="stats-grid">${items.map(
        i => `<div class="stat"><div class="stat-big"${i.id ? ` id="${i.id}"` : ''}>${i.value}</div><div class="stat-unit">${i.unit}</div></div>`
    ).join('')}</div>`,

    // Key-value field row: [label, value] pairs. Falsy pairs and null values are filtered.
    fields: (...pairs) => {
        const html = pairs.filter(p => p && p[1] != null).map(([l, v]) => field(l, v)).join('');
        return html ? `<div class="detail-row">${html}</div>` : '';
    },

    // Section header
    header: (text) => `<div class="section-header">${text}</div>`,

    // Async placeholder — filled later via $('id').innerHTML = ...
    section: (id) => `<div id="${id}"></div>`,
};

// ---------------------------------------------------------------------------
// Shared data helpers for detail cards
// ---------------------------------------------------------------------------

// Distance, formatted as metres under 1km else km.
function fmtDist(distKm) {
    if (distKm == null) return '';
    return distKm < 1 ? `${(distKm * 1000).toFixed(0)} m` : `${distKm.toFixed(1)} km`;
}

function infraRow(name, meta, distKm) {
    const metaText = [meta, fmtDist(distKm)].filter(Boolean).join(' · ');
    return `<div class="gatherer-row">
            <span class="gatherer-name">${name}</span>
            <span class="gatherer-meta">${metaText}</span>
        </div>`;
}

// Nearby infrastructure — deliberately NOT a single attribution. Proximity
// within a satellite pixel is not ownership, so we list what's near the flare
// (permitted flares, gas plants) and let the reader judge, rather than naming
// one operator/facility with a confidence verdict.
function nearbyInfraHtml(facs, filings, firstDate, lastDate) {
    const parts = [card.fields(
        ['First detected', formatDate(firstDate)],
        ['Last detected', formatDate(lastDate)],
    )];

    // Permitted flares within the 375m match radius (deduped by site + operator)
    const seen = new Set();
    const permitRows = filings
        .slice()
        .sort((a, b) => (a.distance_km ?? 99) - (b.distance_km ?? 99))
        .filter(f => {
            const k = `${f.operator_name}|${f.name}`;
            if (seen.has(k)) return false;
            seen.add(k);
            return true;
        })
        .map(f => infraRow(
            f.operator_name || 'Unknown operator',
            [f.name, f.release_type].filter(Boolean).join(' · '),
            f.distance_km,
        )).join('');
    if (permitRows) {
        parts.push(card.header('Nearby permitted flares'));
        parts.push(`<div class="gatherer-list">${permitRows}</div>`);
    }

    // R-3 gas processing facilities within 5km
    if (facs.length) {
        const facRows = facs.map(f => infraRow(
            f.facility_name || 'Gas plant',
            f.plant_type || '',
            f.distance_km,
        )).join('');
        parts.push(card.header('Nearby gas plants'));
        parts.push(`<div class="gatherer-list">${facRows}</div>`);
    }
    return parts.join('');
}

// ---------------------------------------------------------------------------
// Detail cards
// ---------------------------------------------------------------------------

function showFlareDetail(feature) {
    const p = feature.properties;
    updateHash({ vnf: p.flare_id });

    const flow = Number(p.avg_flow_rate) > 0 ? Number(p.avg_flow_rate) : 0;
    openDetail(`VNF ${p.flare_id}`, p.lat, p.lon, [
        card.stats([
            { value: num(p.total_rh_mw), unit: 'total MW' },
            { value: num(p.detection_days), unit: 'detection days' },
            ...(flow > 0 ? [{ value: num(flow), unit: 'avg flow rate' }] : []),
        ]),
        card.section('vnf-operator-section'),
        card.section('vnf-lease-section'),
    ]);

    Promise.all([
        db.queryNearbyFacilities(Number(p.lat), Number(p.lon)),
        db.queryPermitFilings(Number(p.lat), Number(p.lon)),
    ]).then(([facs, filings]) => {
        const el = $('vnf-operator-section');
        if (el) el.innerHTML = nearbyInfraHtml(facs, filings, p.first_detected, p.last_detected);
    }).catch(() => {});

    db.queryLeases(p.flare_id).then(leases => {
        const el = $('vnf-lease-section');
        if (!el || leases.length === 0) return;
        // Group leases by operator
        const groups = new Map();
        for (const l of leases) {
            const op = l.lease_operator || 'Unknown operator';
            if (!groups.has(op)) groups.set(op, []);
            groups.get(op).push(l);
        }
        el.innerHTML = card.header('Nearby leases · by current operator') + [...groups.entries()].map(([op, grp]) => {
            const totalFlared = grp.reduce((s, l) => s + (Number(l.reported_flared_mcf) || 0), 0);
            const totalWells = grp.reduce((s, l) => s + (Number(l.well_count) || 0), 0);
            const leaseRows = grp.map(l => {
                const name = l.lease_name || `${l.lease_district}-${l.lease_number}`;
                const flared = Number(l.reported_flared_mcf) || 0;
                return `<div class="gatherer-row">
                    <span class="gatherer-name">${name}</span>
                    <span class="gatherer-meta">${num(l.well_count)} wells${flared > 0 ? ' · ' + flared.toLocaleString() + ' MCF' : ''}</span>
                </div>`;
            }).join('');
            return card.header(op) +
                card.fields(
                    ['Wells', num(totalWells)],
                    totalFlared > 0 && ['Reported flared', totalFlared.toLocaleString() + ' MCF'],
                ) +
                `<div class="gatherer-list"><details class="gatherer-history"><summary>${grp.length} lease${grp.length > 1 ? 's' : ''}</summary>${leaseRows}</details></div>`;
        }).join('');
    }).catch(() => {});

    db.queryDetections(p.flare_id).then(detections => {
        renderSparkline(detections);
    }).catch(() => {});
}

function showPermitDetail(feature) {
    const p = feature.properties;
    const rate = Number(p.max_release_rate_mcf_day);
    openDetail(p.name || 'Permit location', p.latitude, p.longitude, [
        card.stats([
            { value: rate > 0 ? rate.toLocaleString() : 'N/A', unit: 'max Mcf/day' },
            { value: Number(p.n_filings), unit: 'filings', id: 'permit-filings-count' },
        ]),
        card.fields(
            ['Operator', p.operator_name || 'N/A'],
            ['County', p.county || 'N/A'],
            ['District', p.district || 'N/A'],
            ['Release type', p.release_type || 'N/A'],
            ['Earliest effective', formatDate(p.earliest_effective)],
            ['Latest expiration', formatDate(p.latest_expiration)],
        ),
        card.section('permit-filings-section'),
    ]);

    db.queryPermitFilings(Number(p.latitude), Number(p.longitude), { radiusKm: 0.01, name: p.name, operator: p.operator_name }).then(filings => {
        const el = $('permit-filings-section');
        if (!el || filings.length === 0) return;
        const countEl = $('permit-filings-count');
        if (countEl) countEl.textContent = filings.length;
        el.innerHTML = `<div class="filings-list">${filings.map(f =>
            card.fields(
                ['Effective', formatDate(f.effective_dt)],
                ['Expiration', formatDate(f.expiration_dt)],
                f.status && ['Status', f.status],
                f.exception_reasons && ['Reasons', f.exception_reasons],
            )
        ).join('')}</div>`;
    }).catch(() => {});
}

function plumeUrl(source, id) {
    if (source === 'cm') return `https://data.carbonmapper.org/?plume_id=${encodeURIComponent(id)}`;
    if (source === 'imeo') return `https://methanedata.unep.org`;
    return null;
}

function showPlumeDetail(feature) {
    const p = feature.properties;
    const url = plumeUrl(p.source, p.plume_id);
    openDetail(p.plume_id, p.latitude, p.longitude, [
        card.stats([
            { value: Number(p.emission_rate).toLocaleString(), unit: 'kg/hr' },
            { value: `&plusmn;${Number(p.emission_uncertainty || 0).toLocaleString()}`, unit: 'uncertainty' },
        ]),
        card.fields(
            ['Source', p.source],
            ['Satellite', p.satellite || 'N/A'],
            ['Date', formatDate(p.date)],
            ['Sector', p.sector || 'N/A'],
        ),
    ]);
    if (url) $('detail-title').innerHTML = `<a href="${url}" target="_blank" rel="noopener" style="color: inherit; text-decoration: none;">${p.plume_id}</a>`;
}

function showWellDetail(feature) {
    const p = feature.properties;
    const flared = Number(p.flared_mcf) || 0;
    const produced = Number(p.produced_mcf) || 0;
    const intensity = p.flaring_intensity_pct != null ? Number(p.flaring_intensity_pct).toFixed(1) + '%' : 'N/A';
    const leaseId = p.lease_district && p.lease_number ? `${p.lease_district}-${p.lease_number}` : null;
    const leaseName = p.lease_name || leaseId;
    const hasLease = leaseId != null;

    openDetail(`Well ${p.api}`, p.latitude, p.longitude, [
        card.fields(
            ['Operator', p.operator_name || 'N/A'],
            ['Type', p.oil_gas_code === 'O' ? 'Oil' : p.oil_gas_code === 'G' ? 'Gas' : p.oil_gas_code || 'N/A'],
            ['District', p.lease_district || 'N/A'],
            ['Well #', p.well_number || 'N/A'],
        ),
        hasLease && [
            card.header(`Lease ${leaseName}`),
            card.fields(
                ['Gas flared (MCF)', flared > 0 ? flared.toLocaleString() : 'None reported'],
                ['Gas produced (MCF)', produced > 0 ? produced.toLocaleString() : 'None reported'],
                ['Flaring intensity', intensity],
            ),
            `<div id="well-lease-chart" class="intensity-chart"></div>`,
            card.header('Gatherers & Purchasers'),
            `<div id="well-gatherers" class="gatherer-list loading-placeholder">Loading\u2026</div>`,
        ],
    ]);

    if (hasLease) {
        db.queryLeaseMonthly(p.lease_district, p.lease_number).then(monthly => {
            const el = document.getElementById('well-lease-chart');
            if (!el || monthly.length === 0) return;
            renderLeaseChartIn(el, monthly);
        }).catch(() => {});

        db.queryGatherers(p.lease_district, p.lease_number).then(rows => {
            const el = document.getElementById('well-gatherers');
            if (!el || rows.length === 0) {
                if (el) el.innerHTML = '<span class="dim">No records</span>';
                return;
            }
            const current = rows.filter(r => r.is_current === '1');
            const currentKeys = new Set(current.map(r => r.type + '|' + r.gpn_name));
            const historical = rows.filter(r => r.is_current !== '1' && !currentKeys.has(r.type + '|' + r.gpn_name));
            const fmtDate = d => d ? d.slice(0, 7) : '';
            const renderRows = (list) => list.map(r => {
                const parts = [r.type];
                if (r.percentage) parts.push(r.percentage + '%');
                if (r.first_date) {
                    const from = fmtDate(r.first_date);
                    const to = fmtDate(r.last_date);
                    parts.push(from === to || !to ? from : `${from} – ${to}`);
                }
                return `<div class="gatherer-row">
                    <span class="gatherer-name">${r.gpn_name}</span>
                    <span class="gatherer-meta">${parts.join(' · ')}</span>
                </div>`;
            }).join('');
            el.innerHTML =
                (current.length ? renderRows(current) : '<div class="gatherer-row dim">None active</div>') +
                (historical.length ? `<details class="gatherer-history"><summary>${historical.length} historical</summary>${renderRows(historical)}</details>` : '');
        }).catch(() => {});
    }
}

function showInfraDetail(feature) {
    const p = feature.properties;
    openDetail(p.facility_name || 'Facility', p.latitude, p.longitude, [
        card.fields(
            ['Serial #', p.serial_number],
            p.plant_type && ['Type', p.plant_type],
        ),
    ]);
}

// Render an inline review callout for uncorroborated S2 sites whose spectral
// signature looks more like sun glint than combustion. Returns '' if nothing to
// flag (corroborated sites are confirmed real, so never flagged).
function glintNoticeHtml(site) {
    if (site.corroborated) return '';
    const notes = [];
    if (site.b12_b11_ratio != null && site.b12_b11_ratio < 1.15) {
        notes.push(`<strong>Review me.</strong> Peak B12/B11 ratio <code>${site.b12_b11_ratio.toFixed(2)}</code> — real flares emit at temperatures where this exceeds ~1.15. A solar reflection off metal is the typical cause below it.`);
    }
    if (notes.length === 0) return '';
    return `<div class="glint-notice">${notes.join('<br>')}</div>`;
}

// Static detail card for a permian-flaring catalogue site (no live detection).
function showS2Detail(site) {
    updateHash({ s2: site.id });
    activateSelection({ s2Id: site.id });

    const span = site.n_dates === 1
        ? `${site.n_dates} date · ${site.n_detections} detection${site.n_detections !== 1 ? 's' : ''}`
        : `${site.n_dates} dates · ${site.n_detections} detections`;

    openDetail('S2 flare site', site.lat, site.lon, [
        glintNoticeHtml(site),
        card.stats([
            { value: site.max_b12.toFixed(2), unit: 'peak B12' },
            { value: site.mean_max_b12.toFixed(2), unit: 'mean B12' },
        ]),
        card.fields(
            ['First detected', formatDate(site.first_date)],
            ['Last detected', formatDate(site.last_date)],
            ['Detections', span],
            site.persistence != null && ['Clear-sky persistence',
                `${Math.round(site.persistence * 100)}% (${site.n_dates}/${site.n_clear_obs} clear looks)`],
            site.b12_b11_ratio != null && ['Peak B12 / B11', site.b12_b11_ratio.toFixed(2)],
            site.min_glint_score != null && ['Min glint score', site.min_glint_score.toFixed(2)],
            site.total_score != null && ['Score', site.total_score.toFixed(2)],
            ['Corroboration', site.corroborated ? (site.nearest_source || 'yes') : 'none'],
        ),
        card.section('s2-permit-section'),
        `<div id="s2-event-list" class="s2-event-list"></div>`,
    ]);
    // The badge always shows clear-sky persistence; corroboration is conveyed by
    // the card's Corroboration field (and the site's own VNF/RRC/OSM/CM layers).
    const badge = $('detail-badge');
    badge.className = 'status-badge s2';
    badge.textContent = site.persistence != null
        ? `${Math.round(site.persistence * 100)}% persistent`
        : `${site.n_detections} det`;
    badge.classList.remove('hidden');
    $('overlap-nav').classList.add('hidden');

    // Per-date detections are fetched lazily by h3 (split out of the site
    // parquet to keep the boot payload small) — fill the event list + chart
    // once they arrive.
    db.queryS2Detections(site.id).then(dets => {
        if (!dets.length) return;
        const el = $('s2-event-list');
        if (el) {
            const sorted = dets.slice().sort((a, b) => b.date.localeCompare(a.date));
            el.innerHTML = sorted.map(d => `<div class="s2-event-item">
            <span class="s2-event-dot" style="background:${b12Color(d.max_b12)}"></span>
            <span class="s2-event-date">${formatDate(d.date)}</span>
            <span class="s2-event-b12">B12 ${d.max_b12.toFixed(2)}</span>
        </div>`).join('');
        }
        renderS2Chart(dets);
    }).catch(() => {});

    Promise.all([
        db.queryNearbyFacilities(site.lat, site.lon),
        db.queryPermitFilings(site.lat, site.lon),
    ]).then(([facs, filings]) => {
        const el = $('s2-permit-section');
        if (el) el.innerHTML = nearbyInfraHtml(facs, filings, site.first_date, site.last_date);
    }).catch(() => {});
}

// Shared timeline chart builder
function renderTimeline(detections, { valueKey, colorFn, scaleFn, gridStyle = 'months' } = {}) {
    const container = $('intensity-chart');
    if (!detections?.length) { container.innerHTML = ''; return; }

    const M = { top: 8, right: 8, bottom: 16, left: 8 };
    const width = container.clientWidth || 400, height = 100;
    const innerW = width - M.left - M.right;
    const innerH = height - M.top - M.bottom;

    const dates = detections.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates), maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${M.left}" y1="${height - M.bottom}" x2="${width - M.right}" y2="${height - M.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    const xOf = t => M.left + ((t - minDate) / dateRange) * innerW;

    if (gridStyle === 'months') {
        // Month gridlines with labels
        const charW = 5, minGap = 30;
        const startD = new Date(minDate), endD = new Date(maxDate);
        const startX = M.left, endX = width - M.right;
        const startLabel = `${MONTHS[startD.getMonth()]} ${startD.getFullYear()}`;
        svg += `<text x="${startX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="9" text-anchor="start">${startLabel}</text>`;
        let lastLabelX = startX + startLabel.length * charW;
        const firstMonth = new Date(startD);
        firstMonth.setDate(1);
        firstMonth.setMonth(firstMonth.getMonth() + 1);
        for (let d = new Date(firstMonth); d <= endD; d.setMonth(d.getMonth() + 1)) {
            const x = xOf(d.getTime());
            const isJan = d.getMonth() === 0;
            svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${height - M.bottom}" stroke="rgba(255,255,255,${isJan ? 0.15 : 0.06})" stroke-width="1"/>`;
            const label = isJan ? `${MONTHS[0]} ${d.getFullYear()}` : MONTHS[d.getMonth()];
            const labelW = label.length * charW;
            if (x - labelW / 2 > lastLabelX + minGap && x + labelW / 2 < endX - minGap) {
                svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,${isJan ? 0.4 : 0.25})" font-size="9" text-anchor="middle">${label}</text>`;
                lastLabelX = x + labelW / 2;
            }
        }
        const endLabel = `${MONTHS[endD.getMonth()]} ${endD.getFullYear()}`;
        if (endLabel !== startLabel && endX - endLabel.length * charW > lastLabelX + minGap)
            svg += `<text x="${endX}" y="${height - 2}" fill="rgba(255,255,255,0.35)" font-size="9" text-anchor="end">${endLabel}</text>`;
    } else {
        // Year-only gridlines
        const firstYear = new Date(minDate).getFullYear(), lastYear = new Date(maxDate).getFullYear();
        for (let y = firstYear + 1; y <= lastYear; y++) {
            const x = xOf(new Date(y, 0, 1).getTime());
            svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${height - M.bottom}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
            svg += `<text x="${x}" y="${height - 2}" fill="rgba(255,255,255,0.3)" font-size="10" text-anchor="middle">${y}</text>`;
        }
    }

    detections.forEach(det => {
        const x = xOf(new Date(det.date).getTime());
        const t = scaleFn(det);
        const y = M.top + innerH - t * innerH;
        svg += `<circle class="chart-dot" cx="${x}" cy="${y}" r="2" fill="${colorFn(det[valueKey])}" opacity="0.8"/>`;
    });

    svg += '</svg>';
    container.innerHTML = svg;
}

function renderS2Chart(detections) {
    const maxVal = Math.max(1.5, ...detections.map(d => d.max_b12));
    renderTimeline(detections, {
        valueKey: 'max_b12', colorFn: b12Color, gridStyle: 'months',
        scaleFn: d => Math.min(1, d.max_b12 / maxVal),
    });
}

function renderSparkline(detections) {
    const vals = detections.map(d => d.rh_mw).filter(v => v > 0);
    const lo = 0.1, hi = Math.max(10, ...vals);
    const logLo = Math.log(lo), logRange = Math.log(hi) - logLo;
    renderTimeline(detections, {
        valueKey: 'rh_mw', colorFn: mwColor, gridStyle: 'years',
        scaleFn: d => d.rh_mw > 0 ? Math.max(0, Math.min(1, (Math.log(Math.max(lo, d.rh_mw)) - logLo) / logRange)) : 0,
    });
}

function renderLeaseChartIn(container, monthly) {
    if (!monthly?.length) { container.innerHTML = ''; return; }

    const M = { top: 4, right: 8, bottom: 14, left: 8 };
    const width = container.clientWidth || 400, chartH = 80, legendH = 20;
    const height = chartH + legendH;
    const innerW = width - M.left - M.right;
    const innerH = chartH - M.top - M.bottom;

    const dates = monthly.map(d => new Date(d.date).getTime());
    const minDate = Math.min(...dates), maxDate = Math.max(...dates);
    const dateRange = maxDate - minDate || 1;
    const xOf = t => M.left + ((t - minDate) / dateRange) * innerW;

    const maxProd = Math.max(1, ...monthly.map(d => Math.max(d.produced_mcf || 0, d.flared_mcf || 0)));
    const yOf = v => M.top + innerH - (Math.min(v, maxProd) / maxProd) * innerH;

    let svg = `<svg viewBox="0 0 ${width} ${height}">`;
    svg += `<line x1="${M.left}" y1="${chartH - M.bottom}" x2="${width - M.right}" y2="${chartH - M.bottom}" stroke="rgba(255,255,255,0.15)" stroke-width="1"/>`;

    const startD = new Date(minDate), endD = new Date(maxDate);
    const firstYear = startD.getFullYear(), lastYear = endD.getFullYear();

    // Year gridlines — track positions to avoid overlapping end labels
    const charW = 6, minGap = 30;
    let firstYearX = Infinity, lastYearX = -Infinity;
    for (let y = firstYear + 1; y <= lastYear; y++) {
        const jan = new Date(y, 0, 1).getTime();
        if (jan <= minDate || jan >= maxDate) continue;
        const x = xOf(jan);
        if (x < firstYearX) firstYearX = x;
        if (x > lastYearX) lastYearX = x;
        svg += `<line x1="${x}" y1="${M.top}" x2="${x}" y2="${chartH - M.bottom}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
        svg += `<text x="${x}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="middle">${y}</text>`;
    }

    // Start/end date labels — skip if too close to a year gridline label
    const fmtLabel = d => `${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
    const startLabel = fmtLabel(startD), endLabel = fmtLabel(endD);
    const startLabelEnd = M.left + startLabel.length * charW;
    const endLabelStart = (width - M.right) - endLabel.length * charW;
    if (startLabelEnd + minGap < firstYearX)
        svg += `<text x="${M.left}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="start">${startLabel}</text>`;
    if (endLabel !== startLabel && endLabelStart - minGap > lastYearX)
        svg += `<text x="${width - M.right}" y="${chartH - 1}" fill="rgba(255,255,255,0.3)" font-size="11" text-anchor="end">${endLabel}</text>`;

    const prodPoints = monthly.map(d => {
        const x = xOf(new Date(d.date).getTime());
        return `${x},${yOf(d.produced_mcf || 0)}`;
    });
    const baseline = `${xOf(dates[dates.length - 1])},${yOf(0)} ${xOf(dates[0])},${yOf(0)}`;
    svg += `<polygon points="${prodPoints.join(' ')} ${baseline}" fill="rgba(255,255,255,0.08)"/>`;
    svg += `<polyline points="${prodPoints.join(' ')}" fill="none" stroke="rgba(255,255,255,0.3)" stroke-width="1"/>`;

    const flaredPoints = monthly.map(d => {
        const x = xOf(new Date(d.date).getTime());
        return `${x},${yOf(d.flared_mcf || 0)}`;
    });
    svg += `<polygon points="${flaredPoints.join(' ')} ${baseline}" fill="rgba(255,100,50,0.2)"/>`;
    svg += `<polyline points="${flaredPoints.join(' ')}" fill="none" stroke="${COLORS.flare}" stroke-width="1.5"/>`;

    const legendY = chartH + 14;
    const mid = width / 2;
    svg += `<line x1="${mid - 68}" y1="${legendY - 3}" x2="${mid - 56}" y2="${legendY - 3}" stroke="${COLORS.flare}" stroke-width="2"/>`;
    svg += `<text x="${mid - 53}" y="${legendY}" fill="${COLORS.flare}" font-size="10">Flared</text>`;
    svg += `<line x1="${mid + 10}" y1="${legendY - 3}" x2="${mid + 22}" y2="${legendY - 3}" stroke="rgba(255,255,255,0.3)" stroke-width="2"/>`;
    svg += `<text x="${mid + 25}" y="${legendY}" fill="rgba(255,255,255,0.4)" font-size="10">Produced</text>`;

    svg += '</svg>';
    container.innerHTML = svg;
}

function num(v) {
    const n = Number(v);
    return isNaN(n) || v == null ? '--' : n.toLocaleString();
}

function formatDate(d) {
    if (!d || d === 'null') return 'N/A';
    return String(d).slice(0, 10);
}
