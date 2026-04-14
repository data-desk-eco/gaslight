const _css = k => getComputedStyle(document.documentElement).getPropertyValue(k).trim();

const LAYERS = {
    flares:  { label: 'VNF',     color: () => _css('--color-flare'),  latCol: 'lat',       lonCol: 'lon',       idCol: 'flare_id' },
    s2:      { label: 'S2',      color: () => _css('--color-flare'),  latCol: 'lat',       lonCol: 'lon',       idCol: 'id' },
    permits: { label: 'Permits', color: () => _css('--color-permit'), latCol: 'latitude',  lonCol: 'longitude',  idCol: null },
    plumes:  { label: 'Plumes',  color: () => _css('--color-plume'),  latCol: 'latitude',  lonCol: 'longitude',  idCol: 'plume_id' },
    wells:   { label: 'Wells',   color: () => _css('--color-well'),   latCol: 'latitude', lonCol: 'longitude', idCol: 'api' },
    infra:   { label: 'Infrastructure', color: () => _css('--color-infra'), latCol: 'latitude', lonCol: 'longitude', idCol: 'serial_number' },
};

const TAB_MAP = {
    'flares-layer': 'flares', 'flare-pixels-fill': 'flares', 'flare-pixels-layer': 'flares',
    's2-points': 's2', 's2-points-fill': 's2',
    'permits-layer': 'permits', 'plumes-layer': 'plumes', 'wells-layer': 'wells', 'infra-layer': 'infra',
};

const MIN_WIDTH = 300;
const MAX_ROWS = 1000;

function rowCoords(row, info) {
    return [Number(row[info.latCol]), Number(row[info.lonCol])];
}

function esc(v) {
    return String(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

let map = null;
let drawerEl = null;
let handleEl = null;
let tableEl = null;
let tabBarEl = null;
let searchEl = null;
let footerEl = null;
let drawerWidth = 0;
let activeTab = null;
let currentRows = [];
let currentTotalCount = 0;
let selectedId = null;
let selectedIdx = -1;
let sortCol = null;
let sortDir = 'ASC';
let searchTerm = '';
let _queryGen = 0;

// Feature data pushed from app.js after each load
const allData = {};
// Pre-extracted numeric coords for fast viewport filtering (parallel arrays per layer)
const _coords = {};

// Callbacks set by app.js
let onRowClick = null;
let _queryFn = null;
let _onSearch = null;
let _onState = null;

function emitState() {
    if (_onState) _onState({ tab: drawerWidth >= MIN_WIDTH ? activeTab : null, q: searchTerm || null });
}

export function init(mapInstance, { onSelect, onQuery, onSearch, onState } = {}) {
    map = mapInstance;
    onRowClick = onSelect || null;
    _queryFn = onQuery || null;
    _onSearch = onSearch || null;
    _onState = onState || null;
    createDOM();
    bindDrag();
    bindLayerToggles();
    bindMapEvents();
    bindKeyboard();
}

// Called by app.js after loading data for a layer
export function setData(layer, features) {
    const props = features.map(f => f.properties);
    allData[layer] = props;
    // Pre-extract numeric coords into typed arrays for fast viewport filtering
    const info = LAYERS[layer];
    const n = props.length;
    const lats = new Float64Array(n);
    const lons = new Float64Array(n);
    for (let i = 0; i < n; i++) {
        lats[i] = +props[i][info.latCol];
        lons[i] = +props[i][info.lonCol];
    }
    _coords[layer] = { lats, lons };
    if (layer === activeTab && drawerWidth >= MIN_WIDTH) refreshTable();
}

function createDOM() {
    drawerEl = document.createElement('div');
    drawerEl.id = 'data-drawer';
    drawerEl.tabIndex = -1; // focusable but not in tab order

    const headerEl = document.createElement('div');
    headerEl.className = 'drawer-header';

    tabBarEl = document.createElement('div');
    tabBarEl.className = 'drawer-tab-bar';
    headerEl.appendChild(tabBarEl);

    searchEl = document.createElement('input');
    searchEl.type = 'text';
    searchEl.className = 'drawer-search';
    searchEl.placeholder = 'Search\u2026';
    searchEl.addEventListener('input', () => {
        searchTerm = searchEl.value.trim();
        if (_onSearch) _onSearch(activeTab, searchTerm);
        refreshTable();
        emitState();
    });
    headerEl.appendChild(searchEl);

    drawerEl.appendChild(headerEl);

    const tableContainer = document.createElement('div');
    tableContainer.className = 'drawer-table-wrap';
    tableEl = document.createElement('table');
    tableEl.className = 'drawer-table';
    tableContainer.appendChild(tableEl);
    drawerEl.appendChild(tableContainer);

    footerEl = document.createElement('div');
    footerEl.className = 'drawer-footer';
    drawerEl.appendChild(footerEl);

    handleEl = document.createElement('div');
    handleEl.id = 'drawer-handle';
    handleEl.innerHTML = '<div class="handle-bar"></div><div class="handle-label">data table</div>';

    document.body.appendChild(drawerEl);
    document.body.appendChild(handleEl);
}

// Open drawer from a deep link: set width, active tab, search term.
// Caller is responsible for toggling the underlying layer visible first.
export function openAt({ tab, q, width } = {}) {
    if (width && width >= MIN_WIDTH) setDrawerWidth(width);
    updateTabs();
    if (tab && LAYERS[tab] && getVisibleLayers().includes(tab)) {
        activeTab = tab;
        selectedId = null; selectedIdx = -1;
        updateTabs();
    }
    if (q != null && searchEl) {
        searchEl.value = q;
        searchTerm = q;
        if (_onSearch) _onSearch(activeTab, searchTerm);
    }
    refreshTable();
}

function setDrawerWidth(w) {
    drawerWidth = w;
    drawerEl.style.width = w + 'px';
    handleEl.style.left = w + 'px';

    // Map stays full-width; satellite tiles render under the drawer.
    // setPadding shifts the logical centre so pan/zoom feel correct.
    map.setPadding({ left: w });

    // Keep left panel in sync
    const leftPanel = document.getElementById('left-panel');
    if (leftPanel) leftPanel.style.left = (w + 16) + 'px';
}

function bindDrag() {
    let startX = 0;
    let startWidth = 0;
    let dragging = false;

    // Pre-render table on hover so data is ready before drag starts
    let preloaded = false;
    handleEl.addEventListener('pointerenter', () => {
        if (preloaded || drawerWidth >= MIN_WIDTH) return;
        preloaded = true;
        ensureActiveTab();
        refreshTable();
    });
    handleEl.addEventListener('pointerleave', () => { preloaded = false; });

    handleEl.addEventListener('pointerdown', e => {
        e.preventDefault();
        handleEl.setPointerCapture(e.pointerId);
        startX = e.clientX;
        startWidth = drawerWidth;
        dragging = true;
        drawerEl.style.transition = 'none';

        ensureActiveTab();
    });

    let refreshRAF = null;

    handleEl.addEventListener('pointermove', e => {
        if (!dragging) return;
        const maxW = window.innerWidth - 400;
        const newW = Math.max(0, Math.min(maxW, startWidth + (e.clientX - startX)));
        setDrawerWidth(newW);

        // Live-render table while dragging, throttled to one per frame
        if (newW >= MIN_WIDTH && !refreshRAF) {
            refreshRAF = requestAnimationFrame(() => {
                refreshRAF = null;
                refreshTable();
            });
        }
    });

    const endDrag = () => {
        if (!dragging) return;
        dragging = false;
        if (refreshRAF) { cancelAnimationFrame(refreshRAF); refreshRAF = null; }
        drawerEl.style.transition = 'width 0.2s';

        if (drawerWidth > 0 && drawerWidth < MIN_WIDTH) {
            setDrawerWidth(0);
        } else if (drawerWidth >= MIN_WIDTH) {
            refreshTable();
        }
        emitState();
    };

    handleEl.addEventListener('pointerup', endDrag);
    handleEl.addEventListener('pointercancel', endDrag);
}

function bindLayerToggles() {
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const cb = row.querySelector('input');
        cb.addEventListener('change', () => {
            updateTabs();
            if (drawerWidth >= MIN_WIDTH) refreshTable();
        });
    }
}

function bindMapEvents() {
    let rafId = null;
    const LIVE_MIN_ZOOM = 8; // below this zoom, too many features — only update on moveend
    map.on('move', () => {
        if (drawerWidth < MIN_WIDTH || rafId) return;
        if (searchTerm || map.getZoom() < LIVE_MIN_ZOOM) return;
        rafId = requestAnimationFrame(() => {
            rafId = null;
            refreshTable();
        });
    });
    map.on('moveend', () => {
        if (drawerWidth < MIN_WIDTH) return;
        if (rafId) { cancelAnimationFrame(rafId); rafId = null; }
        refreshTable();
    });
}

function bindKeyboard() {
    // Focus drawer when clicking inside it (but not the search input)
    drawerEl.addEventListener('pointerdown', e => {
        if (e.target !== searchEl) drawerEl.focus({ preventScroll: true });
    });
    // Focus map when clicking on the map canvas
    map.getCanvas().addEventListener('pointerdown', () => {
        if (document.activeElement === drawerEl) drawerEl.blur();
    });

    // Keyboard events only fire when drawer has focus
    drawerEl.addEventListener('keydown', e => {
        if (drawerWidth < MIN_WIDTH || !activeTab) return;
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

        const visible = getVisibleLayers();

        switch (e.key) {
            case 'j':
            case 'ArrowDown':
                e.preventDefault();
                selectRow(selectedIdx + 1);
                break;
            case 'k':
            case 'ArrowUp':
                e.preventDefault();
                selectRow(selectedIdx - 1);
                break;
            case 'h':
            case 'ArrowLeft': {
                e.preventDefault();
                const ci = visible.indexOf(activeTab);
                if (ci > 0) {
                    activeTab = visible[ci - 1];
                    selectedId = null; selectedIdx = -1;
                    updateTabs();
                    refreshTable();
                }
                break;
            }
            case 'l':
            case 'ArrowRight': {
                e.preventDefault();
                const ci = visible.indexOf(activeTab);
                if (ci < visible.length - 1) {
                    activeTab = visible[ci + 1];
                    selectedId = null; selectedIdx = -1;
                    updateTabs();
                    refreshTable();
                }
                break;
            }
            case 'Enter': {
                if (selectedIdx >= 0 && selectedIdx < currentRows.length) {
                    e.preventDefault();
                    const row = currentRows[selectedIdx];
                    const [lat, lon] = rowCoords(row, LAYERS[activeTab]);
                    if (onRowClick) onRowClick(activeTab, row);
                    map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
                }
                break;
            }
            case 'g':
                e.preventDefault();
                selectRow(0);
                break;
            case 'G':
                e.preventDefault();
                selectRow(currentRows.length - 1);
                break;
        }
    });
}

function selectRow(idx) {
    if (currentRows.length === 0) return;
    idx = Math.max(0, Math.min(currentRows.length - 1, idx));
    selectedIdx = idx;
    selectedId = getRowId(currentRows[idx], activeTab);
    renderTable(currentRows, currentTotalCount);
    const tr = tableEl.querySelector(`tr[data-idx="${idx}"]`);
    if (tr) tr.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
}

function getVisibleLayers() {
    const visible = [];
    for (const row of document.querySelectorAll('.toggle-row[data-layer]')) {
        const layer = row.dataset.layer;
        const cb = row.querySelector('input');
        if (cb.checked && LAYERS[layer]) visible.push(layer);
    }
    return visible;
}

function updateTabs() {
    const visible = getVisibleLayers();
    tabBarEl.innerHTML = '';

    if (visible.length === 0) {
        activeTab = null;
        tableEl.innerHTML = '';
        footerEl.textContent = '';
        return;
    }

    if (!activeTab || !visible.includes(activeTab)) {
        activeTab = visible[0];
        selectedId = null; selectedIdx = -1;
    }

    for (const layer of visible) {
        const tab = document.createElement('div');
        const info = LAYERS[layer];
        tab.className = 'drawer-tab' + (layer === activeTab ? ' active' : '');
        tab.textContent = info.label;
        if (layer === activeTab) {
            tab.style.color = info.color();
            tab.style.borderBottomColor = info.color();
        }
        tab.addEventListener('click', () => {
            if (activeTab === layer) return;
            activeTab = layer;
            selectedId = null; selectedIdx = -1;
            updateTabs();
            refreshTable();
            emitState();
        });
        tabBarEl.appendChild(tab);
    }
}

// Pick a tab if none is set yet — preserves existing selection
function ensureActiveTab() {
    if (activeTab) { updateTabs(); return; }
    const visible = getVisibleLayers();
    if (visible.length > 0) {
        activeTab = visible[0];
        updateTabs();
    }
}

// Filter + sort the already-loaded data client-side, or query DuckDB when searching
async function refreshTable() {
    if (!activeTab || drawerWidth < MIN_WIDTH) return;

    // When searching, push filtering to DuckDB
    if (searchTerm && _queryFn) {
        const gen = ++_queryGen;
        const b = map.getBounds();
        const bounds = { south: b.getSouth(), north: b.getNorth(), west: b.getWest(), east: b.getEast() };
        const { rows: data, total } = await _queryFn(activeTab, bounds, searchTerm, sortCol, sortDir);
        if (gen !== _queryGen) return; // stale

        if (selectedId != null) {
            const idx = data.findIndex(r => getRowId(r, activeTab) === selectedId);
            if (idx > 0) {
                const [row] = data.splice(idx, 1);
                data.unshift(row);
            }
            selectedIdx = idx >= 0 ? (idx > 0 ? 0 : idx) : -1;
        } else {
            selectedIdx = -1;
        }

        currentRows = data;
        currentTotalCount = total;
        renderTable(data, total);
        return;
    }

    const rows = allData[activeTab] || [];
    const coords = _coords[activeTab];
    if (!coords) return;
    const { lats, lons } = coords;
    const b = map.getBounds();
    const south = b.getSouth(), north = b.getNorth(), west = b.getWest(), east = b.getEast();

    // Viewport filter using pre-extracted typed arrays
    const n = rows.length;
    let filtered = [];
    for (let i = 0; i < n; i++) {
        const lat = lats[i], lon = lons[i];
        if (lat >= south && lat <= north && lon >= west && lon <= east) {
            filtered.push(rows[i]);
        }
    }

    const totalCount = filtered.length;

    // Sort
    if (sortCol) {
        const dir = sortDir === 'DESC' ? -1 : 1;
        filtered.sort((a, b) => {
            const va = a[sortCol], vb = b[sortCol];
            if (va == null && vb == null) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            return (va < vb ? -1 : va > vb ? 1 : 0) * dir;
        });
    }

    // Paginate
    if (filtered.length > MAX_ROWS) filtered = filtered.slice(0, MAX_ROWS);

    // Pin selected row at the top — even if it's scrolled out of the viewport
    if (selectedId != null) {
        const idx = filtered.findIndex(r => getRowId(r, activeTab) === selectedId);
        if (idx > 0) {
            const [row] = filtered.splice(idx, 1);
            filtered.unshift(row);
        } else if (idx === -1) {
            const row = rows.find(r => getRowId(r, activeTab) === selectedId);
            if (row) filtered.unshift(row);
        }
        selectedIdx = 0;
    } else {
        selectedIdx = -1;
    }

    currentRows = filtered;
    currentTotalCount = totalCount;

    renderTable(filtered, totalCount);
}

function renderTable(data, totalCount) {
    if (!data.length) {
        tableEl.innerHTML = '<tr><td class="drawer-empty">No data in view</td></tr>';
        footerEl.textContent = '0 in view';
        return;
    }

    const cols = Object.keys(data[0]);
    const nCols = cols.length;
    // Pre-size: header row + data rows, ~nCols tags each
    const parts = [];

    parts.push('<thead><tr>');
    for (let c = 0; c < nCols; c++) {
        const col = cols[c];
        const isSorted = sortCol === col;
        parts.push(isSorted
            ? `<th data-col="${col}" class="sorted">${col}${sortDir === 'ASC' ? ' \u2191' : ' \u2193'}</th>`
            : `<th data-col="${col}">${col}</th>`);
    }
    parts.push('</tr></thead><tbody>');

    function fmtCell(v) {
        if (typeof v === 'number' && !Number.isInteger(v)) return v.toFixed(3);
        return v;
    }

    for (let i = 0, n = data.length; i < n; i++) {
        const row = data[i];
        parts.push(i === selectedIdx ? `<tr data-idx="${i}" class="selected">` : `<tr data-idx="${i}">`);
        for (let c = 0; c < nCols; c++) {
            const v = row[cols[c]];
            if (v == null) { parts.push('<td></td>'); continue; }
            parts.push(`<td>${esc(fmtCell(v))}</td>`);
        }
        parts.push('</tr>');
    }
    parts.push('</tbody>');
    tableEl.innerHTML = parts.join('');

    // Snap to top so pinned selected row is visible
    if (selectedIdx === 0) {
        const wrap = tableEl.closest('.drawer-table-wrap');
        if (wrap) wrap.scrollTop = 0;
    }

    if (totalCount > data.length) {
        footerEl.textContent = `${data.length.toLocaleString()} of ${totalCount.toLocaleString()} in view`;
    } else {
        footerEl.textContent = `${totalCount.toLocaleString()} in view`;
    }

    // Header click for sorting
    tableEl.querySelectorAll('th[data-col]').forEach(th => {
        th.addEventListener('click', () => {
            const col = th.dataset.col;
            if (sortCol === col) {
                sortDir = sortDir === 'ASC' ? 'DESC' : 'ASC';
            } else {
                sortCol = col;
                sortDir = 'ASC';
            }
            refreshTable();
        });
    });

    // Row click for selection
    tableEl.querySelectorAll('tbody tr[data-idx]').forEach(tr => {
        tr.addEventListener('click', () => {
            const idx = Number(tr.dataset.idx);
            const row = currentRows[idx];
            if (!row) return;

            const [lat, lon] = rowCoords(row, LAYERS[activeTab]);

            selectedIdx = idx;
            selectedId = getRowId(row, activeTab);
            renderTable(currentRows, currentTotalCount);

            if (onRowClick) onRowClick(activeTab, row);
            map.flyTo({ center: [lon, lat], zoom: Math.max(map.getZoom(), 13) });
        });
    });
}

function getRowId(row, table) {
    const info = LAYERS[table];
    if (info.idCol) return String(row[info.idCol]);
    const lat = row.latitude ?? row.lat;
    const lon = row.longitude ?? row.lon;
    return `${lat}_${lon}_${row.name ?? row.operator ?? ''}`;
}

export function highlight(layerType, id) {
    if (layerType == null || id == null) {
        selectedId = null;
        selectedIdx = -1;
        if (tabBarEl && activeTab && drawerWidth >= MIN_WIDTH) renderTable(currentRows, currentTotalCount);
        return;
    }

    const tab = TAB_MAP[layerType] || layerType;
    selectedId = String(id);

    // Switch to the correct tab (works even before DOM init — activeTab is just a string)
    if (getVisibleLayers().includes(tab) && tab !== activeTab) {
        activeTab = tab;
        sortCol = null; sortDir = 'ASC';
    }

    if (!tabBarEl || drawerWidth < MIN_WIDTH) return;

    updateTabs();
    refreshTable();
}
