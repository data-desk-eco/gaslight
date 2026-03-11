# s2-flares: Sentinel-2 flare detection as a shared module

## Problem

VIIRS Nightfire detections have 750m spatial uncertainty (M-band pixel size). When investigating a specific VNF flare site in gaslight, there's no way to determine the precise location of the flare source(s) within that pixel. Burnoff already solves this — it detects flares at 20m resolution using Sentinel-2 SWIR imagery — but its detection logic is coupled to its P2P distributed processing architecture.

## Solution

Extract burnoff's Sentinel-2 detection algorithm into a standalone module (`s2-flares`) that both projects consume as a git submodule. Gaslight adds an "Enhance" button to the flare detail panel that runs S2 detection over the 750m pixel area and renders precise flare locations live as results stream in.

## Module design: s2-flares

### Repository structure

```
s2-flares/
├── detect.js      # core algorithm: band math, connected components, cluster filtering
├── stac.js        # STAC search (Element84) + COG opening via geotiff.js
├── cluster.js     # cross-date spatial merge (grid-indexed anchor-based)
├── utm.js         # WGS84 ↔ UTM coordinate transforms
├── worker.js      # high-level worker harness (simple bbox+dates → detections)
└── vendor/
    └── geotiff.js # vendored geotiff.js library
```

### Two API levels

#### High-level: Worker message contract

For consumers that want a simple "detect flares in this area" interface (gaslight).

```
// Consumer spawns worker
const w = new Worker('vendor/s2-flares/worker.js');

// Request
→ { type: 'detect', bbox: [west, south, east, north], start: 'YYYY-MM-DD', end: 'YYYY-MM-DD' }

// Responses (streamed)
← { type: 'detection', features: [...] }      // deduped detections per S2 image, as found
← { type: 'progress', done: N, total: M }     // images processed
← { type: 'clusters', features: [...] }       // final cross-date merged clusters
← { type: 'error', message: string }          // STAC/network/processing failures
← { type: 'done' }

// Cancel
→ { type: 'cancel' }
```

The worker handles block-boundary overlap dedup internally — consumers receive clean, deduplicated detections per image.

Each detection feature includes: `{ lon, lat, max_b12, avg_b12, pixels, date, mgrs_tile }`.

Each cluster feature includes: `{ lon, lat, max_b12, avg_b12, detection_count, date_count, first_date, last_date, persistence }`.

#### Low-level: Exported functions

For consumers that manage their own concurrency/distribution (burnoff).

- `searchSTAC(bbox, start, end)` → async generator of STAC items (paginated)
- `openImage(item)` → `{ b12Image, metadata }` — opens B12 COG, returns image handle + geometry (bbox, dimensions, resolution, UTM zone)
- `openRemainingBands(item)` → `{ b11, b8a, scl }` — lazy open of other bands (only call when blocks need processing)
- `enumerateBlocks(metadata, bbox)` → array of `{ blockRow, blockCol, pixelWindow, cacheKey }` — requires metadata from `openImage`
- `detectBlock(rasters, meta)` → array of raw detections for one 256×256 block
- `clusterDetections(detections, observations, { mergeDistance, minDates, minAvgB12 })` → merged clusters. `observations` is a `Map<date, { cloudFree: boolean }>` for computing persistence; consumers build this from their own processing state. Terminal naming is consumer-specific (burnoff only) and not included.

### Detection algorithm (unchanged from burnoff)

Preserved exactly as-is — no threshold changes, no logic changes:

1. **Cloud screening**: SCL band, skip blocks >75% cloud
2. **DN → reflectance**: `(DN - 1000) / 10000` (L2A offset)
3. **Fused filter**: brightness (B12 > 0.3, B11 > 0.2), contrast (3× local median), thermal (NHISWNIR index)
4. **Connected components**: BFS 4-connectivity on passing pixels
5. **Cluster quality**: peak B12, max size, peakedness, halo rejection
6. **Overlap dedup**: canonical block ownership of peak pixels

### Cross-date clustering (from burnoff app.js)

Moved into `cluster.js`:

- Grid-indexed anchor-based merge (default 135m radius)
- Sort detections by max_b12 descending
- Merge within grid cells; create new cluster if no nearby anchor
- Filter: ≥4 distinct dates, avg B12 ≥ 0.85
- Seasonal false-positive flag: all detections fall within April–August (solar glint window)
- Persistence metric: detection dates / cloud-free observation dates

### Constants

All detection constants preserved from burnoff (B12_MIN=0.30, CONTRAST_RATIO=3.0, MERGE_DISTANCE_M=135, CLUSTER_MIN_DATES=4, etc.). Exported as named constants so consumers can inspect but should not override.

### Module format

All s2-flares files use `self.*` exports (worker-compatible). `utm.js` currently defines globals on `self` — this pattern is preserved for worker compatibility, but each file also attaches its exports to a namespaced object (e.g. `self.s2flares = { ... }`) so consumers can `importScripts` without polluting the global scope. The high-level `worker.js` uses `importScripts` internally to load the other modules.

## Changes to burnoff

`worker.js` in s2-flares is **net-new code** — a simple harness that calls the low-level functions for the high-level bbox-in/detections-out contract. Burnoff's existing worker becomes `detect-worker.js` with its own message loop for P2P block assignments. These are two distinct worker implementations.

Burnoff's current `detect.js` (Web Worker) gets refactored:

**Before:**
```
detect.js  — monolithic worker: STAC search + COG reading + detection + block partitioning + P2P awareness
utm.js     — coordinate transforms
```

**After:**
```
vendor/s2-flares/  — git submodule (detect.js, stac.js, cluster.js, utm.js, vendor/geotiff.js)
detect-worker.js   — thin harness: receives block assignments, calls s2-flares low-level API, posts results back
app.js             — clustering logic replaced with import from s2-flares/cluster.js
```

Burnoff's P2P block partitioning, CRDT sync, IndexedDB persistence, and awareness protocol remain untouched. The worker harness continues to receive block assignments from the main thread and dispatch to peers — it just calls `s2-flares/detectBlock()` instead of inline code.

The STAC search and block enumeration in burnoff's main thread can optionally migrate to use `s2-flares/stac.js`, but this is a low-priority cleanup since the existing code works.

## Changes to gaslight

### New files

**`web/enhance.js`** (~100–150 lines): orchestrator module.

- `enhance(flareId, lat, lon)` — main entry point
  - Computes 750m bbox from flare coordinates (same math as `flarePixelData`)
  - Determines date range: flare's `first_detected` to `last_detected` (from existing flare properties)
  - Spawns `s2-flares/worker.js`
  - On `detection` messages: converts features to GeoJSON, updates `s2-detections` map source
  - On `progress` messages: updates progress indicator in detail panel
  - On `clusters` messages: replaces raw detections with clustered points on map
  - On `done`: finalizes UI state
- `cancelEnhance()` — sends cancel message to worker, terminates it, clears S2 map sources. Called automatically by `closeDetail()` and when `showFeatureDetail()` switches to a different flare.
- Exports state for UI binding: `{ enhancing, progress, clusters }`

### New map layers

Added to `app.js`:

- **Source `s2-detections`**: GeoJSON, updated live during detection
- **Layer `s2-detection-points`** (circle): S2 flare detections within the pixel square
  - Radius: 4–8px scaled by max_b12
  - Color: warm ramp (similar to flare layer but distinct — e.g. white-hot palette)
  - Visible only during/after enhance
- **Layer `s2-cluster-points`** (circle): final merged clusters (replaces raw detections)
  - Radius: scaled by detection_count
  - Color: by persistence (how often detected vs how often observable)

### Detail panel additions

When viewing a flare:

- **Enhance button**: appears below the existing sparkline chart. Label: "Enhance with Sentinel-2". Disabled during detection (shows progress).
- **Progress indicator**: "Processing image 12 of 87..." — updates live via `progress` messages.
- **Results summary** (after completion): "N sources found, M observations across DATE–DATE". Listed below existing flare stats.
- **Per-cluster mini-list**: each S2 cluster shown as a clickable row with max B12, detection count, persistence. Clicking zooms to that cluster on the map.

### Vendor addition

```
web/vendor/s2-flares/  ← git submodule pointing to s2-flares repo
```

geotiff.js is already vendored in burnoff's copy; gaslight gets it via the submodule.

## UX flow

1. User clicks a VNF flare on the map → detail panel opens (existing behavior)
2. User sees "Enhance with Sentinel-2" button below the sparkline
3. User clicks Enhance:
   - Button changes to progress state ("Searching Sentinel-2 archive...")
   - Map zooms to fit the 750m pixel square if not already visible
   - As S2 images are processed, detection points appear live inside the pixel square
   - Progress updates: "Processing image 12 of 87..."
4. Detection completes:
   - Raw points replaced by merged clusters
   - Button changes to "Enhanced" (completed state)
   - Summary appears: "3 sources found, 142 observations, 2020–2025"
   - Each cluster listed as clickable row in detail panel
5. Clicking a cluster row highlights it on the map and shows its stats
6. Clicking a different flare or closing the panel clears S2 results
7. Re-clicking Enhance on same flare re-runs detection (no cache in gaslight; burnoff handles its own caching)

## Sequencing

1. **Create s2-flares repo** — empty, with README
2. **Refactor burnoff** — extract detect.js internals into s2-flares module files, refactor burnoff's worker to import from submodule, validate burnoff still works
3. **Integrate into gaslight** — add submodule, build enhance.js, add UI to detail panel
4. **Test end-to-end** — pick a known VNF flare, run enhance, verify S2 detections appear at expected locations

## Open questions

- **Date range strategy**: use the flare's `first_detected`–`last_detected` from VNF data, or always go back to 2020 (S2 L2A availability)? Former is faster; latter might catch flares that predate VNF detection.
- **Rate limiting**: Element84 STAC API has no published rate limits but we should add backoff. Going back years for a single pixel could mean 200+ images.
- **Result persistence**: should gaslight cache S2 results in IndexedDB (like burnoff does), or treat each enhance as ephemeral? Ephemeral is simpler; caching avoids re-processing.
- **geotiff.js version**: burnoff vendors a specific version. If gaslight already has geotiff.js for another purpose, we need to ensure version compatibility. (Currently gaslight does not use geotiff.js.)
