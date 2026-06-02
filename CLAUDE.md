# gaslight

Flaring analysis for the Permian Basin. Matches VIIRS Nightfire satellite flare detections and Sentinel-2 imagery to SWR 32 permitted flare locations, RRC wells, and methane plume observations.

## Layout

- `scripts/scrape_permits.py` — SWR 32 permit metadata scraper
- `scripts/scrape_permit_details.py` — downloads permit detail HTML pages
- `scripts/parse_permit_details.py` — parses HTML to CSVs (permit_details, permit_properties, permit_locations)
- `scripts/download_rrc.py` — downloads EBCDIC files from RRC MFT (Playwright)
- `scripts/parse_rrc.py` — parses EBCDIC to `wells.csv` + `operators.csv` + `gatherers.csv` (Permian districts 6E/7B/7C/08/8A)
- `scripts/fetch_vnf.py` — fetches VNF profiles from EOG
- `scripts/fetch_plumes.py` — fetches Carbon Mapper + IMEO methane plume data
- `scripts/fetch_r3.py` — fetches RRC R-3 gas processing facility locations
- `queries/load.sql` → `rrc.sql` → `publish.sql` → `export.sql` — SQL pipeline (load → normalise → build shareable DB → export parquets)
- `queries/s2.sql` — fetch+shape permian-flaring's S2 catalogue into the committed `data/s2_catalogue.parquet` (run by `make s2`)
- `scripts/build_dictionary.py` — generates the data dictionary (nested markdown under `docs/data-dictionary/` + in-DB `_dictionary`/`_sources` tables) from `docs/data-dictionary/_meta.yaml`
- `web/` — interactive map (MapLibre GL + DuckDB WASM, zero npm deps)
- `web/app.js` — main app: map setup, feature detail panels, S2 site detail card, shared helpers (`$`, `openDetail`, `fmtCoords`, color ramps, `renderTimeline`)
- `web/db.js` — DuckDB WASM wrapper: all data queries, operator attribution index, bbox helpers
- `web/drawer.js` — sliding data drawer (tabbed table view of flares/permits/plumes/wells with sort, viewport-sync, keyboard nav)
- `web/s2.js` — loads the S2 catalogue (`web/data/s2.parquet`) for display; one row per H3 site with an embedded per-date `detections` array

## Architecture

Three-schema database design (in `data/data.duckdb`), with analytical work done client-side in DuckDB WASM:

- **`raw`** — staging area, faithful load of source files (CSVs, DSVs). Mixed scope (some statewide, some Permian) — internal only.
- **`rrc`** — Texas oil & gas foundation tables derived from RRC data (permits, production)
- **`permian`** — clean, whole-Permian, geom-free, consistently-named tables built by `publish.sql`. The single source of truth.

Pipeline: `load → rrc → publish → export`

- **`publish.sql`** builds `permian.*` and copies it into the standalone, shareable **`dist/gaslight.duckdb`** (a `main` schema with no `raw`/`rrc` internals) — the one product shared with newsrooms *and* projected to the web map. Time-series tables are 2021+ (single `start_date` var); whole-Permian wells/gatherers live here in full.
- **`export.sql`** projects `permian.*` into the web app's `web/data/*.parquet` (flare-centric subsets sized for in-browser loading).

The shareable DB is documented by `docs/data-dictionary/` (see `scripts/build_dictionary.py`).

**S2 layer (consumed, not produced):** gaslight runs no S2 detection of its own. The sibling **permian-flaring** repo (the S2 detection engine + paper) publishes a per-date-enriched, site-level catalogue. `make s2` is a **fetch step** (symmetric to `make plumes`/`make r3`): `queries/s2.sql` reads p-f's export, applies the Permian/Texas clip + a top-`S2_LIMIT`-by-score cap, and writes the committed source file `data/s2_catalogue.parquet`. From there S2 flows through the *normal* pipeline like every other table — `load.sql` → `raw.s2_catalogue`, `publish.sql` → `permian.s2_detections` (→ `dist.s2_detections` in the shareable DB), `export.sql` → `web/data/s2.parquet`. One row per H3 site, with the per-date observations embedded as a `detections` JSON array. So gaslight rebuilds without p-f present (the catalogue is committed); only `make s2` needs p-f. Data flow is bidirectional but one-way each: gaslight → p-f (ground-truth CSVs, via p-f's `sql/40_truth.sql`); p-f → gaslight (the ranked S2 catalogue). Caveat: p-f's S2 window is 2025-01-01..2026-05-31, so the S2 layer covers only 2025–2026 (other layers are 2021+).

Client-side (DuckDB WASM): operator attribution, plume display, operator search, drawer text search — all computed live from the exported parquets.

### Web app structure

Single-page app with no build step and zero npm dependencies. MapLibre GL and DuckDB WASM are vendored.

- **app.js** — entry point. Initialises map, loads data, binds UI. Contains shared utilities: `$` (DOM lookup), `openDetail` (detail panel lifecycle), `fmtCoords`, color ramp functions (`b12Color`, `mwColor`), `renderTimeline` (shared SVG chart builder for both VNF sparklines and S2 timelines), and geo constants (`LAT_PER_M`, `lonPerM`).
- **db.js** — DuckDB WASM interface. Loads parquets, exposes typed query functions. Shared helpers: `bboxDeltas` (lat/lon deltas from radius). `queryDrawerRows`/`queryMapSearch` power the drawer's text search with DuckDB ILIKE on text columns.
- **drawer.js** — data drawer with tabbed tables (flares/permits/plumes/wells/infra), column sorting, keyboard navigation (j/k/h/l/g/G), viewport-synced queries, and text search. Search box filters both the table and the map layer via DuckDB ILIKE queries on text columns. Clicking a map feature switches to the relevant tab and pins the selected row at the top. Selection persists across pan/zoom and deep links.
- **s2.js** — loads permian-flaring's score-capped S2 catalogue (`s2.parquet`) into a `Map<h3, site>`. Each site carries its per-date `detections` array (parsed from the embedded JSON column) for the timeline chart. Display-only; no detection runs in-browser.
- **style.css** — all styling via CSS custom properties. `.btn-action` base class for action buttons. `.glass` / `.panel` for frosted-glass panels.

## Methodology

1. **Flare detection**: VNF flare sites matched to SWR 32 permit locations and RRC wells within 375m (VIIRS M-band pixel radius).
2. **Lease matching**: flares matched to leases via nearby wells within 375m. Wells carry `lease_district` and `lease_number` from RRC records; grouping by these fields links flares to their underlying leases.
3. **Nearby infrastructure (not attribution)**: flare and S2 cards deliberately do *not* name a single operator/facility with a confidence verdict — proximity within a satellite pixel is not ownership, and definitive attribution produced too many wrong calls. Instead the card (`nearbyInfraHtml` in app.js) lists nearby permitted flares (within 375m, operator + site + distance) and nearby R-3 gas plants (within 5km, name + distance); the VNF card additionally lists nearby leases grouped by operator (`vnf-lease-section`). Reader judges.
4. **Facility matching**: RRC R-3 gas processing facilities matched to flares within 5km, listed as "Nearby gas plants" in the detail card. Gas Plant permits also filtered from the permits layer.
5. **S2 flare layer (display-only)**: the top `S2_LIMIT` sites by score from permian-flaring's published S2 catalogue, ingested into the gaslight DB (`s2_detections`) and projected to the web map. Each site is a first-class map feature with its own static detail card (B12 stats, per-date timeline chart, glint/score/corroboration, nearby infrastructure) and deep link (`#s2=<h3>`, keyed by the stable H3 site id). The per-date timeline is rendered from the site's embedded `detections` JSON array via the shared `renderTimeline` helper. gaslight does no detection itself — see Architecture (S2 layer). Refresh from p-f with `make s2`, then `make db`.
6. **Reported flaring volumes**: Monthly lease-level gas disposition data from RRC PDQ (Production Data Query). Disposition code 04 = gas vented/flared. `rrc.production` stores monthly totals per lease (gas flared MCF + casinghead gas flared MCF). Flaring intensity = flared gas / total gas produced (%). Shown per-well in detail cards with monthly production charts.

## Key details

- **EBCDIC districts**: numeric codes mapped to alphanumeric via `rrc.district_map` (08→7B, 09→7C, 10→08, 11→8A)
- **Permits**: `rrc.permits` merges raw filings + detail pages with parsed dates, eliminating repeated COALESCE patterns downstream.
- **Well flaring**: `wells.parquet` includes per-lease flaring metrics (`flared_mcf`, `produced_mcf`, `flaring_intensity_pct`) joined from PDQ production data. Wells rendered as X markers (SDF symbol layer, visible at all zooms) colored by a combined score `sqrt(intensity% × ln(1 + flared_mcf))` on the same dark-red→white-hot ramp as flare sites. Well detail cards show a lease section with flaring stats and monthly production charts.
- **Gatherers/Purchasers**: `gatherers.parquet` from P-4 EBCDIC type 03 records (P4GPN segment). Links each lease to its gatherers, purchasers, and nominators via P-5 org numbers. Shown in well detail cards under the lease section, with current entities displayed prominently and historical ones collapsed.
- **IMEO source**: `data/imeo_plumes.geojson` — manual download from methanedata.unep.org (no API).
- **Permit coverage**: `rrc.permit_leases` maps each SWR 32 filing to its underlying leases.
- **Permian bbox**: 30–33.5°N, 100–104.5°W (applied at export time via `in_permian()` macro). Texas-only: sites above 32°N must be east of -103.064° (TX-NM border) to exclude New Mexico.
- **Match radius**: 375m (VIIRS M-band pixel radius = 750m / 2). Bounding box pre-filter ±0.0034° (~375m).
- **VIIRS pixel squares**: 750m squares generated client-side in the web app for visual review of spatial matching.
- **Selection behaviour**: clicking a feature selects it (dims map, highlights selected + associated features). Clicking anywhere while a feature is selected always deselects first — you can't jump directly from one selection to another.
- **Deep linking**: all params in the hash alongside MapLibre's map position. `#map=zoom/lat/lon&vnf=ID` opens VNF detail, `#map=…&s2=<h3>` opens an S2 site detail card (keyed by the stable H3 id).
- **Colors**: defined centrally as CSS custom properties (`--color-flare`, `--color-permit`, `--color-plume`, `--color-well`) in `:root`; JS reads them via `getComputedStyle`. Color ramps for intensity (`b12Color`, `mwColor`) are shared functions in app.js. Wells use the same dark-red→white-hot ramp as flares, driven by a combined intensity×volume score.
- **Legend order**: Flare sites → Permit locations → Methane plumes → Infrastructure → Oil/gas wells.

## Commands

- `make db` — full pipeline (load → rrc → publish → export)
- `make refresh` — rebuild DB from scratch
- `make publish` — build shareable `dist/gaslight.duckdb` + data dictionary
- `make export` — re-export parquets for web app
- `make vendor` — download vendored JS deps
- `make serve` — dev server on :8080
- `make plumes` — fetch latest plume data
- `make r3` — fetch RRC R-3 gas processing facilities
- `make s2` — fetch permian-flaring's S2 catalogue → `data/s2_catalogue.parquet` (run `make s2-export` in permian-flaring first, then `make db` to ingest)
- `make clean` — removes derived data
- `make help` — list all targets
- `duckdb data/data.duckdb` — query interactively
