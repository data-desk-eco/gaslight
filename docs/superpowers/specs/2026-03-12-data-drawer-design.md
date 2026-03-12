# Data Drawer

Draggable left-side panel showing raw parquet table contents via DuckDB WASM, filtered to the current map viewport. Provides a spreadsheet-style data inspector alongside the map.

## Layout & Interaction

- **Data drawer**: full-height panel on the left edge, default closed (0 width). A vertical drag handle sits on its right edge, always visible at the left screen edge.
- **Dragging**: grabbing the handle slides the drawer open. The map container's `left` offset matches the drawer width, pushing the map right. MapLibre's `map.resize()` is called on drag end (or debounced during drag).
- **Max width**: `calc(100vw - 400px)` — leaves a usable mobile-width map.
- **Min width**: ~300px when open (enough for a readable table). Below that threshold, snaps closed to 0px.
- **Closed state**: drawer is 0px wide. Handle is a thin vertical square pill (6px wide, 48px tall, square corners) visible at the screen edge, centered vertically.
- **Existing left panel**: unchanged. It remains `position: fixed` over the map. When the map narrows, the left panel stays in place — no repositioning needed.
- **Mobile** (<768px): drawer is hidden entirely. No drag handle shown.

## Data & Tabs

- **Tabs**: one per currently visible layer (flares, permits, plumes, wells, detections, leases). Tabs appear/disappear as layers are toggled on/off in the existing left panel controls. If no layers are visible, drawer shows an empty state.
- **Active tab**: queries DuckDB with a bounding-box WHERE clause using `map.getBounds()`. Query fires on `moveend` (debounced) and on tab switch.
- **Table**: renders all columns from the parquet table for the active tab. Horizontally scrollable if columns overflow. Rows are the raw DuckDB query result — no GeoJSON transformation.
- **Row count**: shown in footer (e.g., "142 rows in view").

## Selection & Bidirectional Linking

- **Click row → map**: flies to the feature's lat/lon and opens the right-side detail card (reusing existing `showFlareDetail`, `showPermitDetail`, etc.). The clicked row highlights with the layer's color as a left border accent + subtle background tint.
- **Click map → table**: when a detail card opens from a map click, the corresponding row in the data table highlights and scrolls into view (if the drawer is open and the matching tab is active).
- **Highlight style**: left border in the layer's map color + subtle background tint on the row.

## Styling

- **Drawer background**: solid dark (`#16213e` palette), not frosted glass — it's a separate panel, not an overlay.
- **Border**: subtle right border (1px, muted) where drawer meets map.
- **Drag handle**: square shape (no border-radius), centered vertically on the drawer's right edge. Visible in both open and closed states. Cursor changes to `col-resize` on hover.
- **Table**: compact rows, monospace for numeric columns, standard font for text. Alternating row backgrounds for readability. Sticky header row.
- **Tabs**: horizontal tab bar at top of drawer. Active tab has bottom border accent in the layer's color. Inactive tabs are muted.
- **Layer colors**: each tab/table uses the same color already assigned to that layer on the map.
- **Transitions**: drawer width animates with CSS transition when snapping open/closed. During active drag, no transition (immediate response).

## Implementation

- **New file**: `web/drawer.js` — ES module owning drawer DOM, drag logic, tab management, DuckDB queries, and selection state. Exports an `init(map)` function called from `app.js` after map loads.
- **DOM**: drawer markup injected by `drawer.js`, not in `index.html`. A flex container wraps both drawer and `#map` div so drawer width pushes the map.
- **DuckDB queries**: new query functions in `db.js` for raw table scans with bbox filter (e.g., `queryTable(tableName, bounds)`) returning plain objects, not GeoJSON.
- **Drag logic**: pointer events on the handle (`pointerdown` → `pointermove` → `pointerup`). During drag, updates drawer width directly. On release, calls `map.resize()` and snaps closed if below 300px.
- **Map integration**: `moveend` listener fires the active tab's query. Layer toggle changes in `app.js` call a drawer method to update visible tabs.
- **Selection sync**: drawer exposes `highlight(layerType, id)` for map→table direction. Row clicks call existing detail functions from `app.js`.
- **No new dependencies**: vanilla JS, DOM APIs, existing DuckDB WASM instance.
