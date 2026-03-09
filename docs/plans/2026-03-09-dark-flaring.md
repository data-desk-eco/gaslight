# Dark Flaring Analysis — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a self-contained DuckDB database that combines SWR 32 flaring permits, RRC well/operator data, and VIIRS Nightfire satellite detections to identify unpermitted ("dark") flaring in the Permian Basin.

**Architecture:** Makefile-driven pipeline. Download public datasets (RRC EBCDIC files, EOG VNF profiles, SWR 32 via existing scraper), parse into CSVs/parquet, load into a single DuckDB database, run spatial+temporal analysis as SQL views. Everything under `data/` is gitignored and reproducible.

**Tech Stack:** bash, Python (EBCDIC parsing, VNF download), DuckDB (spatial extension), Make

---

### Task 1: Project setup

**Files:**
- Modify: `pyproject.toml` (create)
- Modify: `.gitignore`
- Modify: `CLAUDE.md`

**Step 1: Create pyproject.toml**

```toml
[project]
name = "tx-swr32"
version = "0.1.0"
description = "Dark flaring analysis: SWR 32 permits vs VIIRS Nightfire detections"
requires-python = ">=3.10"
dependencies = [
    "playwright",
    "requests",
    "beautifulsoup4",
    "duckdb",
    "python-dotenv",
]
```

**Step 2: Update .gitignore**

```
data/
.env
```

**Step 3: Update CLAUDE.md**

Update the project description to reflect the expanded scope. Mention the DuckDB database, the three data sources (SWR 32, RRC wellbore/P-5, VNF), and that the Permian Basin (districts 08, 7C, 8A) is the focus. Note that `uv` manages the Python project.

**Step 4: Create directory structure**

```bash
mkdir -p scripts queries
```

**Step 5: Install dependencies**

```bash
uv sync
uv run playwright install chromium
```

**Step 6: Commit**

```bash
git add pyproject.toml .gitignore CLAUDE.md
git commit -m "project setup for dark flaring analysis"
```

---

### Task 2: RRC data download script

**Files:**
- Create: `scripts/download_rrc.py`

Downloads wellbore (`dbf900.ebc.gz`) and P-5 org (`orf850.ebc.gz`) files from the RRC MFT server using Playwright. Adapted from nom-de-plume's `download_rrc.py`.

**Step 1: Write download script**

```python
#!/usr/bin/env python3
"""Download RRC EBCDIC files from MFT server."""
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

DATASETS = {
    "dbf900.ebc.gz": "b070ce28-5c58-4fe2-9eb7-8b70befb7af9",  # Wellbore
    "orf850.ebc.gz": "04652169-eed6-4396-9019-2e270e790f6c",  # P-5 Org
}


def download(out_dir: Path, filename: str):
    link_id = DATASETS[filename]
    out_path = out_dir / filename
    if out_path.exists():
        print(f"{filename} already exists, skipping")
        return

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(f"https://mft.rrc.texas.gov/link/{link_id}", wait_until="networkidle")
        page.get_by_text(filename).click()
        page.get_by_role("button", name="Download").click()
        with page.expect_download(timeout=300_000) as dl:
            pass
        dl.value.save_as(str(out_path))
        browser.close()
        print(f"Downloaded {filename} -> {out_path}")


if __name__ == "__main__":
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name in DATASETS:
        download(out_dir, name)
```

**Step 2: Test manually**

```bash
uv run scripts/download_rrc.py data
ls -lh data/dbf900.ebc.gz data/orf850.ebc.gz
```

Expected: two gzipped EBCDIC files (~490MB and ~21MB).

**Step 3: Commit**

```bash
git add scripts/download_rrc.py
git commit -m "add RRC data download script"
```

---

### Task 3: EBCDIC parsers

**Files:**
- Create: `scripts/parse_rrc.py`

Single script that parses both wellbore and P-5 EBCDIC files into CSVs. Extracts only the fields we need, filtered to Permian districts (08, 7C, 8A). Adapted from nom-de-plume's parsers.

**Step 1: Write the parser**

```python
#!/usr/bin/env python3
"""Parse RRC EBCDIC files into CSVs for DuckDB loading."""
import csv
import gzip
import sys
from pathlib import Path

PERMIAN_DISTRICTS = {"08", "7C", "8A"}


def ebcdic(data: bytes) -> str:
    """Decode EBCDIC bytes to string, stripped."""
    return data.decode("cp500").strip()


def signed_decimal(data: bytes, decimal_places: int) -> float | None:
    """Parse EBCDIC zoned decimal (sign in last byte upper nibble)."""
    if not data or all(b == 0 for b in data):
        return None
    digits = []
    for i, b in enumerate(data):
        digit = b & 0x0F
        digits.append(digit)
    zone = data[-1] & 0xF0
    negative = zone == 0xD0
    value = 0
    for d in digits:
        value = value * 10 + d
    result = value / (10**decimal_places)
    return -result if negative else result


def parse_wellbore(gz_path: Path, out_dir: Path):
    """Parse wellbore EBCDIC → wells.csv (root+location+wellid joined)."""
    RECLEN = 247
    roots = {}  # api -> {district, ...}
    locations = {}  # api -> {lat, lon}
    wellids = []  # [{api, oil_gas_code, district, lease_number, well_number}]

    with gzip.open(gz_path, "rb") as f:
        while True:
            rec = f.read(RECLEN)
            if len(rec) < RECLEN:
                break
            rtype = ebcdic(rec[0:2])

            if rtype == "01":
                api_county = ebcdic(rec[2:5])
                api_unique = ebcdic(rec[5:10])
                district = ebcdic(rec[14:16])
                api = f"{api_county}{api_unique}"
                roots[api] = {"district": district}

            elif rtype == "13":
                api_county = ebcdic(rec[2:5])
                api_unique = ebcdic(rec[5:10])
                api = f"{api_county}{api_unique}"
                lat = signed_decimal(rec[132:142], 7)
                lon = signed_decimal(rec[142:152], 7)
                if lat and lon and lat != 0 and lon != 0:
                    locations[api] = {"lat": lat, "lon": -abs(lon)}

            elif rtype == "21":
                api_county = ebcdic(rec[2:5])
                api_unique = ebcdic(rec[5:10])
                api = f"{api_county}{api_unique}"
                # Need district from root record
                root = roots.get(api, {})
                district = root.get("district", "")

                og_code = ebcdic(rec[10:11]) if len(rec) > 10 else ""
                # Record type 21 layout differs from what we saw
                # Oil: district(2) + lease(5) + well(6) starting at byte 10
                # Gas: district+gasid(6) starting at byte 10
                og_raw = ebcdic(rec[10:11])
                if og_raw == "O":
                    lease_district = ebcdic(rec[11:13])
                    lease_number = ebcdic(rec[13:18])
                    well_number = ebcdic(rec[18:24])
                elif og_raw == "G":
                    lease_district = ebcdic(rec[11:13])
                    lease_number = ebcdic(rec[13:19])
                    well_number = ""
                else:
                    continue

                wellids.append({
                    "api": api,
                    "oil_gas_code": og_raw,
                    "lease_district": lease_district,
                    "lease_number": lease_number,
                    "well_number": well_number,
                })

    # Join and filter to Permian
    out_path = out_dir / "wells.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["api", "oil_gas_code", "lease_district", "lease_number",
                     "well_number", "latitude", "longitude"])
        seen = set()
        for wid in wellids:
            if wid["lease_district"] not in PERMIAN_DISTRICTS:
                continue
            api = wid["api"]
            loc = locations.get(api, {})
            lat = loc.get("lat", "")
            lon = loc.get("lon", "")
            key = (api, wid["oil_gas_code"], wid["lease_number"])
            if key in seen:
                continue
            seen.add(key)
            w.writerow([api, wid["oil_gas_code"], wid["lease_district"],
                        wid["lease_number"], wid["well_number"], lat, lon])

    print(f"Wrote {len(seen)} Permian wells to {out_path}")


def parse_p5(gz_path: Path, out_dir: Path):
    """Parse P-5 org EBCDIC → operators.csv."""
    RECLEN = 350
    out_path = out_dir / "operators.csv"

    with open(out_path, "w", newline="") as fout:
        w = csv.writer(fout)
        w.writerow(["operator_number", "operator_name", "status"])
        count = 0

        with gzip.open(gz_path, "rb") as f:
            while True:
                rec = f.read(RECLEN)
                if len(rec) < RECLEN:
                    break
                rtype = rec[0:2].decode("cp500")
                if rtype != "A ":
                    continue
                operator_number = ebcdic(rec[2:8])
                operator_name = ebcdic(rec[8:40])
                status = ebcdic(rec[41:42])
                w.writerow([operator_number, operator_name, status])
                count += 1

    print(f"Wrote {count} operators to {out_path}")


if __name__ == "__main__":
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")

    wellbore = data_dir / "dbf900.ebc.gz"
    p5 = data_dir / "orf850.ebc.gz"

    if wellbore.exists():
        parse_wellbore(wellbore, data_dir)
    else:
        print(f"Missing {wellbore}, skipping wellbore parse")

    if p5.exists():
        parse_p5(p5, data_dir)
    else:
        print(f"Missing {p5}, skipping P-5 parse")
```

**Step 2: Test the parser**

```bash
uv run scripts/parse_rrc.py data
head -5 data/wells.csv
head -5 data/operators.csv
wc -l data/wells.csv data/operators.csv
```

Expected: wells.csv with ~100K+ Permian wells, operators.csv with ~200K+ orgs.

**Step 3: Verify field layout**

Cross-check a few well records against the RRC public lookup to confirm API numbers, lease numbers, and coordinates are parsed correctly. The EBCDIC byte offsets for record type 21 need careful validation — the layout in nom-de-plume's parser has:
- byte 2: oil_gas_code (1 byte) — but this is after the 2-byte record type
- For oil: district (bytes 3-5), lease (bytes 5-10), well (bytes 10-16)
- For gas: district+gas_rrc_id (bytes 3-9)

If the byte offsets are wrong (likely — EBCDIC field layouts are fiddly), adjust them by comparing parsed output against known records. Use the RRC online well lookup as ground truth.

**Step 4: Commit**

```bash
git add scripts/parse_rrc.py
git commit -m "add RRC EBCDIC parsers for wellbore and P-5"
```

---

### Task 4: VNF data fetcher

**Files:**
- Create: `scripts/fetch_vnf.py`
- Create: `.env.example`

Downloads VIIRS Nightfire profiles for Permian Basin flares from EOG. Requires EOG account credentials in `.env`. Adapted from burnoff's `fetch_vnf_profiles.py`.

**Step 1: Create .env.example**

```
EOG_EMAIL=your-email@example.com
EOG_PASSWORD=your-password
```

**Step 2: Write the VNF fetcher**

```python
#!/usr/bin/env python3
"""Fetch VIIRS Nightfire profiles for Permian Basin flares."""
import csv
import io
import os
import re
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

INDEX_URL = "https://eogdata.mines.edu/wwwdata/downloads/VNF_multiyear_2012-2021/multiyear_201204_202405_monthly.zip"
PROFILES_URL = "https://eogdata.mines.edu/wwwdata/downloads/vnf_profiles/profiles_multiyear"

# Permian Basin bounding box
PERMIAN = {"lat_min": 30.0, "lat_max": 33.5, "lon_min": -104.5, "lon_max": -100.0}
WORKERS = 8


def eog_session() -> requests.Session:
    """Authenticate with EOG via OIDC."""
    s = requests.Session()
    email = os.environ["EOG_EMAIL"]
    password = os.environ["EOG_PASSWORD"]

    r = s.get(PROFILES_URL, allow_redirects=True)
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    if not form:
        return s  # already authenticated or no login needed

    action = form.get("action", r.url)
    data = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
    data["username"] = email
    data["password"] = password
    r = s.post(action, data=data, allow_redirects=True)

    # Handle possible consent form
    if "consent" in r.text.lower():
        soup = BeautifulSoup(r.text, "html.parser")
        form = soup.find("form")
        if form:
            action = form.get("action", r.url)
            data = {i["name"]: i.get("value", "") for i in form.find_all("input") if i.get("name")}
            r = s.post(action, data=data, allow_redirects=True)

    return s


def find_permian_flares(index_path: Path) -> set[int]:
    """Read VNF index CSV, return flare IDs within Permian bounding box."""
    flares = set()
    with open(index_path) as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                lat = float(row["Lat_GMTCO"])
                lon = float(row["Lon_GMTCO"])
            except (ValueError, KeyError):
                continue
            if (PERMIAN["lat_min"] <= lat <= PERMIAN["lat_max"] and
                    PERMIAN["lon_min"] <= lon <= PERMIAN["lon_max"]):
                flares.add(int(float(row["Flare_ID"])))
    return flares


def download_index(session: requests.Session, out_dir: Path) -> Path:
    """Download and extract VNF monthly index."""
    import zipfile
    zip_path = out_dir / "vnf_index.zip"
    csv_path = out_dir / "vnf_index.csv"

    if csv_path.exists():
        return csv_path

    print("Downloading VNF index...")
    r = session.get(INDEX_URL, stream=True)
    r.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(8192):
            f.write(chunk)

    with zipfile.ZipFile(zip_path) as zf:
        # Extract the CSV (usually one file inside)
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        with zf.open(names[0]) as src, open(csv_path, "wb") as dst:
            dst.write(src.read())
    zip_path.unlink()
    return csv_path


def download_profile(session: requests.Session, flare_id: int, out_dir: Path) -> str:
    """Download a single flare profile CSV."""
    fname = f"site_{flare_id}_multiyear_vnf_series.csv"
    out_path = out_dir / fname
    if out_path.exists():
        return f"skip {flare_id}"

    url = f"{PROFILES_URL}/{fname}"
    r = session.get(url, timeout=60)
    if r.status_code == 200:
        out_path.write_bytes(r.content)
        return f"ok {flare_id}"
    return f"fail {flare_id} ({r.status_code})"


def main():
    data_dir = Path("data")
    profiles_dir = data_dir / "vnf_profiles"
    profiles_dir.mkdir(parents=True, exist_ok=True)

    session = eog_session()

    # Get index and find Permian flares
    index_path = download_index(session, data_dir)
    flare_ids = find_permian_flares(index_path)
    print(f"Found {len(flare_ids)} Permian flares in index")

    # Filter to not-yet-downloaded
    existing = {int(m.group(1)) for p in profiles_dir.glob("site_*.csv")
                if (m := re.match(r"site_(\d+)", p.stem))}
    todo = flare_ids - existing
    print(f"{len(existing)} already downloaded, {len(todo)} remaining")

    if not todo:
        return

    # Download profiles
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(download_profile, session, fid, profiles_dir): fid
                   for fid in todo}
        for f in as_completed(futures):
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(todo)}")

    print(f"Done. {len(list(profiles_dir.glob('site_*.csv')))} profiles total")


if __name__ == "__main__":
    main()
```

**Step 3: Test**

```bash
uv run scripts/fetch_vnf.py
ls data/vnf_profiles/ | head
ls data/vnf_profiles/ | wc -l
```

Expected: ~1000-3000 profile CSVs for Permian flares.

**Step 4: Commit**

```bash
git add scripts/fetch_vnf.py .env.example
git commit -m "add VNF profile fetcher for Permian flares"
```

---

### Task 5: DuckDB schema and loading

**Files:**
- Create: `queries/schema.sql`
- Create: `queries/load.sql`

**Step 1: Write schema**

```sql
-- schema.sql: Dark flaring analysis database
INSTALL spatial; LOAD spatial;

-- SWR 32 exception permits
CREATE TABLE IF NOT EXISTS permits (
    excep_seq       INTEGER,
    submittal_dt    VARCHAR,
    filing_no       INTEGER,
    status          VARCHAR,
    filing_type     VARCHAR,
    operator_no     INTEGER,
    operator_name   VARCHAR,
    property        VARCHAR,
    effective_dt    VARCHAR,
    expiration_dt   VARCHAR,
    fv_district     VARCHAR,
    -- parsed from property field
    property_type   VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR
);

-- RRC wells (Permian only, with locations)
CREATE TABLE IF NOT EXISTS wells (
    api             VARCHAR,
    oil_gas_code    VARCHAR,
    lease_district  VARCHAR,
    lease_number    VARCHAR,
    well_number     VARCHAR,
    latitude        DOUBLE,
    longitude       DOUBLE,
    geom            GEOMETRY
);

-- RRC operators
CREATE TABLE IF NOT EXISTS operators (
    operator_number VARCHAR,
    operator_name   VARCHAR,
    status          VARCHAR
);

-- VNF daily flare detections (Permian)
CREATE TABLE IF NOT EXISTS vnf (
    flare_id    INTEGER,
    lat         DOUBLE,
    lon         DOUBLE,
    date        DATE,
    clear       BOOLEAN,
    detected    BOOLEAN,
    rh_mw       DOUBLE,
    temp_k      DOUBLE,
    n_passes    INTEGER,
    geom        GEOMETRY
);
```

**Step 2: Write load script**

```sql
-- load.sql: Load all data into the database

-- 1. Load SWR 32 permits
INSERT INTO permits
SELECT
    *,
    -- Parse property field: "Oil Lease-08-43066" → type, district, number
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 1)
         ELSE NULL END AS property_type,
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 2)
         ELSE NULL END AS lease_district,
    CASE WHEN property LIKE '%-%-%'
         THEN split_part(property, '-', 3)
         ELSE NULL END AS lease_number
FROM read_csv('data/filings.csv', delim='\t', header=true, all_varchar=true,
              columns={
                  'excep_seq': 'INTEGER',
                  'submittal_dt': 'VARCHAR',
                  'filing_no': 'INTEGER',
                  'status': 'VARCHAR',
                  'filing_type': 'VARCHAR',
                  'operator_no': 'INTEGER',
                  'operator_name': 'VARCHAR',
                  'property': 'VARCHAR',
                  'effective_dt': 'VARCHAR',
                  'expiration_dt': 'VARCHAR',
                  'fv_district': 'VARCHAR'
              });

-- 2. Load wells
INSERT INTO wells
SELECT
    api, oil_gas_code, lease_district, lease_number, well_number,
    latitude, longitude,
    CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL
              AND latitude != 0 AND longitude != 0
         THEN ST_Point(longitude, latitude)
         ELSE NULL END AS geom
FROM read_csv('data/wells.csv', header=true, auto_detect=true);

-- 3. Load operators
INSERT INTO operators
SELECT * FROM read_csv('data/operators.csv', header=true, auto_detect=true);

-- 4. Load VNF from profiles → aggregate to daily
CREATE TEMP TABLE vnf_passes AS
SELECT
    CAST(regexp_extract(filename, 'site_(\d+)', 1) AS INTEGER) AS flare_id,
    CAST("Date_Mscan" AS DATE) AS date,
    CAST("Lat_GMTCO" AS DOUBLE) AS lat,
    CAST("Lon_GMTCO" AS DOUBLE) AS lon,
    CAST("Cloud_Mask" AS INTEGER) AS cloud_mask,
    CAST("Temp_BB" AS DOUBLE) AS temp_bb,
    CAST("RH" AS DOUBLE) AS rh
FROM read_csv('data/vnf_profiles/site_*.csv',
              filename=true, union_by_name=true,
              ignore_errors=true, auto_detect=true)
WHERE CAST("Sunlit" AS INTEGER) = 0;  -- nighttime only

INSERT INTO vnf
SELECT
    flare_id,
    AVG(lat) AS lat,
    AVG(lon) AS lon,
    date,
    BOOL_OR(cloud_mask = 0) AS clear,
    BOOL_OR(cloud_mask = 0 AND temp_bb != 999999) AS detected,
    AVG(CASE WHEN cloud_mask = 0 AND temp_bb != 999999 THEN rh END) AS rh_mw,
    AVG(CASE WHEN cloud_mask = 0 AND temp_bb != 999999 THEN temp_bb END) AS temp_k,
    COUNT(*) AS n_passes
FROM vnf_passes
GROUP BY flare_id, date;

DROP TABLE vnf_passes;

-- Update VNF geometry
UPDATE vnf SET geom = ST_Point(lon, lat) WHERE lat IS NOT NULL AND lon IS NOT NULL;

-- Spatial indexes
CREATE INDEX IF NOT EXISTS idx_wells_geom ON wells USING RTREE (geom);
CREATE INDEX IF NOT EXISTS idx_vnf_geom ON vnf USING RTREE (geom);
```

**Step 3: Test loading**

```bash
duckdb data/dark_flaring.duckdb < queries/schema.sql
duckdb data/dark_flaring.duckdb < queries/load.sql
duckdb data/dark_flaring.duckdb -c "
    SELECT 'permits' AS t, count(*) FROM permits
    UNION ALL SELECT 'wells', count(*) FROM wells
    UNION ALL SELECT 'operators', count(*) FROM operators
    UNION ALL SELECT 'vnf', count(*) FROM vnf;
"
```

**Step 4: Commit**

```bash
git add queries/schema.sql queries/load.sql
git commit -m "add DuckDB schema and data loading"
```

---

### Task 6: Dark flaring analysis

**Files:**
- Create: `queries/dark_flaring.sql`

The core analysis: spatial-join VNF detections to nearest wells, then check for valid SWR 32 permits.

**Step 1: Write analysis query**

```sql
-- dark_flaring.sql: Identify VNF detections without valid SWR 32 permits
LOAD spatial;

-- Step 1: Match VNF detections to nearest well (within 750m ≈ 0.0075°)
CREATE OR REPLACE TABLE vnf_matched AS
WITH nearest AS (
    SELECT
        v.flare_id,
        v.date,
        v.rh_mw,
        v.temp_k,
        v.lat AS vnf_lat,
        v.lon AS vnf_lon,
        w.api,
        w.oil_gas_code,
        w.lease_district,
        w.lease_number,
        w.well_number,
        ST_Distance_Sphere(v.geom, w.geom) / 1000.0 AS distance_km,
        ROW_NUMBER() OVER (
            PARTITION BY v.flare_id, v.date
            ORDER BY ST_Distance_Sphere(v.geom, w.geom)
        ) AS rn
    FROM vnf v
    JOIN wells w ON w.geom IS NOT NULL
        AND w.longitude BETWEEN v.lon - 0.015 AND v.lon + 0.015
        AND w.latitude  BETWEEN v.lat - 0.015 AND v.lat + 0.015
        AND ST_DWithin(v.geom, w.geom, 0.0075)
    WHERE v.detected = true
)
SELECT * EXCLUDE (rn) FROM nearest WHERE rn = 1;

-- Step 2: Join to permits — find valid SWR 32 coverage
CREATE OR REPLACE TABLE dark_flares AS
SELECT
    m.*,
    o.operator_name,
    p.filing_no AS permit_filing_no,
    p.status AS permit_status,
    p.effective_dt AS permit_effective,
    p.expiration_dt AS permit_expiration,
    p.property AS permit_property,
    CASE WHEN p.filing_no IS NOT NULL THEN false ELSE true END AS is_dark
FROM vnf_matched m
LEFT JOIN operators o
    ON LPAD(o.operator_number, 6, '0') = (
        -- Get operator from well via lease lookup
        -- For now, join via the permit's operator since we have operator_no there
        SELECT LPAD(CAST(pp.operator_no AS VARCHAR), 6, '0')
        FROM permits pp
        WHERE pp.lease_district = m.lease_district
          AND pp.lease_number = m.lease_number
        LIMIT 1
    )
LEFT JOIN permits p
    ON p.lease_district = m.lease_district
    AND p.lease_number = m.lease_number
    AND p.status = 'Approved'
    AND TRY_STRPTIME(p.effective_dt, '%m/%d/%Y') <= m.date
    AND (p.expiration_dt = ''
         OR TRY_STRPTIME(p.expiration_dt, '%m/%d/%Y') >= m.date);

-- Summary view
CREATE OR REPLACE VIEW dark_flaring_summary AS
SELECT
    is_dark,
    count(*) AS detection_days,
    count(DISTINCT flare_id) AS flare_sites,
    round(avg(rh_mw), 2) AS avg_rh_mw,
    round(sum(rh_mw), 0) AS total_rh_mw,
    min(date) AS earliest,
    max(date) AS latest
FROM dark_flares
GROUP BY is_dark;

-- Top dark flare sites by cumulative radiant heat
CREATE OR REPLACE VIEW top_dark_flares AS
SELECT
    flare_id,
    lease_district,
    lease_number,
    operator_name,
    vnf_lat,
    vnf_lon,
    count(*) AS detection_days,
    round(sum(rh_mw), 1) AS total_rh_mw,
    min(date) AS first_seen,
    max(date) AS last_seen
FROM dark_flares
WHERE is_dark = true
GROUP BY flare_id, lease_district, lease_number, operator_name, vnf_lat, vnf_lon
ORDER BY total_rh_mw DESC;
```

**Step 2: Test**

```bash
duckdb data/dark_flaring.duckdb < queries/dark_flaring.sql
duckdb data/dark_flaring.duckdb -c "SELECT * FROM dark_flaring_summary;"
duckdb data/dark_flaring.duckdb -c "SELECT * FROM top_dark_flares LIMIT 20;"
```

**Step 3: Commit**

```bash
git add queries/dark_flaring.sql
git commit -m "add dark flaring analysis"
```

---

### Task 7: Makefile

**Files:**
- Modify: `Makefile`

**Step 1: Rewrite Makefile**

```makefile
.PHONY: all permits rrc vnf db clean

all: db

# --- data downloads ---

permits: data/filings.csv
rrc: data/wells.csv data/operators.csv
vnf: data/vnf_profiles/.done

data/filings.csv:
	./scrape.sh metadata

data/dbf900.ebc.gz data/orf850.ebc.gz:
	uv run scripts/download_rrc.py data

data/wells.csv data/operators.csv: data/dbf900.ebc.gz data/orf850.ebc.gz
	uv run scripts/parse_rrc.py data

data/vnf_profiles/.done:
	uv run scripts/fetch_vnf.py
	@touch $@

# --- database ---

db: data/dark_flaring.duckdb

data/dark_flaring.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done
	@rm -f $@
	duckdb $@ < queries/schema.sql
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/dark_flaring.sql
	@echo "Database ready: $@"

clean:
	rm -f data/dark_flaring.duckdb data/wells.csv data/operators.csv
```

**Step 2: Test full build**

```bash
make clean
make db
```

**Step 3: Commit**

```bash
git add Makefile
git commit -m "makefile: full pipeline from download to dark flaring analysis"
```

---

### Task 8: End-to-end validation

**Step 1: Verify the database**

```bash
duckdb data/dark_flaring.duckdb <<'SQL'
-- Record counts
SELECT 'permits' AS table_name, count(*) AS rows FROM permits
UNION ALL SELECT 'wells', count(*) FROM wells
UNION ALL SELECT 'operators', count(*) FROM operators
UNION ALL SELECT 'vnf', count(*) FROM vnf
UNION ALL SELECT 'vnf_matched', count(*) FROM vnf_matched
UNION ALL SELECT 'dark_flares', count(*) FROM dark_flares;

-- Dark flaring summary
SELECT * FROM dark_flaring_summary;

-- Top 10 dark flare sites
SELECT * FROM top_dark_flares LIMIT 10;
SQL
```

**Step 2: Spot-check a dark flare**

Pick a result from top_dark_flares, look up the operator and lease on the RRC website, and verify there's genuinely no SWR 32 exception for that lease on those dates.

**Step 3: Iterate on the analysis query**

The dark_flaring.sql query will likely need tuning:
- The property field parsing may not handle all formats (e.g. multi-lease entries like `Oil Lease-08-43066; Oil Lease-8A-65656`)
- The operator join path (well→lease→permit→operator) may need refinement
- Distance threshold (750m) may need adjustment

Fix issues found during spot-checking and re-run `make db`.
