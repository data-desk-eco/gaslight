#!/usr/bin/env python3
"""Scrape flare/vent GPS coordinates from SWR 32 filing detail pages."""
import csv
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE = "https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
DETAIL = "https://webapps.rrc.state.tx.us/swr32/pbfiling.xhtml?action=open"
WORKERS = 4
PERMIAN_DISTRICTS = {"7B", "7C", "08", "8A", "8"}
TIMEOUT = 120

FIELDNAMES = [
    "filing_no", "name", "county", "district", "release_type",
    "release_height_ft", "gps_datum",
    "latitude", "longitude",
    "h2s_area", "h2s_concentration_ppm", "h2s_distance_ft",
    "h2s_public_area_type",
]

progress_lock = threading.Lock()
progress = {"done": 0, "locs": 0, "errors": 0, "total": 0, "start": 0.0}


def log(msg: str):
    print(msg, flush=True)


def get_viewstate(text: str) -> str:
    m = re.search(r'name="javax\.faces\.ViewState"[^/]*value="([^"]*)"', text)
    if m:
        return m.group(1)
    m = re.search(r'javax\.faces\.ViewState:0">(.*?)]]', text)
    return m.group(1).replace("<![CDATA[", "") if m else ""


def find_view_button(search_xml: str) -> str | None:
    from xml.etree import ElementTree as ET
    try:
        root = ET.fromstring(search_xml)
        updates = root.findall(".//update") or root.findall(".//{http://java.sun.com/jsf/ajax}update")
        for update in updates:
            fragment = update.text or ""
            if "View Application" not in fragment:
                continue
            soup = BeautifulSoup(fragment, "html.parser")
            for a in soup.find_all("a"):
                if "View Application" in a.get_text():
                    onclick = a.get("onclick", "")
                    m = re.search(r"'(pbqueryForm:pQueryTable:0:[^']+)'", onclick)
                    if m:
                        return m.group(1)
    except ET.ParseError:
        pass
    return None


def _label_after(soup: BeautifulSoup, header_text: str, prefix: str) -> str:
    header = soup.find("label", id=lambda x: x and prefix in x,
                        string=lambda s: s and header_text in s)
    if not header:
        return ""
    sibling = header.find_next("label", id=lambda x: x and prefix in x)
    return sibling.get_text(strip=True) if sibling else ""


def parse_flare_locations(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    locations = []
    idx = 0
    while True:
        prefix = f"pbactivefv:{idx}:"
        if not soup.find(id=lambda x: x and prefix in x):
            break
        lat_text = _label_after(soup, "Degrees (Latitude)", prefix)
        lon_text = _label_after(soup, "Degrees (Longitude)", prefix)
        row = {
            "name": _label_after(soup, "Flare or Vent Name", prefix),
            "county": _label_after(soup, "County", prefix),
            "district": _label_after(soup, "District", prefix),
            "release_type": _label_after(soup, "Release Type", prefix),
            "release_height_ft": _label_after(soup, "Release Height", prefix),
            "gps_datum": _label_after(soup, "GPS Datum", prefix),
            "latitude": "",
            "longitude": "",
            "h2s_area": _label_after(soup, "subject to SWR 36", prefix),
            "h2s_concentration_ppm": _label_after(soup, "H2S Concentration", prefix),
            "h2s_distance_ft": _label_after(soup, "distance to public area", prefix),
            "h2s_public_area_type": _label_after(soup, "Public Area Type", prefix),
        }
        if lat_text and lon_text:
            try:
                row["latitude"] = float(lat_text)
                row["longitude"] = float(lon_text)
                locations.append(row)
            except ValueError:
                pass
        idx += 1
    return locations


def scrape_one(filing_no: str) -> list[dict]:
    """Scrape a single filing. Fresh session each time."""
    s = requests.Session()
    r = s.get(BASE, timeout=TIMEOUT)
    vs = get_viewstate(r.text)

    r = s.post(BASE, data={
        "javax.faces.partial.ajax": "true",
        "javax.faces.source": "pbqueryForm:searchExceptions",
        "javax.faces.partial.execute": "@all",
        "javax.faces.partial.render": "pbqueryForm:pQueryTable",
        "pbqueryForm:searchExceptions": "pbqueryForm:searchExceptions",
        "pbqueryForm": "pbqueryForm",
        "javax.faces.ViewState": vs,
        "pbqueryForm:filingNumber_input": filing_no,
        "pbqueryForm:filingNumber_hinput": filing_no,
        "pbqueryForm:filingTypeList_focus": "",
        "pbqueryForm:filingTypeList_input": "",
        "pbqueryForm:permanentException_focus": "",
        "pbqueryForm:permanentException_input": "",
        "pbqueryForm:swr32h8_focus": "",
        "pbqueryForm:swr32h8_input": "",
        "pbqueryForm:propertyTypeList_focus": "",
        "pbqueryForm:propertyTypeList_input": "",
    }, headers={
        "Faces-Request": "partial/ajax",
        "X-Requested-With": "XMLHttpRequest",
    }, timeout=TIMEOUT)
    vs = get_viewstate(r.text) or vs

    btn = find_view_button(r.text)
    if not btn:
        return []

    r = s.post(BASE, data={
        "pbqueryForm": "pbqueryForm",
        "javax.faces.ViewState": vs,
        btn: btn,
    }, allow_redirects=True, timeout=TIMEOUT)

    html = r.text
    if "pbactivefv" not in html:
        html = s.get(DETAIL, timeout=TIMEOUT).text

    return parse_flare_locations(html)


def worker_fn(filing_no: str, writer: csv.DictWriter, fout, lock: threading.Lock):
    t0 = time.time()
    locs = []
    error = False
    for attempt in range(2):
        try:
            locs = scrape_one(filing_no)
            break
        except Exception as e:
            if attempt == 1:
                error = True
                log(f"  #{filing_no} FAILED: {e}")
            time.sleep(3)

    if locs:
        with lock:
            for loc in locs:
                writer.writerow({"filing_no": filing_no, **loc})
            fout.flush()

    elapsed = time.time() - t0
    with progress_lock:
        progress["done"] += 1
        progress["locs"] += len(locs)
        if error:
            progress["errors"] += 1
        d = progress["done"]
        total = progress["total"]
        total_locs = progress["locs"]
        errs = progress["errors"]
        rate = d / (time.time() - progress["start"])
        eta = (total - d) / rate if rate > 0 else 0

    if d % 10 == 0 or locs:
        log(f"  [{d}/{total}] #{filing_no} -> {len(locs)} locs ({elapsed:.0f}s) | {total_locs} total | {rate:.1f}/s | ETA {eta/60:.0f}m | {errs} err")


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    filings_csv = data_dir / "filings.csv"
    out_csv = data_dir / "flare_locations.csv"

    with open(filings_csv) as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    filings = [
        r["filing_no"] for r in rows
        if not r.get("fv_district", "").strip()
        or any(d.strip() in PERMIAN_DISTRICTS for d in r["fv_district"].split(","))
    ]
    log(f"{len(filings)} Permian filings (of {len(rows)} total)")

    done = set()
    if out_csv.exists():
        with open(out_csv) as f:
            done = {row["filing_no"] for row in csv.DictReader(f)}
    remaining = [fn for fn in filings if fn not in done]
    log(f"{len(remaining)} to scrape ({len(done)} already done)")

    if not remaining:
        return

    progress["total"] = len(remaining)
    progress["start"] = time.time()
    log(f"Launching {WORKERS} workers")

    write_header = not out_csv.exists() or out_csv.stat().st_size == 0
    lock = threading.Lock()

    with open(out_csv, "a", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=FIELDNAMES)
        if write_header:
            w.writeheader()
            fout.flush()

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(worker_fn, fn, w, fout, lock): fn for fn in remaining}
            for future in as_completed(futures):
                future.result()

    elapsed = time.time() - progress["start"]
    total = sum(1 for _ in open(out_csv)) - 1
    log(f"Done: {total} flare locations in {out_csv} ({elapsed/60:.0f}m)")


if __name__ == "__main__":
    main()
