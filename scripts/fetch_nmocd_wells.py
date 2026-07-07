#!/usr/bin/env python3
"""Fetch New Mexico OCD well headers (active, un-plugged, surface locations).

Mirror of `fetch_nmocd.py`: the OCD well search exports one giant HTML <table>
(served as .aspx/ms-excel). We stream it down, parse every row, and write a
compact, faithfully-typed parquet — one row per well — that the normal pipeline
(load → publish → export) ingests like any other fetched source. New Mexico is
the RRC's counterpart across the Delaware side of the Permian.
"""

import html
import re

import duckdb
import httpx
import pandas as pd

URL = ("https://wwwapps.emnrd.nm.gov/ocd/ocdpermitting/data/WellSearchExpandedResultsExcel.aspx"
       "?OperatorSearchClause=BeginsWith&WellSearchClause=BeginsWith&WellNumberSearchClause=BeginsWith"
       "&PoolSearchClause=BeginsWith&section=00&CancelledAPDs=Exclude&PluggedWells=Exclude"
       "&SearchLocation=Surface")
OUT = "data/wells_nm.parquet"

# curated subset of the 30 source columns → (source index, output name)
COLS = [
    (0, "api"), (1, "well_name"), (2, "well_number"), (3, "well_type"),
    (7, "status"), (8, "apd_date"), (10, "section"), (11, "township"),
    (12, "range"), (14, "footages"), (15, "latitude"), (16, "longitude"),
    (18, "last_production"), (19, "spud_date"), (20, "measured_depth"),
    (21, "true_vertical_depth"), (28, "operator"), (29, "district"),
]


def isodate(s):
    """MM/DD/YYYY → YYYY-MM-DD, dropping garbage years."""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})$", s)
    if not m or not 1900 <= int(m[3]) <= 2026:
        return None
    return f"{m[3]}-{m[1]}-{m[2]}"


def isomonth(s):
    """M/YYYY (last-production stamp) → YYYY-MM-01."""
    m = re.match(r"(\d{1,2})/(\d{4})$", s)
    if not m or not 1900 <= int(m[2]) <= 2026:
        return None
    return f"{m[2]}-{int(m[1]):02d}-01"


def num(s):
    try:
        return float(s)
    except ValueError:
        return None


def main():
    with httpx.Client(timeout=600, follow_redirects=True) as c:
        text = c.get(URL).text
    rows = []
    for tr in re.split(r"<tr>", text):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(tds) < 30:
            continue
        tds = [html.unescape(re.sub(r"<[^>]+>", "", x)).replace("\xa0", "").strip() for x in tds]
        r = {name: tds[i] for i, name in COLS}
        # operator arrives as "[ogrid] Name" — split the org number out
        m = re.match(r"\[(\d+)\]\s*(.*)", r["operator"])
        r["ogrid"], r["operator"] = (m[1], m[2]) if m else (None, r["operator"] or None)
        for k in ("apd_date", "spud_date"):
            r[k] = isodate(r[k])
        r["last_production"] = isomonth(r["last_production"])
        for k in ("latitude", "longitude", "measured_depth", "true_vertical_depth"):
            r[k] = num(r[k])
        rows.append(r)
    cols = [n for _, n in COLS[:-2]] + ["operator", "ogrid", "district"]
    df = pd.DataFrame(rows, columns=cols)
    duckdb.sql("COPY df TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)" % OUT)
    print(f"{OUT}: {len(df)} wells")


if __name__ == "__main__":
    main()
