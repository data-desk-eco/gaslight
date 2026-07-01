#!/usr/bin/env python3
"""Fetch New Mexico OCD spill/release incidents (flaring, venting, spills).

The OCD spill search exports one giant HTML <table> (served as .aspx/ms-excel).
We stream it down, parse every row, and write a compact, faithfully-typed
parquet source file — one row per reported incident-material — that the normal
pipeline (load → publish → export) then ingests like any other fetched source.
"""

import html
import re

import duckdb
import httpx
import pandas as pd

URL = ("https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/Spills/"
       "SpillSearchResultsExcel.aspx?IncidentIdSearchClause=BeginsWith&Severity=All"
       "&OperatorSearchClause=BeginsWith&FacilityIdSearchClause=BeginsWith"
       "&FacilityNameSearchClause=BeginsWith&WellNameSearchClause=BeginsWith&Section=00")
OUT = "data/nm_incidents.parquet"

# curated subset of the 33 source columns → (source index, output name)
COLS = [
    (0, "incident_number"), (1, "facility_id"), (2, "facility_name"), (3, "api"),
    (4, "well_name"), (5, "ogrid"), (6, "operator"), (7, "severity"),
    (8, "incident_type"), (9, "lease_type"), (11, "incident_date"),
    (12, "notification_date"), (14, "material"), (15, "volume_released"),
    (18, "volume_unit"), (19, "cause"), (20, "spill_source"), (21, "district"),
    (22, "county"), (23, "ulstr"), (28, "latitude"), (29, "longitude"),
]


def isodate(s):
    """MM/DD/YYYY → YYYY-MM-DD, dropping the many garbage years (1820, 9201, …)."""
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})$", s)
    if not m or not 1990 <= int(m[3]) <= 2026:
        return None
    return f"{m[3]}-{m[1]}-{m[2]}"


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
        if len(tds) < 33:
            continue
        tds = [html.unescape(x).replace("\xa0", "").strip() for x in tds]
        r = {name: tds[i] for i, name in COLS}
        for k in ("incident_date", "notification_date"):
            r[k] = isodate(r[k])
        for k in ("latitude", "longitude", "volume_released"):
            r[k] = num(r[k])
        rows.append(r)
    df = pd.DataFrame(rows, columns=[n for _, n in COLS])
    duckdb.sql("COPY df TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)" % OUT)
    print(f"{OUT}: {len(df)} incidents")


if __name__ == "__main__":
    main()
