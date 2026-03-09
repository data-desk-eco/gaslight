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
