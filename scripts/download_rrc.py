#!/usr/bin/env python3
"""Download RRC data files from MFT server and OTLS survey polygons."""
import sys
import urllib.request
import zipfile
from pathlib import Path
from playwright.sync_api import sync_playwright

# RRC MFT datasets (require Playwright for JSF download portal)
MFT_DATASETS = {
    "dbf900.ebc.gz": "b070ce28-5c58-4fe2-9eb7-8b70befb7af9",  # Wellbore
    "p4f606.ebc.gz": "19f9b9c7-2b82-4d7c-8dbd-77145a86d3de",  # P-4 Schedule
    "orf850.ebc.gz": "04652169-eed6-4396-9019-2e270e790f6c",  # P-5 Org
    "PDQ_DSV.zip": "1f5ddb8d-329a-4459-b7f8-177b4f5ee60d",    # Production Data Query
}

# OTLS survey polygons (direct download from ArcGIS Online)
OTLS_URL = "https://www.arcgis.com/sharing/rest/content/items/9812bbcbdae64d51be9ffef36a966101/data"


def download_otls(out_dir: Path):
    """Download statewide OTLS survey polygons from ArcGIS Online."""
    shp_path = out_dir / "survALLp.shp"
    if shp_path.exists():
        print("OTLS shapefile already exists, skipping")
        return
    zip_path = out_dir / "survALLp.zip"
    print("Downloading OTLS survey polygons (~57 MB)...")
    urllib.request.urlretrieve(OTLS_URL, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(out_dir)
    zip_path.unlink()
    print(f"Extracted OTLS shapefile to {out_dir}")


if __name__ == "__main__":
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 1200})

        for filename, link_id in MFT_DATASETS.items():
            out_path = out_dir / filename
            if out_path.exists():
                print(f"{filename} already exists, skipping")
                continue
            page.goto(f"https://mft.rrc.texas.gov/link/{link_id}", wait_until="networkidle")
            timeout = 1_800_000 if "PDQ" in filename else 300_000
            with page.expect_download(timeout=timeout) as dl:
                page.get_by_text(filename, exact=True).click()
            dl.value.save_as(str(out_path))
            print(f"Downloaded {filename}")

        browser.close()

    download_otls(out_dir)
