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
