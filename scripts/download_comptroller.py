#!/usr/bin/env python3
"""Download Texas Comptroller open-records crude oil and natural gas tax files
from the SIFT portal at data-secure.comptroller.texas.gov.

Requires a free SIFT account (register at https://data-secure.comptroller.texas.gov/).
Pass credentials via the TX_COMPTROLLER_EMAIL and TX_COMPTROLLER_PASSWORD env vars.

Files downloaded (all from the public open-records group):
  LEASE.zip      master list of natural gas and crude oil leases (RRC ↔ CPA IDs)
  COMASTER.zip   crude oil taxpayer master (producer + purchaser names/addresses)
  NGMASTER.zip   natural gas taxpayer master (producer + purchaser names/addresses)
  DPvLease.zip   drilling permit ↔ lease number cross-reference
  COExempt.zip   crude oil exempt-lease records (with requestor taxpayer)
  NGExempt.zip   natural gas exempt-lease records

Note: the Comptroller does NOT publish lease-level purchaser linkage as bulk
data — form 10-161 (Purchaser Lease Detail Supplement) records are filed
monthly but only available via formal Open Records Request to
open.records@cpa.texas.gov.
"""
import os
import sys
from pathlib import Path
from playwright.sync_api import sync_playwright

FILES = [
    "LEASE.zip",
    "COMASTER.zip",
    "NGMASTER.zip",
    "DPvLease.zip",
    "COExempt.zip",
    "NGExempt.zip",
]


def main(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    pending = [f for f in FILES if not (out_dir / f).exists()]
    if not pending:
        print("All Comptroller files already present, skipping")
        return

    email = os.environ.get("TX_COMPTROLLER_EMAIL")
    password = os.environ.get("TX_COMPTROLLER_PASSWORD")
    if not email or not password:
        sys.exit("Set TX_COMPTROLLER_EMAIL and TX_COMPTROLLER_PASSWORD env vars")

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 1200})
        page.goto("https://data-secure.comptroller.texas.gov/", wait_until="networkidle")
        page.fill("#email", email)
        page.fill("#password", password)
        page.click("button[type=submit]")
        page.wait_for_url("**/main/view", timeout=30_000)
        page.goto("https://data-secure.comptroller.texas.gov/main/files/public-files",
                  wait_until="networkidle")
        # Sort by name so files cluster alphabetically
        page.get_by_role("button", name="Change sorting for display_name").click()
        page.wait_for_timeout(1000)

        for filename in pending:
            print(f"Downloading {filename}...")
            # Page through until the row is visible
            while True:
                row_visible = page.evaluate(
                    """(name) => {
                        const rows = Array.from(document.querySelectorAll('tbody tr'));
                        const r = rows.find(x => x.querySelector('td')?.innerText.trim() === name);
                        return !!r;
                    }""",
                    filename,
                )
                if row_visible:
                    break
                next_btn = page.locator('button[aria-label="Next page"]').first
                if next_btn.is_disabled():
                    sys.exit(f"Could not find {filename} on any page")
                next_btn.click()
                page.wait_for_timeout(500)

            with page.expect_download(timeout=120_000) as dl:
                page.evaluate(
                    """(name) => {
                        const rows = Array.from(document.querySelectorAll('tbody tr'));
                        rows.find(x => x.querySelector('td')?.innerText.trim() === name)
                            .querySelector('button').click();
                    }""",
                    filename,
                )
            dl.value.save_as(str(out_dir / filename))
            print(f"  → {out_dir / filename}")

        browser.close()


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/comptroller")
    main(out)
