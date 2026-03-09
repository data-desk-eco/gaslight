# Texas RRC SWR 32 — Dark Flaring Analysis

Combines three data sources into a DuckDB database to identify unpermitted ("dark") flaring in the Permian Basin (districts 08, 7C, 8A):

1. **SWR 32 permits** — scraped from RRC public query tool (`scrape.sh`)
2. **RRC wellbore/P-5 data** — EBCDIC files from RRC MFT server, parsed to CSV
3. **VIIRS Nightfire (VNF)** — satellite flare detections from EOG

Pipeline is Makefile-driven. All data lives under `data/` (gitignored, reproducible). `uv` manages the Python project.
