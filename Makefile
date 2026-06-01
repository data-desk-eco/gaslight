WORKERS ?= 32
export WORKERS

.PHONY: all db refresh publish export vendor serve permits permit-details wells vnf plumes r3 s2 clean help

all: db

# --- scrapers ---

permits: data/filings.csv
wells: data/wells.csv data/operators.csv
vnf: data/vnf_profiles/.done
plumes: data/plumes_cm.csv data/plumes_imeo.csv
r3: data/r3_facilities.csv
permit-details: data/permit_details.csv

data/filings.csv:
	uv run scripts/scrape_permits.py

data/raw_html/.done: data/filings.csv
	uv run scripts/scrape_permit_details.py
	@touch $@

data/permit_details.csv data/permit_properties.csv data/flare_locations.csv data/permit_attachments.csv: data/raw_html/.done
	uv run scripts/parse_permit_details.py

data/r3_facilities.csv:
	uv run scripts/fetch_r3.py

data/plumes_cm.csv data/plumes_imeo.csv:
	uv run scripts/fetch_plumes.py

data/.rrc_downloaded:
	uv run scripts/download_rrc.py data
	@touch $@

data/wells.csv data/operators.csv: data/.rrc_downloaded
	uv run scripts/parse_rrc.py data

data/pdq/.done: data/.rrc_downloaded
	mkdir -p data/pdq
	unzip -o data/PDQ_DSV.zip -d data/pdq
	@touch $@

data/vnf_profiles/.done:
	uv run scripts/fetch_vnf.py
	@touch $@

# --- S2 flare detection (via s2-flares CLI + Lambda) ---

S2_BBOX    := -104.5,30.0,-100.0,33.5
S2_START   := 2023-01-01
S2_CLOUD   := 50
S2_CONC    := 8

s2: web/data/s2.parquet

data/s2-raw.csv:
	cd ../s2-flares && bun cli.js \
		--bbox $(S2_BBOX) --start $(S2_START) --cloud $(S2_CLOUD) \
		--mode lambda --concurrency $(S2_CONC) \
		--min-dates 4 --min-avg-b12 0.85 \
		--out $(CURDIR)/$@

web/data/s2.parquet: data/s2-raw.csv queries/s2.sql
	duckdb < queries/s2.sql
	@echo "S2 precomputed: $@ ($$(du -h $@ | cut -f1))"

# --- database ---

refresh:
	rm -f data/data.duckdb dist/gaslight.duckdb
	$(MAKE) db

# Full pipeline: foundation (load → rrc) → publish (shareable DB + dict) → export (web parquets)
db: dist/gaslight.duckdb export

# Foundation: faithful raw load + normalised rrc tables
data/data.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done data/flare_locations.csv data/permit_details.csv data/permit_properties.csv data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv data/pdq/.done queries/load.sql queries/rrc.sql
	@rm -f $@
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/rrc.sql
	@echo "Foundation ready: $@"

# --- shareable database ---

# dist/gaslight.duckdb: the single clean, whole-Permian product (also builds
# the permian.* schema inside data.duckdb that export.sql projects from) plus
# the data dictionary (nested markdown + in-DB _dictionary/_sources tables).
publish: dist/gaslight.duckdb

# NB: publish.sql reads the existing web/data/s2.parquet at runtime but it is
# deliberately not a prerequisite — regenerating it is a separate `make s2` step.
dist/gaslight.duckdb: data/data.duckdb queries/publish.sql docs/data-dictionary/_meta.yaml scripts/build_dictionary.py
	@rm -f $@
	@mkdir -p dist
	duckdb data/data.duckdb < queries/publish.sql
	uv run scripts/build_dictionary.py
	@echo "Shareable DB ready: $@ ($$(du -h $@ | cut -f1))"

# --- web app ---

export: dist/gaslight.duckdb queries/export.sql
	@mkdir -p web/data
	duckdb data/data.duckdb < queries/export.sql
	@echo "Web parquets exported"

vendor:
	scripts/vendor.sh

serve:
	uv run python -m http.server 8080 -d web

clean:
	rm -f data/data.duckdb dist/gaslight.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv data/s2-raw.csv

help:
	@echo "gaslight — dark flaring analysis for the Permian Basin"
	@echo ""
	@echo "  make db              Full pipeline (load → rrc → publish → export)"
	@echo "  make refresh         Rebuild database from scratch"
	@echo "  make publish         Build shareable dist/gaslight.duckdb + data dictionary"
	@echo "  make export          Re-export parquets for web app"
	@echo "  make vendor          Download vendored JS dependencies"
	@echo "  make serve           Dev server on :8080"
	@echo ""
	@echo "  make permits         Scrape SWR 32 permit metadata"
	@echo "  make permit-details  Scrape + parse permit detail pages"
	@echo "  make wells           Download + parse RRC well/operator data"
	@echo "  make vnf             Fetch VNF profiles from EOG"
	@echo "  make plumes          Fetch Carbon Mapper + IMEO plumes"
	@echo "  make r3              Fetch RRC R-3 gas processing facilities"
	@echo ""
	@echo "  make s2              Run S2 flare detection (Lambda) + export parquet"
	@echo ""
	@echo "  make clean           Remove derived data"
	@echo "  make help            This message"
