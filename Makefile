WORKERS ?= 32
export WORKERS

.PHONY: all db refresh publish export release vendor serve permits permit-details wells vnf plumes r3 s2 clean help

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

data/permit_details.csv data/permit_properties.csv data/permit_locations.csv data/permit_attachments.csv: data/raw_html/.done
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

# --- S2 flare catalogue (fetched from permian-flaring) ---
#
# gaslight does NO S2 detection. permian-flaring (the detection engine + paper)
# is the upstream. `make s2` is a FETCH step (like `make plumes`/`make r3`): it
# refreshes the committed source file data/s2_catalogue.parquet from p-f's
# export, after which the normal pipeline (load → publish → export) ingests it
# like any other table. Run `make s2-export` in permian-flaring first to
# (re)build PF_CATALOGUE, then `make s2 && make db` to bake it into the DB.

PF_CATALOGUE := $(HOME)/Research/permian-flaring/data/s2_catalogue_detail.parquet
# Number of top-scoring catalogue sites to keep, applied in s2.sql. The full
# in-basin catalogue is ~60k sites — far more than the product needs.
S2_LIMIT     := 5000

s2: data/s2_catalogue.parquet

data/s2_catalogue.parquet: $(PF_CATALOGUE) queries/s2.sql
	duckdb \
		-c "SET VARIABLE pf_catalogue='$(PF_CATALOGUE)'" \
		-c "SET VARIABLE score_limit=$(S2_LIMIT)" \
		-c ".read queries/s2.sql"
	@echo "S2 catalogue: $@ ($$(du -h $@ | cut -f1)) — run 'make db' to ingest"

# --- database ---

refresh:
	rm -f data/data.duckdb dist/gaslight.duckdb
	$(MAKE) db

# Full pipeline: foundation (load → rrc) → publish (shareable DB + dict) → export (web parquets)
db: dist/gaslight.duckdb export

# Foundation: faithful raw load + normalised rrc tables
data/data.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done data/permit_locations.csv data/permit_details.csv data/permit_properties.csv data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv data/pdq/.done data/s2_catalogue.parquet queries/load.sql queries/rrc.sql
	@rm -f $@
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/rrc.sql
	@echo "Foundation ready: $@"

# --- shareable database ---

# dist/gaslight.duckdb: the single clean, whole-Permian product (also builds
# the permian.* schema inside data.duckdb that export.sql projects from) plus
# the data dictionary (nested markdown + in-DB _dictionary/_sources tables).
publish: dist/gaslight.duckdb

dist/gaslight.duckdb: data/data.duckdb queries/publish.sql docs/data-dictionary/_meta.yaml scripts/build_dictionary.py
	@rm -f $@
	@mkdir -p dist
	duckdb data/data.duckdb < queries/publish.sql
	uv run scripts/build_dictionary.py
	@echo "Shareable DB ready: $@ ($$(du -h $@ | cut -f1))"

# --- release ---

# Upload the locally-built shareable DB as a GitHub release asset.
# CI can't build it (the raw scraped source files are gitignored), so this
# publishes the DB from your machine. Re-running updates the asset in place.
RELEASE_TAG ?= db-latest
release: dist/gaslight.duckdb
	gh release view $(RELEASE_TAG) >/dev/null 2>&1 || \
		gh release create $(RELEASE_TAG) --title "Shareable database" \
			--notes "Standalone DuckDB database (main schema, no internals). Rebuilt from \`make publish\`." \
			--latest=false
	gh release upload $(RELEASE_TAG) dist/gaslight.duckdb --clobber
	@echo "Released: $$(gh release view $(RELEASE_TAG) --json url -q .url)"

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
	rm -f data/data.duckdb dist/gaslight.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv

help:
	@echo "gaslight — dark flaring analysis for the Permian Basin"
	@echo ""
	@echo "  make db              Full pipeline (load → rrc → publish → export)"
	@echo "  make refresh         Rebuild database from scratch"
	@echo "  make publish         Build shareable dist/gaslight.duckdb + data dictionary"
	@echo "  make export          Re-export parquets for web app"
	@echo "  make release         Upload dist/gaslight.duckdb to a GitHub release"
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
	@echo "  make s2              Fetch permian-flaring's S2 catalogue → data/s2_catalogue.parquet (then make db)"
	@echo ""
	@echo "  make clean           Remove derived data"
	@echo "  make help            This message"
