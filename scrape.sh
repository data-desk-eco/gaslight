#!/usr/bin/env bash
set -euo pipefail

# Texas RRC SWR 32 Exceptions Scraper
# Downloads all PDFs and metadata from the public query tool at:
# https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml
#
# Usage: ./scrape.sh {metadata|documents|download|combine|all}

BASE="https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
DPIMG="https://webapps.rrc.state.tx.us/dpimages/r"
WORKERS=${WORKERS:-32}  # parallel workers for Phase 2 & 3
DATA_DIR="data"
PDF_DIR="$DATA_DIR/pdfs"
COOKIES="$DATA_DIR/.cookies"
METADATA_CSV="$DATA_DIR/filings.csv"
DOCS_CSV="$DATA_DIR/docs.csv"
STATE_FILE="$DATA_DIR/.state"
TODAY=$(date +%m/%d/%Y)
TODAY_URL=$(printf '%s' "$TODAY" | sed 's|/|%2F|g')
SEARCH_FROM="01/01/2019"
SEARCH_FROM_URL="01%2F01%2F2019"

mkdir -p "$PDF_DIR"

# --- helpers ---

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# Retry a curl command up to 3 times with backoff
curl_retry() {
  local attempt=0
  while [ $attempt -lt 3 ]; do
    if curl --connect-timeout 15 --max-time 60 "$@" ; then
      return 0
    fi
    attempt=$(( attempt + 1 ))
    log "RETRY: curl failed (attempt $attempt/3)"
  done
  log "ERROR: curl failed after 3 attempts"
  return 1
}

# Check if a response looks valid (not empty, no ViewExpiredException)
response_ok() {
  local f="$1"
  [ -f "$f" ] && [ -s "$f" ] || return 1
  ! grep -q 'ViewExpiredException' "$f" 2>/dev/null
}

get_viewstate() {
  local f="$1" vs=""
  vs=$(grep -o 'name="javax.faces.ViewState"[^/]*' "$f" 2>/dev/null \
    | head -1 | sed 's/.*value="//;s/".*//') || true
  if [ -z "$vs" ]; then
    vs=$(grep -o 'javax.faces.ViewState:0">.*]]' "$f" 2>/dev/null \
      | head -1 | sed 's/.*CDATA\[//;s/]].*//' ) || true
  fi
  echo "$vs"
}

init_session() {
  rm -f "$COOKIES"
  curl_retry -s -c "$COOKIES" "$BASE" -o "$DATA_DIR/.page.html"
  local vs=$(get_viewstate "$DATA_DIR/.page.html")
  if [ -z "$vs" ]; then
    log "WARN: init_session got empty ViewState, retrying..."
    rm -f "$COOKIES"
    curl_retry -s -c "$COOKIES" "$BASE" -o "$DATA_DIR/.page.html"
    vs=$(get_viewstate "$DATA_DIR/.page.html")
  fi
  echo "$vs"
}

do_search() {
  local vs="$1" filing="${2:-}" date_from="${3:-}" date_to="${4:-}"
  local out="$DATA_DIR/.search.xml"
  local data="javax.faces.partial.ajax=true"
  data="$data&javax.faces.source=pbqueryForm%3AsearchExceptions"
  data="$data&javax.faces.partial.execute=%40all"
  data="$data&javax.faces.partial.render=pbqueryForm%3ApQueryTable"
  data="$data&pbqueryForm%3AsearchExceptions=pbqueryForm%3AsearchExceptions"
  data="$data&pbqueryForm=pbqueryForm"
  data="$data&javax.faces.ViewState=$(printf '%s' "$vs" | sed 's/:/%3A/g')"
  data="$data&pbqueryForm%3AfilingTypeList_focus="
  data="$data&pbqueryForm%3AfilingTypeList_input="
  data="$data&pbqueryForm%3ApermanentException_focus="
  data="$data&pbqueryForm%3ApermanentException_input="
  data="$data&pbqueryForm%3Aswr32h8_focus="
  data="$data&pbqueryForm%3Aswr32h8_input="
  data="$data&pbqueryForm%3ApropertyTypeList_focus="
  data="$data&pbqueryForm%3ApropertyTypeList_input="

  if [ -n "$filing" ]; then
    data="$data&pbqueryForm%3AfilingNumber_input=$filing"
    data="$data&pbqueryForm%3AfilingNumber_hinput=$filing"
  fi
  if [ -n "$date_from" ]; then
    local df_url=$(printf '%s' "$date_from" | sed 's|/|%2F|g')
    local dt_url=$(printf '%s' "$date_to" | sed 's|/|%2F|g')
    data="$data&pbqueryForm%3AsubmittalDateFrom_input=$df_url"
    data="$data&pbqueryForm%3AsubmittalDateTo_input=$dt_url"
  fi

  curl_retry -s -b "$COOKIES" -c "$COOKIES" \
    -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
    -H 'Faces-Request: partial/ajax' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -d "$data" "$BASE" -o "$out"

  echo "$out"
}

do_paginate() {
  local vs="$1" first="$2"
  local out="$DATA_DIR/.paginate.xml"
  local vs_encoded=$(printf '%s' "$vs" | sed 's/:/%3A/g')

  curl_retry -s -b "$COOKIES" -c "$COOKIES" \
    -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
    -H 'Faces-Request: partial/ajax' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -d "javax.faces.partial.ajax=true&javax.faces.source=pbqueryForm%3ApQueryTable&javax.faces.partial.execute=pbqueryForm%3ApQueryTable&javax.faces.partial.render=pbqueryForm%3ApQueryTable&javax.faces.behavior.event=page&javax.faces.partial.event=page&pbqueryForm%3ApQueryTable_pagination=true&pbqueryForm%3ApQueryTable_first=${first}&pbqueryForm%3ApQueryTable_rows=10&pbqueryForm%3ApQueryTable_encodeFeature=true&pbqueryForm%3ApQueryTable_rppDD=10&pbqueryForm=pbqueryForm&pbqueryForm%3AfilingTypeList_focus=&pbqueryForm%3AfilingTypeList_input=&pbqueryForm%3ApermanentException_focus=&pbqueryForm%3ApermanentException_input=&pbqueryForm%3Aswr32h8_focus=&pbqueryForm%3Aswr32h8_input=&pbqueryForm%3ApropertyTypeList_focus=&pbqueryForm%3ApropertyTypeList_input=&pbqueryForm%3AsubmittalDateFrom_input=${SEARCH_FROM_URL}&pbqueryForm%3AsubmittalDateTo_input=${TODAY_URL}&pbqueryForm%3ApQueryTable%3Aj_idt152%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt154%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt156%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt158%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt160%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt162%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt164%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt166%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt168%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt170%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt172%3Afilter=&pbqueryForm%3ApQueryTable_selection=&pbqueryForm%3ApQueryTable_resizableColumnState=&javax.faces.ViewState=${vs_encoded}" \
    "$BASE" -o "$out"

  echo "$out"
}

view_detail() {
  local vs="$1" row="$2"
  local out="$DATA_DIR/.detail.html"
  local vs_encoded=$(printf '%s' "$vs" | sed 's/:/%3A/g')

  curl_retry -s -L -b "$COOKIES" -c "$COOKIES" \
    -d "pbqueryForm=pbqueryForm&javax.faces.ViewState=${vs_encoded}&pbqueryForm%3ApQueryTable%3A${row}%3Aj_idt150=pbqueryForm%3ApQueryTable%3A${row}%3Aj_idt150" \
    "$BASE" -o "$out"

  echo "$out"
}

parse_table_rows() {
  local f="$1"
  # Split tags onto separate lines, extract gridcell contents, strip HTML
  sed 's/></>\
</g' "$f" \
    | grep 'role="gridcell"' \
    | sed 's/.*role="gridcell"[^>]*>//;s/<\/td.*//;s/<[^>]*>//g' \
    | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
    > "$DATA_DIR/.cells.tmp"

  # 12 cells per row; skip cell 0 (actions button), output cells 1-11
  awk -v OFS='\t' '
    { cells[NR-1] = $0 }
    END {
      for (r = 0; r * 12 + 11 < NR; r++) {
        base = r * 12
        printf "%s", cells[base+1]
        for (c = 2; c <= 11; c++) printf "\t%s", cells[base+c]
        print ""
      }
    }
  ' "$DATA_DIR/.cells.tmp"
}

parse_detail_docs() {
  local f="$1"
  # Pair up doc IDs, filenames, and file types from the attachment table
  local ids_file="$DATA_DIR/.doc_ids.tmp"
  local names_file="$DATA_DIR/.doc_names.tmp"
  local types_file="$DATA_DIR/.doc_types.tmp"

  grep -o 'dpimages[^0-9]*r[^0-9]*[0-9][0-9]*' "$f" \
    | grep -o '[0-9][0-9]*$' > "$ids_file"

  sed -n '/attachmentTable/,/<\/table>/p' "$f" \
    | grep -o 'text-align: left; ">[^<]*' \
    | sed 's/text-align: left; ">//' > "$names_file"

  sed -n '/attachmentTable/,/<\/table>/p' "$f" \
    | grep -o 'text-align: center; width:30%">[^<]*' \
    | sed 's/text-align: center; width:30%">//' \
    | sed '/^$/d' > "$types_file"

  paste "$ids_file" "$names_file" "$types_file" 2>/dev/null || true
}

get_total_records() {
  grep -o 'out of [0-9]* records' "$1" | head -1 | sed 's/out of //;s/ records//'
}

save_state() { echo "$1" > "$STATE_FILE"; }
load_state() { cat "$STATE_FILE" 2>/dev/null || echo ""; }

# --- Phase 1: Collect metadata from search results ---

phase_metadata() {
  log "Phase 1: Collecting metadata from search results"

  if [ ! -f "$METADATA_CSV" ]; then
    printf 'excep_seq\tsubmittal_dt\tfiling_no\tstatus\tfiling_type\toperator_no\toperator_name\tproperty\teffective_dt\texpiration_dt\tfv_district\n' > "$METADATA_CSV"
  fi

  local state=$(load_state)
  local start_page=0
  case "$state" in meta:*) start_page="${state#meta:}" ; log "Resuming from page $start_page" ;; esac

  log "Initializing session..."
  local vs=$(init_session)

  log "Searching ($SEARCH_FROM to $TODAY)..."
  local search_result=$(do_search "$vs" "" "$SEARCH_FROM" "$TODAY")
  vs=$(get_viewstate "$search_result")

  local total=$(get_total_records "$search_result")
  log "Found $total records"

  local total_pages=$(( (total + 9) / 10 ))

  if [ "$start_page" -eq 0 ]; then
    parse_table_rows "$search_result" >> "$METADATA_CSV"
    save_state "meta:1"
    start_page=1
    log "Page 1/$total_pages"
  fi

  local page=0 first=0 fail_count=0
  for page in $(seq "$start_page" $(( total_pages - 1 )) ); do
    first=$(( page * 10 ))
    local page_result=$(do_paginate "$vs" "$first")

    # Validate response; on failure, reinit session and re-search
    if ! response_ok "$page_result"; then
      log "WARN: Bad response on page $((page + 1)), reinitializing session..."
      vs=$(init_session)
      do_search "$vs" "" "$SEARCH_FROM" "$TODAY" > /dev/null
      vs=$(get_viewstate "$DATA_DIR/.search.xml")
      page_result=$(do_paginate "$vs" "$first")
      if ! response_ok "$page_result"; then
        fail_count=$(( fail_count + 1 ))
        if [ $fail_count -ge 5 ]; then
          log "ERROR: Too many consecutive failures, stopping"
          break
        fi
        log "WARN: Still failing, skipping page $((page + 1))"
        continue
      fi
      fail_count=0
    else
      fail_count=0
    fi

    local new_vs=$(get_viewstate "$page_result")
    [ -n "$new_vs" ] && vs="$new_vs"
    parse_table_rows "$page_result" >> "$METADATA_CSV"
    save_state "meta:$((page + 1))"
    log "Page $((page + 1))/$total_pages"
  done

  # Deduplicate
  local tmp="$DATA_DIR/.dedup.tmp"
  head -1 "$METADATA_CSV" > "$tmp"
  tail -n +2 "$METADATA_CSV" | sort -t'	' -k3,3 -u >> "$tmp"
  mv "$tmp" "$METADATA_CSV"

  local count=$(( $(wc -l < "$METADATA_CSV") - 1 ))
  log "Phase 1 complete: $count unique filings"
  save_state "meta:done"
}

# --- Phase 2: Get document IDs from detail pages (parallel workers) ---

_documents_worker() {
  local worker_id="$1" shard_file="$2" out_file="$3"
  local d="$DATA_DIR/.w${worker_id}"
  mkdir -p "$d"
  local cookies="$d/c" i=0

  # Init worker session
  curl_retry -s -c "$cookies" "$BASE" -o "$d/p"
  local vs=$(get_viewstate "$d/p")

  _reinit_session() {
    rm -f "$cookies"
    curl_retry -s -c "$cookies" "$BASE" -o "$d/p"
    vs=$(get_viewstate "$d/p")
  }

  while IFS= read -r fn; do
    [ -z "$fn" ] && continue
    i=$(( i + 1 ))

    # Search by filing number
    local ve=$(printf '%s' "$vs" | sed 's/:/%3A/g')
    curl_retry -s -b "$cookies" -c "$cookies" \
      -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
      -H 'Faces-Request: partial/ajax' \
      -H 'X-Requested-With: XMLHttpRequest' \
      -d "javax.faces.partial.ajax=true&javax.faces.source=pbqueryForm%3AsearchExceptions&javax.faces.partial.execute=%40all&javax.faces.partial.render=pbqueryForm%3ApQueryTable&pbqueryForm%3AsearchExceptions=pbqueryForm%3AsearchExceptions&pbqueryForm=pbqueryForm&javax.faces.ViewState=${ve}&pbqueryForm%3AfilingNumber_input=${fn}&pbqueryForm%3AfilingNumber_hinput=${fn}&pbqueryForm%3AfilingTypeList_focus=&pbqueryForm%3AfilingTypeList_input=&pbqueryForm%3ApermanentException_focus=&pbqueryForm%3ApermanentException_input=&pbqueryForm%3Aswr32h8_focus=&pbqueryForm%3Aswr32h8_input=&pbqueryForm%3ApropertyTypeList_focus=&pbqueryForm%3ApropertyTypeList_input=" \
      "$BASE" -o "$d/s"

    if ! response_ok "$d/s"; then
      _reinit_session
      continue
    fi
    vs=$(get_viewstate "$d/s")

    local count=$(get_total_records "$d/s" 2>/dev/null)
    if [ -z "$count" ] || [ "$count" = "0" ]; then
      printf '%s\t\t\t\n' "$fn" >> "$out_file"
      continue
    fi

    # View detail page
    ve=$(printf '%s' "$vs" | sed 's/:/%3A/g')
    curl_retry -s -L -b "$cookies" -c "$cookies" \
      -d "pbqueryForm=pbqueryForm&javax.faces.ViewState=${ve}&pbqueryForm%3ApQueryTable%3A0%3Aj_idt150=pbqueryForm%3ApQueryTable%3A0%3Aj_idt150" \
      "$BASE" -o "$d/d"

    if ! response_ok "$d/d"; then
      _reinit_session
      printf '%s\t\t\t\n' "$fn" >> "$out_file"
      continue
    fi

    # Extract documents
    local ids=$(grep -o 'dpimages[^0-9]*r[^0-9]*[0-9][0-9]*' "$d/d" 2>/dev/null | grep -o '[0-9][0-9]*$')
    if [ -n "$ids" ]; then
      local names_file="$d/n" types_file="$d/t"
      sed -n '/attachmentTable/,/<\/table>/p' "$d/d" \
        | grep -o 'text-align: left; ">[^<]*' \
        | sed 's/text-align: left; ">//' > "$names_file"
      sed -n '/attachmentTable/,/<\/table>/p' "$d/d" \
        | grep -o 'text-align: center; width:30%">[^<]*' \
        | sed 's/text-align: center; width:30%">//' \
        | sed '/^$/d' > "$types_file"
      paste <(echo "$ids") "$names_file" "$types_file" 2>/dev/null \
        | while IFS='	' read -r did fname ftype; do
            printf '%s\t%s\t%s\t%s\n' "$fn" "$did" "$fname" "$ftype"
          done >> "$out_file"
    else
      printf '%s\t\t\t\n' "$fn" >> "$out_file"
    fi

    # Reuse ViewState from detail page instead of navigating back (saves 1 RT)
    local dv=$(get_viewstate "$d/d")
    if [ -n "$dv" ]; then
      vs="$dv"
    else
      _reinit_session
    fi

    [ $(( i % 50 )) -eq 0 ] && log "W$worker_id: $i done"
  done < "$shard_file"

  rm -rf "$d"
  log "W$worker_id: finished ($i)"
}

phase_documents() {
  log "Phase 2: Collecting document IDs ($WORKERS workers)"

  [ -f "$METADATA_CSV" ] || { log "ERROR: Run 'metadata' first"; exit 1; }
  [ -f "$DOCS_CSV" ] || printf 'filing_no\tdoc_id\tfilename\tfile_type\n' > "$DOCS_CSV"

  # Find remaining filings (not yet in docs.csv)
  local remaining="$DATA_DIR/.remaining.tmp"
  comm -23 \
    <(tail -n +2 "$METADATA_CSV" | cut -f3 | sort -u) \
    <(tail -n +2 "$DOCS_CSV" 2>/dev/null | cut -f1 | sort -u) \
    > "$remaining"

  local count=$(wc -l < "$remaining" | tr -d ' ')
  log "$count filings remaining"
  [ "$count" -eq 0 ] && return

  # Split into shards and launch workers
  local per_shard=$(( (count + WORKERS - 1) / WORKERS ))
  split -l "$per_shard" "$remaining" "$DATA_DIR/.shard_"

  local pids="" w=0
  for shard in "$DATA_DIR"/.shard_*; do
    _documents_worker "$w" "$shard" "$DATA_DIR/.docs_w${w}.tsv" &
    pids="$pids $!"
    w=$(( w + 1 ))
  done

  log "Launched $w workers"
  for pid in $pids; do wait "$pid" 2>/dev/null; done

  # Merge
  cat "$DATA_DIR"/.docs_w*.tsv >> "$DOCS_CSV" 2>/dev/null
  rm -f "$DATA_DIR"/.shard_* "$DATA_DIR"/.docs_w*.tsv "$remaining"

  log "Phase 2 complete: $(( $(wc -l < "$DOCS_CSV") - 1 )) entries"
}

# --- Phase 3: Download all documents (parallel) ---

phase_download() {
  log "Phase 3: Downloading documents ($WORKERS workers)"

  [ -f "$DOCS_CSV" ] || { log "ERROR: Run 'documents' first"; exit 1; }

  # Build download list (skip already-downloaded)
  local dl_list="$DATA_DIR/.downloads.tmp"
  : > "$dl_list"
  tail -n +2 "$DOCS_CSV" | while IFS='	' read -r filing_no doc_id filename file_type; do
    [ -z "$doc_id" ] && continue
    local ext="${filename##*.}"
    [ -z "$ext" ] || [ "$ext" = "$filename" ] && ext="pdf"
    local outfile="$PDF_DIR/${filing_no}_${doc_id}.${ext}"
    [ -f "$outfile" ] && [ -s "$outfile" ] && continue
    printf '%s\t%s\n' "$doc_id" "$outfile" >> "$dl_list"
  done

  local total=$(wc -l < "$dl_list" | tr -d ' ')
  log "$total files to download"
  [ "$total" -eq 0 ] && { log "Nothing to download"; return; }

  # Use xargs for parallel downloads
  awk -F'\t' -v base="$DPIMG" '{print base "/" $1 "\n  output = " $2}' "$dl_list" \
    | curl --connect-timeout 15 --max-time 120 -s -L --parallel --parallel-max "$WORKERS" \
           --retry 2 --config -

  # Remove any HTML error pages that got saved as PDFs
  for f in "$PDF_DIR"/*; do
    [ -f "$f" ] && head -c 200 "$f" 2>/dev/null | grep -q '<html' && rm -f "$f"
  done

  rm -f "$dl_list"
  log "Phase 3 complete"
}

# --- Phase 4: Build final combined CSV ---

phase_combine() {
  log "Building combined CSV"

  local combined="$DATA_DIR/swr32_exceptions.csv"

  # Use awk to join metadata with documents (handles empty TSV fields correctly)
  awk -F'\t' -v OFS='\t' -v docs_file="$DOCS_CSV" '
    BEGIN {
      # Load docs into associative arrays keyed by filing_no
      while ((getline line < docs_file) > 0) {
        n = split(line, f, "\t")
        fn = f[1]
        if (fn == "filing_no" || fn == "") continue
        did = f[2]
        if (did == "") continue
        fname = f[3]
        # Determine extension
        ext = fname
        sub(/.*\./, "", ext)
        if (ext == fname || ext == "") ext = "pdf"
        if (doc_ids[fn] != "") {
          doc_ids[fn] = doc_ids[fn] ";"
          pdf_files[fn] = pdf_files[fn] ";"
        }
        doc_ids[fn] = doc_ids[fn] did
        pdf_files[fn] = pdf_files[fn] fn "_" did "." ext
      }
    }
    NR == 1 {
      print $0, "doc_ids", "pdf_files"
      next
    }
    {
      fn = $3
      print $0, doc_ids[fn], pdf_files[fn]
    }
  ' "$METADATA_CSV" > "$combined"

  local count=$(( $(wc -l < "$combined") - 1 ))
  log "Combined CSV: $combined ($count records)"
}

# --- main ---

case "${1:-}" in
  metadata)  phase_metadata ;;
  documents) phase_documents ;;
  download)  phase_download ;;
  combine)   phase_combine ;;
  all)       phase_metadata; phase_documents; phase_download; phase_combine ;;
  *)
    echo "Usage: $0 {metadata|documents|download|combine|all}" >&2
    echo "  metadata   Paginate search results, collect filing metadata" >&2
    echo "  documents  Visit each filing detail page for document IDs" >&2
    echo "  download   Download all documents" >&2
    echo "  combine    Build final CSV joining metadata + documents" >&2
    echo "  all        Run all phases" >&2
    exit 1
    ;;
esac
