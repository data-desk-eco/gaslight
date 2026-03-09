#!/bin/bash
# Query LSEG Workspace DataGrid API
# Usage: lseg-query.sh "AAPL.O,MSFT.O" "TR.CommonName,TR.PriceClose"
# Requires: LSEG_API_ENDPOINT and LSEG_APP_ID environment variables

set -e

if [ -z "$LSEG_API_ENDPOINT" ] || [ -z "$LSEG_APP_ID" ]; then
    echo "Error: LSEG_API_ENDPOINT and LSEG_APP_ID must be set" >&2
    echo "  export LSEG_API_ENDPOINT=http://localhost:9000/api/udf/" >&2
    echo "  export LSEG_APP_ID=your_app_key" >&2
    exit 1
fi

if [ $# -lt 2 ]; then
    echo "Usage: $0 'INSTRUMENT1,INSTRUMENT2' 'TR.Field1,TR.Field2'" >&2
    echo "Example: $0 'AAPL.O,MSFT.O' 'TR.CommonName,TR.PriceClose'" >&2
    exit 1
fi

INSTRUMENTS="$1"
FIELDS="$2"

# Convert comma-separated instruments to JSON array
INSTRUMENTS_JSON=$(echo "$INSTRUMENTS" | jq -R 'split(",") | map(gsub("^\\s+|\\s+$"; ""))')

# Convert comma-separated fields to JSON array of objects
FIELDS_JSON=$(echo "$FIELDS" | jq -R 'split(",") | map(gsub("^\\s+|\\s+$"; "") | {name: .})')

# Build request
REQUEST=$(jq -n \
    --argjson instruments "$INSTRUMENTS_JSON" \
    --argjson fields "$FIELDS_JSON" \
    '{
        Entity: {
            E: "DataGrid",
            W: {
                instruments: $instruments,
                fields: $fields
            }
        }
    }')

curl -s -X POST "$LSEG_API_ENDPOINT" \
    -H "Content-Type: application/json" \
    -H "x-tr-applicationid: $LSEG_APP_ID" \
    -H "X-Forwarded-Host: localhost" \
    -d "$REQUEST"
