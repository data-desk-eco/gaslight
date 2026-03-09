#!/bin/bash
# Fetch time series data from LSEG Workspace DataGrid API
# Usage: lseg-timeseries.sh "AAPL.O" "TR.PriceClose,TR.Volume" "2025-01-01" "2025-01-31"
# Requires: LSEG_API_ENDPOINT and LSEG_APP_ID environment variables

set -e

if [ -z "$LSEG_API_ENDPOINT" ] || [ -z "$LSEG_APP_ID" ]; then
    echo "Error: LSEG_API_ENDPOINT and LSEG_APP_ID must be set" >&2
    echo "  export LSEG_API_ENDPOINT=http://localhost:9000/api/udf/" >&2
    echo "  export LSEG_APP_ID=your_app_key" >&2
    exit 1
fi

if [ $# -lt 4 ]; then
    echo "Usage: $0 'INSTRUMENT' 'TR.Field1,TR.Field2' START_DATE END_DATE [TOP]" >&2
    echo "Example: $0 'AAPL.O' 'TR.PriceClose,TR.Volume' '2025-01-01' '2025-01-31'" >&2
    echo "Example: $0 'AAPL.O' 'TR.PriceClose' '2025-01-01' '2025-12-31' 10000" >&2
    exit 1
fi

INSTRUMENT="$1"
FIELDS="$2"
START_DATE="$3"
END_DATE="$4"
TOP="${5:-10000}"

# Convert comma-separated fields to JSON array of objects
FIELDS_JSON=$(echo "$FIELDS" | jq -R 'split(",") | map(gsub("^\\s+|\\s+$"; "") | {name: .})')

# Build request
REQUEST=$(jq -n \
    --arg instrument "$INSTRUMENT" \
    --argjson fields "$FIELDS_JSON" \
    --arg sdate "$START_DATE" \
    --arg edate "$END_DATE" \
    --argjson top "$TOP" \
    '{
        Entity: {
            E: "DataGrid",
            W: {
                instruments: [$instrument],
                fields: $fields,
                parameters: {
                    SDate: $sdate,
                    EDate: $edate,
                    TOP: $top
                }
            }
        }
    }')

curl -s -X POST "$LSEG_API_ENDPOINT" \
    -H "Content-Type: application/json" \
    -H "x-tr-applicationid: $LSEG_APP_ID" \
    -H "X-Forwarded-Host: localhost" \
    -d "$REQUEST"
