#!/bin/bash
# Convert identifiers using LSEG Workspace SymbologySearch API
# Usage: lseg-symbology.sh "US0378331005" ISIN RIC
# Requires: LSEG_API_ENDPOINT and LSEG_APP_ID environment variables

set -e

if [ -z "$LSEG_API_ENDPOINT" ] || [ -z "$LSEG_APP_ID" ]; then
    echo "Error: LSEG_API_ENDPOINT and LSEG_APP_ID must be set" >&2
    echo "  export LSEG_API_ENDPOINT=http://localhost:9000/api/udf/" >&2
    echo "  export LSEG_APP_ID=your_app_key" >&2
    exit 1
fi

if [ $# -lt 3 ]; then
    echo "Usage: $0 'SYMBOL1,SYMBOL2' FROM_TYPE TO_TYPE" >&2
    echo "Types: RIC, ISIN, CUSIP, SEDOL, IMO, ticker" >&2
    echo "Example: $0 'US0378331005' ISIN RIC" >&2
    echo "Example: $0 '9754902,9780485' IMO RIC" >&2
    exit 1
fi

SYMBOLS="$1"
FROM_TYPE="$2"
TO_TYPE="$3"

# Convert comma-separated symbols to JSON array
SYMBOLS_JSON=$(echo "$SYMBOLS" | jq -R 'split(",") | map(gsub("^\\s+|\\s+$"; ""))')

# Build request
REQUEST=$(jq -n \
    --argjson symbols "$SYMBOLS_JSON" \
    --arg from "$FROM_TYPE" \
    --arg to "$TO_TYPE" \
    '{
        Entity: {
            E: "SymbologySearch",
            W: {
                symbols: $symbols,
                from: $from,
                to: [$to],
                bestMatch: true
            }
        }
    }')

curl -s -X POST "$LSEG_API_ENDPOINT" \
    -H "Content-Type: application/json" \
    -H "x-tr-applicationid: $LSEG_APP_ID" \
    -H "X-Forwarded-Host: localhost" \
    -d "$REQUEST"
