#!/bin/bash
# Usage: status.sh [set <file>|clear|list]
# Track which files agents are working on to avoid conflicts

STATUS_FILE="${HOME}/.claude/agent-status.json"
USER="${CC_CHAT_USER:-agent$$}"

mkdir -p "$(dirname "$STATUS_FILE")"

case "${1:-list}" in
    set)
        if [[ -z "$2" ]]; then
            echo "Usage: status.sh set <file-path>"
            exit 1
        fi
        FILE="$2"
        TIMESTAMP=$(date +"%H:%M:%S")

        # Read existing status or start fresh
        if [[ -f "$STATUS_FILE" ]]; then
            # Remove old entry for this user, add new one
            jq --arg user "$USER" --arg file "$FILE" --arg time "$TIMESTAMP" \
                'del(.[$user]) | .[$user] = {"file": $file, "since": $time}' \
                "$STATUS_FILE" > "${STATUS_FILE}.tmp" && mv "${STATUS_FILE}.tmp" "$STATUS_FILE"
        else
            echo "{\"$USER\": {\"file\": \"$FILE\", \"since\": \"$TIMESTAMP\"}}" > "$STATUS_FILE"
        fi
        echo "Status set: $USER is working on $FILE"
        ;;
    clear)
        if [[ -f "$STATUS_FILE" ]]; then
            jq --arg user "$USER" 'del(.[$user])' "$STATUS_FILE" > "${STATUS_FILE}.tmp" \
                && mv "${STATUS_FILE}.tmp" "$STATUS_FILE"
            echo "Cleared status for $USER"
        else
            echo "No status file exists."
        fi
        ;;
    list)
        if [[ -f "$STATUS_FILE" ]] && [[ -s "$STATUS_FILE" ]]; then
            echo "Current file assignments:"
            echo ""
            jq -r 'to_entries | .[] | "  \(.key): \(.value.file) (since \(.value.since))"' "$STATUS_FILE"
        else
            echo "No agents have claimed files yet."
        fi
        ;;
    check)
        # Check if a file is claimed by someone
        if [[ -z "$2" ]]; then
            echo "Usage: status.sh check <file-path>"
            exit 1
        fi
        FILE="$2"
        if [[ -f "$STATUS_FILE" ]]; then
            OWNER=$(jq -r --arg file "$FILE" 'to_entries | .[] | select(.value.file == $file) | .key' "$STATUS_FILE")
            if [[ -n "$OWNER" ]]; then
                echo "Warning: $FILE is being worked on by $OWNER"
                exit 1
            fi
        fi
        echo "File $FILE is not claimed by anyone."
        ;;
    *)
        echo "Usage: status.sh [set <file>|clear|list|check <file>]"
        echo ""
        echo "Commands:"
        echo "  set <file>    Claim a file you're working on"
        echo "  clear         Release your current file claim"
        echo "  list          Show all current file claims"
        echo "  check <file>  Check if a file is claimed"
        exit 1
        ;;
esac
