#!/bin/bash
# Usage: who.sh
# Lists all agents who have sent messages in the chat

CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"

if [[ ! -f "$CHAT_LOG" ]]; then
    echo "No chat log found."
    exit 0
fi

echo "Active participants in chat:"
echo ""

# Extract unique usernames and their last message time
awk -F'[][]' '
    /^\[.*\] \[.*\]/ {
        time = $2
        user = $4
        if (user != "" && user != "system") {
            last_seen[user] = time
            count[user]++
        }
    }
    END {
        for (user in last_seen) {
            printf "  %-20s (last seen: %s, %d messages)\n", user, last_seen[user], count[user]
        }
    }
' "$CHAT_LOG" | sort
