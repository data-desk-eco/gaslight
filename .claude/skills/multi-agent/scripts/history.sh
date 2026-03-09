#!/bin/bash
# Usage: history.sh [n]
# Shows last n messages (default 20)

CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"
LINES="${1:-20}"

if [[ -f "$CHAT_LOG" ]]; then
    tail -n "$LINES" "$CHAT_LOG"
else
    echo "No chat history yet."
fi
