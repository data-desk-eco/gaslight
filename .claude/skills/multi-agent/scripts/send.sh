#!/bin/bash
# Usage: send.sh "message"
# Appends timestamped message to chat log

CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"
# Use CC_CHAT_USER if set, otherwise fall back to agent+pane_id or agent+pid
if [[ -n "$CC_CHAT_USER" ]]; then
    USER="$CC_CHAT_USER"
elif [[ -n "$CC_PANE_ID" ]]; then
    USER="agent${CC_PANE_ID}"
else
    USER="agent$$"
fi
TIMESTAMP=$(date +"%H:%M:%S")

mkdir -p "$(dirname "$CHAT_LOG")"
echo "[$TIMESTAMP] [$USER] $*" >> "$CHAT_LOG"
