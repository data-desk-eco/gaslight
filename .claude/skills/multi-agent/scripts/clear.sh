#!/bin/bash
# Usage: clear.sh [--archive]
# Clears the chat log. Use --archive to save a backup first.

CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"
ARCHIVE_DIR="${HOME}/.claude/chat-archives"

if [[ ! -f "$CHAT_LOG" ]]; then
    echo "No chat log to clear."
    exit 0
fi

if [[ "$1" == "--archive" ]]; then
    mkdir -p "$ARCHIVE_DIR"
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    ARCHIVE_FILE="${ARCHIVE_DIR}/chat_${TIMESTAMP}.log"
    cp "$CHAT_LOG" "$ARCHIVE_FILE"
    echo "Archived to: $ARCHIVE_FILE"
fi

# Clear the log
> "$CHAT_LOG"
echo "[$(date +"%H:%M:%S")] [system] Chat log cleared" >> "$CHAT_LOG"
echo "Chat log cleared."
