#!/bin/bash
# Usage: watch.sh
# Tails the chat log (run in background)

CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"

mkdir -p "$(dirname "$CHAT_LOG")"
touch "$CHAT_LOG"
tail -f "$CHAT_LOG"
