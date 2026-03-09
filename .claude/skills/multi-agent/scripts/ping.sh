#!/bin/bash
# Usage: ping.sh [pane]
# Broadcast a ping to chat, or send directly to a pane

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ -n "$1" ]]; then
    # Direct ping to a specific pane
    "$SCRIPT_DIR/prompt.sh" "$1" "ping - please respond in chat"
    echo "Pinged pane $1"
else
    # Broadcast ping via chat
    "$SCRIPT_DIR/send.sh" "PING - who's active? Reply with 'pong'"
    echo "Broadcast ping sent. Check chat for responses."
fi
