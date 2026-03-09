#!/bin/bash
# Usage: prompt.sh <pane> "message"
# Sends input to another agent's pane

SESSION="multi-agent"

if [[ $# -lt 2 ]]; then
    echo "Usage: prompt.sh <pane-number> \"message\""
    echo ""
    echo "Available panes:"
    tmux list-panes -t "$SESSION" -F "  Pane #{pane_index}: #{pane_current_command}" 2>/dev/null || echo "  Session not running"
    exit 1
fi

PANE="$1"
shift
MESSAGE="$*"

tmux send-keys -t "${SESSION}:0.${PANE}" "$MESSAGE" Enter
echo "Sent to pane $PANE: $MESSAGE"
