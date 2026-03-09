#!/bin/bash
# Usage: capture.sh <pane> [lines]
# Read another pane's recent output

SESSION="multi-agent"

if [[ $# -lt 1 ]]; then
    echo "Usage: capture.sh <pane-number> [lines]"
    echo ""
    echo "Available panes:"
    tmux list-panes -t "$SESSION" -F "  Pane #{pane_index}: #{pane_current_command}" 2>/dev/null || echo "  Session not running"
    exit 1
fi

PANE="$1"
LINES="${2:-50}"

tmux capture-pane -t "${SESSION}:0.${PANE}" -p -S "-${LINES}"
