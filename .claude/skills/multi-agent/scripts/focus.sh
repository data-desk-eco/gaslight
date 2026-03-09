#!/bin/bash
# Usage: focus.sh <pane>
# Switch to a specific pane

SESSION="multi-agent"

if [[ $# -lt 1 ]]; then
    echo "Usage: focus.sh <pane-number>"
    echo ""
    echo "Available panes:"
    tmux list-panes -t "$SESSION" -F "  Pane #{pane_index}: #{pane_current_command}" 2>/dev/null || echo "  Session not running"
    exit 1
fi

PANE="$1"

tmux select-pane -t "${SESSION}:0.${PANE}"
