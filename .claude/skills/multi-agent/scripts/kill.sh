#!/bin/bash
# Usage: kill.sh <pane>
# Kill a specific pane (misbehaving agent)

SESSION="multi-agent"

if [[ $# -lt 1 ]]; then
    echo "Usage: kill.sh <pane-number>"
    echo ""
    echo "Available panes:"
    tmux list-panes -t "$SESSION" -F "  Pane #{pane_index}: #{pane_current_command}" 2>/dev/null || echo "  Session not running"
    exit 1
fi

PANE="$1"

echo "Killing pane $PANE..."
tmux kill-pane -t "${SESSION}:0.${PANE}"
echo "Done."
