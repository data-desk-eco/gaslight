#!/bin/bash
# Usage: session.sh [create|join|list]
# Manages shared tmux session "multi-agent"

SESSION="multi-agent"

case "${1:-list}" in
    create)
        if tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "Session '$SESSION' already exists. Use 'join' to attach."
            exit 1
        fi
        tmux new-session -d -s "$SESSION" -n main
        echo "Created session '$SESSION'. Use 'join' to attach."
        ;;
    join)
        if ! tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "Session '$SESSION' doesn't exist. Creating it..."
            tmux new-session -d -s "$SESSION" -n main
        fi
        tmux attach -t "$SESSION"
        ;;
    list)
        if tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "Session '$SESSION' panes:"
            tmux list-panes -t "$SESSION" -F "  Pane #{pane_index}: #{pane_current_command} (#{pane_width}x#{pane_height})"
        else
            echo "Session '$SESSION' does not exist."
        fi
        ;;
    *)
        echo "Usage: session.sh [create|join|list]"
        exit 1
        ;;
esac
