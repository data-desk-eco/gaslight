#!/bin/bash
# Usage: whoami.sh
# Shows your current agent identity

USER="${CC_CHAT_USER:-agent$$}"
echo "You are: $USER"

# Also show pane info if in tmux
if [[ -n "$TMUX" ]]; then
    PANE_INFO=$(tmux display-message -p "Pane #{pane_index} in session #{session_name}")
    echo "Tmux: $PANE_INFO"
fi
