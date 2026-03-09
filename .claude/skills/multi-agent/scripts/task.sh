#!/bin/bash
# Usage: task.sh [add "desc"|claim <id>|done <id>|list]
# Lightweight task coordination via chat (no storage - tasks live in chat history)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CHAT_LOG="${CC_CHAT_LOG:-.claude/chat.log}"

case "${1:-list}" in
    add)
        shift
        if [[ -z "$1" ]]; then
            echo "Usage: task.sh add \"description\""
            exit 1
        fi
        ID=$(date +%s | tail -c 5)
        "$SCRIPT_DIR/send.sh" "[TASK:$ID] $*"
        echo "Posted task #$ID"
        ;;
    claim)
        if [[ -z "$2" ]]; then
            echo "Usage: task.sh claim <id>"
            exit 1
        fi
        "$SCRIPT_DIR/send.sh" "[CLAIM:$2] Taking this"
        echo "Claimed task #$2"
        ;;
    done)
        if [[ -z "$2" ]]; then
            echo "Usage: task.sh done <id>"
            exit 1
        fi
        "$SCRIPT_DIR/send.sh" "[DONE:$2] Completed"
        echo "Marked task #$2 as done"
        ;;
    list)
        echo "Tasks from chat history:"
        echo ""
        if [[ -f "$CHAT_LOG" ]]; then
            grep -E "\[TASK:[0-9]+\]" "$CHAT_LOG" | while read -r line; do
                TASK_ID=$(echo "$line" | grep -oE "TASK:[0-9]+" | cut -d: -f2)
                # Check if claimed or done
                if grep -q "\[DONE:$TASK_ID\]" "$CHAT_LOG" 2>/dev/null; then
                    STATUS="✓"
                elif grep -q "\[CLAIM:$TASK_ID\]" "$CHAT_LOG" 2>/dev/null; then
                    STATUS="◆"
                else
                    STATUS="○"
                fi
                echo "  $STATUS $line"
            done
        else
            echo "  (no chat log found)"
        fi
        echo ""
        echo "Legend: ○ open  ◆ claimed  ✓ done"
        ;;
    *)
        echo "Usage: task.sh [add \"desc\"|claim <id>|done <id>|list]"
        echo ""
        echo "Commands:"
        echo "  add \"description\"  Create a new task"
        echo "  claim <id>         Claim a task"
        echo "  done <id>          Mark task complete"
        echo "  list               Show all tasks (default)"
        ;;
esac
