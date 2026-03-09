#!/bin/bash
# Usage: test.sh
# Validates that all multi-agent scripts are working

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASS=0
FAIL=0

check() {
    local name="$1"
    local cmd="$2"
    if eval "$cmd" >/dev/null 2>&1; then
        echo "  ✓ $name"
        ((PASS++))
    else
        echo "  ✗ $name"
        ((FAIL++))
    fi
}

echo "Testing multi-agent scripts..."
echo ""

# Test scripts exist and are executable
echo "Script existence:"
for script in send history watch who clear whoami status ping task help session capture focus prompt kill; do
    check "$script.sh exists" "[[ -x '$SCRIPT_DIR/$script.sh' ]]"
done

echo ""
echo "Functionality tests:"

# Test send/history roundtrip
TEST_MSG="test_$(date +%s)"
export CC_CHAT_USER="test"
"$SCRIPT_DIR/send.sh" "$TEST_MSG"
check "send.sh + history.sh roundtrip" "grep -q '$TEST_MSG' .claude/chat.log"

# Test who.sh
check "who.sh runs" "$SCRIPT_DIR/who.sh | grep -q 'participants'"

# Test whoami.sh
check "whoami.sh runs" "$SCRIPT_DIR/whoami.sh | grep -q 'You are'"

# Test status.sh
check "status.sh list runs" "$SCRIPT_DIR/status.sh list"

# Test task.sh
check "task.sh list runs" "$SCRIPT_DIR/task.sh list | grep -q 'Legend'"

# Test help.sh
check "help.sh runs" "$SCRIPT_DIR/help.sh | grep -q 'Multi-Agent'"

# Test session.sh
check "session.sh list runs" "$SCRIPT_DIR/session.sh list"

echo ""
echo "Results: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] && echo "All tests passed!" || exit 1
