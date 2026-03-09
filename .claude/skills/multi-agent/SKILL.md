---
name: multi-agent
description: Multi-agent collaboration. Use when working with other Claude Code sessions, coordinating work, or needing to communicate with teammates.
---

# Multi-Agent Chat & Collaboration

## Quick Setup

1. Check recent chat history:
   ```bash
   .claude/skills/multi-agent/scripts/history.sh
   ```

2. Start monitoring chat in background (optional):
   ```bash
   .claude/skills/multi-agent/scripts/watch.sh &
   ```

3. (Optional) Override your username:
   ```bash
   export CC_CHAT_USER="myname"
   ```
   Note: If started via `start.sh`, you'll automatically have a unique identity (agent1, agent2, etc.)

## Sending Messages

```bash
.claude/skills/multi-agent/scripts/send.sh "your message here"
```

Messages appear as: `[14:32:05] [agent1] your message here`

**Important:**
- Keep messages single-line only. Multi-line messages break log parsing and make history harder to read.
- Do not escape exclamation marks with backslashes (e.g. write "Hello!" not "Hello\!") - the escapes appear literally in the chat log.

## Tmux Session

All agents work in panes of the shared `multi-agent` tmux session.

**Commands:**
```bash
# List panes
.claude/skills/multi-agent/scripts/session.sh list

# Join existing session (or create if missing)
.claude/skills/multi-agent/scripts/session.sh join

# Create new session
.claude/skills/multi-agent/scripts/session.sh create
```

**Navigation:**
- `Ctrl-b <arrow>` - Switch between panes
- `Ctrl-b d` - Detach from session
- `tmux attach -t multi-agent` - Reattach

**Tmux Helpers:**
```bash
# Read another pane's recent output (check on an agent)
.claude/skills/multi-agent/scripts/capture.sh 2        # last 50 lines from pane 2
.claude/skills/multi-agent/scripts/capture.sh 2 100    # last 100 lines

# Switch to a specific pane
.claude/skills/multi-agent/scripts/focus.sh 2

# Kill a misbehaving pane
.claude/skills/multi-agent/scripts/kill.sh 2
```

## Helping Other Agents

If an agent appears stuck or needs prompting:

```bash
# List panes first to find the pane number
.claude/skills/multi-agent/scripts/session.sh list

# Send input to that pane
.claude/skills/multi-agent/scripts/prompt.sh 2 "please continue with the implementation"
```

## Avoiding Conflicts

Use the status tracking system to coordinate file edits:

```bash
# Claim a file before editing
.claude/skills/multi-agent/scripts/status.sh set src/auth.ts

# Release when done
.claude/skills/multi-agent/scripts/status.sh clear

# See who's working on what
.claude/skills/multi-agent/scripts/status.sh list
```

**Additional tips:**
1. **Check chat when starting**: Run `history.sh` to see what others are working on
2. **Coordinate branches**: If working on separate branches, announce which branch
3. **Ask for help**: If blocked, ask other agents via chat

## Identity & Discovery

```bash
# See who's participating in chat
.claude/skills/multi-agent/scripts/who.sh

# Check your current identity
.claude/skills/multi-agent/scripts/whoami.sh
```

**Note:** When started via `start.sh`, agents automatically get unique identities (agent1, agent2, etc.) via `CC_CHAT_USER` and `CC_PANE_ID` environment variables.

## Staying Active

**Before going idle or asking for input, always:**

1. Check chat for new messages or direction:
   ```bash
   .claude/skills/multi-agent/scripts/history.sh | tail -10
   ```

2. If there's new work or direction, act on it instead of stopping

3. If truly idle with nothing to do, announce it in chat:
   ```bash
   .claude/skills/multi-agent/scripts/send.sh "Finished [task]. Available for new work."
   ```

This keeps the team flowing without needing external prompts.

## Task Coordination

Lightweight task tracking via chat messages (no external storage):

```bash
# Create a task
.claude/skills/multi-agent/scripts/task.sh add "implement feature X"

# Claim a task
.claude/skills/multi-agent/scripts/task.sh claim 1234

# Mark task complete
.claude/skills/multi-agent/scripts/task.sh done 1234

# List all tasks with status
.claude/skills/multi-agent/scripts/task.sh list
```

Tasks appear in chat as `[TASK:1234]`, claims as `[CLAIM:1234]`, completions as `[DONE:1234]`.

## Health Checks

```bash
# Ping all agents via chat (they should respond)
.claude/skills/multi-agent/scripts/ping.sh

# Ping a specific pane
.claude/skills/multi-agent/scripts/ping.sh 2
```

## Housekeeping

```bash
# Clear the chat log (optionally archive first)
.claude/skills/multi-agent/scripts/clear.sh           # clear without backup
.claude/skills/multi-agent/scripts/clear.sh --archive # backup to ~/.claude/chat-archives/ first
```

## Quick Reference

Run `help.sh` for a summary of all available scripts:

```bash
.claude/skills/multi-agent/scripts/help.sh
```

## Starting Multiple Agents (For Users)

```bash
# Spawn 3 agents with a shared prompt
python3 .claude/multi_agent.py start 3 "Implement feature X. Coordinate via chat."
```

This creates a tmux session with agents and drops you into the chat interface.

Other commands:
```bash
# Join chat for an existing session
python3 .claude/multi_agent.py chat

# Attach to tmux to see agent panes
python3 .claude/multi_agent.py attach
```
