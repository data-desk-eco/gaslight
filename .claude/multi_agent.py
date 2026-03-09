#!/usr/bin/env python3
"""
Multi-agent collaboration tool for Claude Code.

Usage:
    multi_agent.py start <num-agents> [prompt]   - Spawn agents and enter chat
    multi_agent.py chat [--no-kill]              - Just open chat interface
    multi_agent.py attach                        - Attach to tmux session

Examples:
    ./multi_agent.py start 3 "Implement feature X. Coordinate via chat."
    ./multi_agent.py start 2                     # Start agents without prompt
    ./multi_agent.py chat --no-kill
    ./multi_agent.py attach
"""
import argparse
import curses
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from threading import Thread, Event


TMUX_SESSION = os.environ.get("CC_TMUX_SESSION", "multi-agent")
CHAT_LOG = os.environ.get("CC_CHAT_LOG", ".claude/chat.log")
USER = os.environ.get("CC_CHAT_USER", "human")


def ensure_chat_log():
    """Create chat log if it doesn't exist."""
    Path(CHAT_LOG).parent.mkdir(parents=True, exist_ok=True)
    Path(CHAT_LOG).touch()


def log_message(user: str, msg: str):
    """Append a message to the chat log."""
    if not msg.strip():
        return
    timestamp = time.strftime("%H:%M:%S")
    with open(CHAT_LOG, "a") as f:
        f.write(f"[{timestamp}] [{user}] {msg}\n")


def run_cmd(cmd: list, check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """Run a shell command."""
    return subprocess.run(cmd, capture_output=True, text=True, check=check, **kwargs)


def tmux_session_exists() -> bool:
    """Check if tmux session exists."""
    result = run_cmd(["tmux", "has-session", "-t", TMUX_SESSION], check=False)
    return result.returncode == 0


def kill_tmux_session():
    """Kill the tmux session."""
    if tmux_session_exists():
        run_cmd(["tmux", "kill-session", "-t", TMUX_SESSION], check=False)


def cmd_start(args):
    """Start N agents with optional prompt and enter chat."""
    num_agents = args.num_agents
    prompt = args.prompt

    if num_agents < 1:
        print("Error: Need at least 1 agent", file=sys.stderr)
        sys.exit(1)

    ensure_chat_log()

    # Kill existing session
    if tmux_session_exists():
        print(f"Killing existing session '{TMUX_SESSION}'...")
        kill_tmux_session()

    # Build agent command - with or without initial prompt
    def make_agent_cmd(agent_num: int) -> str:
        base = f"CC_CHAT_USER=agent{agent_num} CC_PANE_ID={agent_num} CC_CHAT_LOG='{CHAT_LOG}' claude --dangerously-skip-permissions"
        if prompt:
            return f"{base} \"{prompt}\""
        # Without prompt, give minimal instruction to activate multi-agent skill
        default_prompt = "Invoke the multi-agent skill and await instructions in chat."
        return f"{base} \"{default_prompt}\""

    # Create new session with first agent
    run_cmd([
        "tmux", "new-session", "-d", "-s", TMUX_SESSION, "-n", "main",
        make_agent_cmd(1)
    ])

    # Log session start
    if prompt:
        log_message("system", f"Starting {num_agents} agents with prompt: {prompt}")
    else:
        log_message("system", f"Starting {num_agents} agents (awaiting instructions)")

    # Spawn remaining agent panes
    for i in range(2, num_agents + 1):
        run_cmd(["tmux", "split-window", "-t", TMUX_SESSION, "-h", make_agent_cmd(i)])
        run_cmd(["tmux", "select-layout", "-t", TMUX_SESSION, "tiled"])

    print(f"Started {num_agents} agents in tmux session '{TMUX_SESSION}'")
    print("Entering chat interface... (Ctrl-D to exit and kill session, Ctrl-C to just exit)")
    print()

    # Drop into chat
    run_chat_ui(kill_on_exit=True)


def cmd_chat(args):
    """Open chat interface."""
    ensure_chat_log()
    print("Opening chat interface...")
    print("Ctrl-D to exit" + (" and kill session" if not args.no_kill else "") + ", Ctrl-C to just exit")
    print()
    run_chat_ui(kill_on_exit=not args.no_kill)


def cmd_attach(args):
    """Attach to tmux session."""
    if not tmux_session_exists():
        print(f"No tmux session '{TMUX_SESSION}' found.", file=sys.stderr)
        print("Use 'start' command to create one.", file=sys.stderr)
        sys.exit(1)
    os.execvp("tmux", ["tmux", "attach", "-t", TMUX_SESSION])


def tail_log(stop_event: Event, new_lines: list):
    """Background thread to tail the chat log."""
    try:
        with open(CHAT_LOG, "r") as f:
            f.seek(0, 2)  # End of file
            while not stop_event.is_set():
                line = f.readline()
                if line:
                    new_lines.append(line.rstrip('\n'))
                else:
                    time.sleep(0.1)
    except Exception:
        pass


def load_recent_history(max_lines: int = 100) -> list:
    """Load recent chat history."""
    try:
        with open(CHAT_LOG, "r") as f:
            lines = f.readlines()
            return [line.rstrip('\n') for line in lines[-max_lines:]]
    except Exception:
        return []


def run_chat_ui(kill_on_exit: bool = True):
    """Run the ncurses chat UI."""
    try:
        curses.wrapper(lambda stdscr: _chat_ui_main(stdscr, kill_on_exit))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _chat_ui_main(stdscr, kill_on_exit: bool):
    """Main chat UI loop."""
    curses.curs_set(1)
    stdscr.clear()
    curses.start_color()
    curses.use_default_colors()

    # Color pairs
    curses.init_pair(1, curses.COLOR_CYAN, -1)    # agents
    curses.init_pair(2, curses.COLOR_GREEN, -1)   # human
    curses.init_pair(3, curses.COLOR_YELLOW, -1)  # system
    curses.init_pair(4, curses.COLOR_WHITE, -1)   # default
    curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_WHITE)  # status bar

    ensure_chat_log()

    height, width = stdscr.getmaxyx()
    msg_height = height - 4  # Reserve extra line for status bar

    # Windows
    msg_win = curses.newwin(msg_height, width, 0, 0)
    msg_win.scrollok(True)

    status_win = curses.newwin(1, width, msg_height, 0)

    sep_win = curses.newwin(1, width, msg_height + 1, 0)
    sep_win.addstr(0, 0, "─" * (width - 1), curses.A_DIM)
    sep_win.refresh()

    input_win = curses.newwin(2, width, msg_height + 2, 0)
    input_win.keypad(True)
    input_win.nodelay(False)

    # Status tracking
    last_status_update = 0
    status_update_interval = 5.0  # seconds

    # State
    messages = load_recent_history()
    new_lines = []
    input_buffer = ""
    cursor_pos = 0

    # Start tail thread
    stop_event = Event()
    tail_thread = Thread(target=tail_log, args=(stop_event, new_lines), daemon=True)
    tail_thread.start()

    def get_color(user: str) -> int:
        if user == "human":
            return curses.color_pair(2)
        elif user == "system":
            return curses.color_pair(3)
        elif user.startswith("agent"):
            return curses.color_pair(1)
        return curses.color_pair(4)

    def parse_msg(line: str):
        try:
            if line.startswith("[") and "] [" in line:
                ts_end = line.index("]")
                timestamp = line[1:ts_end]
                rest = line[ts_end + 2:]
                if rest.startswith("[") and "] " in rest:
                    user_end = rest.index("]")
                    user = rest[1:user_end]
                    message = rest[user_end + 2:]
                    return (timestamp, user, message)
        except (ValueError, IndexError):
            pass
        return (None, None, line)

    def wrap_text(text: str, max_width: int) -> list:
        """Wrap text to fit within max_width."""
        return textwrap.wrap(text, max_width, break_long_words=True, break_on_hyphens=False) or [text]

    def render_messages():
        msg_win.erase()  # erase instead of clear to avoid flash

        # Pre-process messages to handle wrapping
        display_lines = []
        for line in messages:
            ts, user, msg = parse_msg(line)
            if ts and user:
                prefix = f"[{ts}] [{user}] "
                prefix_len = len(prefix)
                # Wrap the message portion
                msg_width = width - prefix_len - 1
                if msg_width < 20:
                    msg_width = width - 1  # Fall back to full width

                wrapped = wrap_text(msg, msg_width)
                for i, wrapped_line in enumerate(wrapped):
                    if i == 0:
                        display_lines.append((ts, user, wrapped_line, False))
                    else:
                        # Continuation lines get indented
                        display_lines.append((ts, user, wrapped_line, True))
            else:
                # Non-chat lines, simple wrap
                wrapped = wrap_text(line, width - 1)
                for w in wrapped:
                    display_lines.append((None, None, w, False))

        # Show the most recent lines that fit
        visible = display_lines[-(msg_height - 1):]

        for i, item in enumerate(visible):
            if i >= msg_height - 1:
                break
            ts, user, msg, is_continuation = item
            try:
                if ts and user:
                    if is_continuation:
                        # Indent continuation lines to align with message
                        indent = len(f"[{ts}] [{user}] ")
                        msg_win.addstr(i, 0, " " * indent)
                        msg_win.addstr(msg[:width - indent - 1])
                    else:
                        msg_win.addstr(i, 0, f"[{ts}] ", curses.A_DIM)
                        msg_win.addstr(f"[{user}]", get_color(user) | curses.A_BOLD)
                        msg_win.addstr(f" {msg[:width - len(ts) - len(user) - 6]}")
                else:
                    msg_win.addstr(i, 0, msg[:width - 1])
            except curses.error:
                pass
        msg_win.noutrefresh()  # noutrefresh to batch updates

    def render_input():
        nonlocal cursor_pos
        input_win.erase()  # erase instead of clear
        prompt = f"[{USER}]> "
        input_win.addstr(0, 0, prompt, curses.color_pair(2) | curses.A_BOLD)

        visible_width = width - len(prompt) - 1
        visible_start = max(0, cursor_pos - visible_width + 1) if cursor_pos >= visible_width else 0
        visible_text = input_buffer[visible_start:visible_start + visible_width]
        input_win.addstr(visible_text)

        cursor_x = len(prompt) + cursor_pos - visible_start
        try:
            input_win.move(0, cursor_x)
        except curses.error:
            pass
        input_win.noutrefresh()
        curses.doupdate()  # single screen update

    def render_status():
        nonlocal last_status_update
        status_win.erase()
        status_win.bkgd(' ', curses.color_pair(5))

        status_text = f"{TMUX_SESSION} │ {time.strftime('%H:%M:%S')}"
        help_text = "^D:exit ^L:clear"

        try:
            status_win.addstr(0, 1, status_text[:width - len(help_text) - 4], curses.color_pair(5))
            status_win.addstr(0, width - len(help_text) - 2, help_text, curses.color_pair(5) | curses.A_DIM)
        except curses.error:
            pass

        status_win.noutrefresh()
        last_status_update = time.time()

    # Initial render
    render_messages()
    render_status()
    render_input()

    # Help message
    exit_msg = "Ctrl-D to exit" + (" and kill session" if kill_on_exit else "") + ", Ctrl-C to just exit"
    messages.append(f"[{time.strftime('%H:%M:%S')}] [system] Chat UI started. {exit_msg}")
    render_messages()

    try:
        while True:
            # Update status bar periodically
            if time.time() - last_status_update > status_update_interval:
                render_status()
                render_input()  # Restore cursor

            # Check for new messages
            if new_lines:
                while new_lines:
                    messages.append(new_lines.pop(0))
                render_messages()
                render_input()

            input_win.timeout(100)
            try:
                ch = input_win.getch()
            except curses.error:
                continue

            if ch == -1:
                continue
            elif ch == 4:  # Ctrl-D
                stop_event.set()
                if kill_on_exit:
                    kill_tmux_session()
                break
            elif ch == 3:  # Ctrl-C
                stop_event.set()
                break
            elif ch in (curses.KEY_ENTER, 10, 13):
                if input_buffer.strip():
                    log_message(USER, input_buffer)
                input_buffer = ""
                cursor_pos = 0
                render_input()
            elif ch == curses.KEY_BACKSPACE or ch == 127:
                if cursor_pos > 0:
                    input_buffer = input_buffer[:cursor_pos - 1] + input_buffer[cursor_pos:]
                    cursor_pos -= 1
                    render_input()
            elif ch == curses.KEY_DC:
                if cursor_pos < len(input_buffer):
                    input_buffer = input_buffer[:cursor_pos] + input_buffer[cursor_pos + 1:]
                    render_input()
            elif ch == curses.KEY_LEFT:
                if cursor_pos > 0:
                    cursor_pos -= 1
                    render_input()
            elif ch == curses.KEY_RIGHT:
                if cursor_pos < len(input_buffer):
                    cursor_pos += 1
                    render_input()
            elif ch in (curses.KEY_HOME, 1):  # Home or Ctrl-A
                cursor_pos = 0
                render_input()
            elif ch in (curses.KEY_END, 5):  # End or Ctrl-E
                cursor_pos = len(input_buffer)
                render_input()
            elif ch == 21:  # Ctrl-U
                input_buffer = ""
                cursor_pos = 0
                render_input()
            elif ch == 12:  # Ctrl-L
                stdscr.clear()
                stdscr.refresh()
                sep_win.addstr(0, 0, "─" * (width - 1), curses.A_DIM)
                sep_win.refresh()
                render_messages()
                render_input()
            elif 32 <= ch <= 126:
                input_buffer = input_buffer[:cursor_pos] + chr(ch) + input_buffer[cursor_pos:]
                cursor_pos += 1
                render_input()
            elif ch == curses.KEY_RESIZE:
                height, width = stdscr.getmaxyx()
                msg_height = height - 4
                msg_win.resize(msg_height, width)
                status_win.mvwin(msg_height, 0)
                status_win.resize(1, width)
                sep_win.mvwin(msg_height + 1, 0)
                sep_win.resize(1, width)
                sep_win.clear()
                sep_win.addstr(0, 0, "─" * (width - 1), curses.A_DIM)
                sep_win.refresh()
                input_win.mvwin(msg_height + 2, 0)
                input_win.resize(2, width)
                render_messages()
                render_status()
                render_input()

    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()


def main():
    parser = argparse.ArgumentParser(
        description="Multi-agent collaboration tool for Claude Code",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s start 3 "Implement feature X"
  %(prog)s start 2                          # No initial prompt
  %(prog)s chat --no-kill
  %(prog)s attach
"""
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # start command
    start_parser = subparsers.add_parser("start", help="Start agents and enter chat")
    start_parser.add_argument("num_agents", type=int, help="Number of agents to spawn")
    start_parser.add_argument("prompt", nargs="?", default="", help="Optional prompt for all agents")
    start_parser.set_defaults(func=cmd_start)

    # chat command
    chat_parser = subparsers.add_parser("chat", help="Open chat interface")
    chat_parser.add_argument("--no-kill", action="store_true",
                            help="Don't kill tmux session on Ctrl-D")
    chat_parser.set_defaults(func=cmd_chat)

    # attach command
    attach_parser = subparsers.add_parser("attach", help="Attach to tmux session")
    attach_parser.set_defaults(func=cmd_attach)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
