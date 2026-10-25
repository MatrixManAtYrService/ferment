"""A fermenter's board (ferment.board) in a terminal.

The board is pinned at the bottom of the terminal and the feed prints above
it, so the feed scrolls into ordinary scrollback (search, copy, tmux all
work) while what must stay in view does. Milestones print a line as they
finish, then roll up in the pinned area once their whole group is done.

What's waiting on someone (the board's `waiting`) is a queue in the pinned
area, with the chosen item's details beside it. In a terminal, ↑/↓ (or k/j)
choose and enter acts: for an action that's this machine's to take and that
`resolve` knows how to do, the board steps aside, the command runs in the
terminal (so a browser can open, prompts work), and the board comes back.
The command comes from `resolve`, this client's own map of action keys, never
from the fermenter.

When the output isn't a terminal, the view prints plain lines instead: the
feed, and milestones as they finish.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import select
import shlex
import subprocess
import sys
import threading
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, nullcontext
from typing import Annotated

import httpx
import typer
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.progress_bar import ProgressBar
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from ferment.board import FeedEntry, Milestone, Service, Snapshot, WaitItem
from ferment.broadcasts import BroadcastWatcher
from ferment.server.board import KEEPALIVE

_SERVICE_DOT = {"up": ("●", "green"), "starting": ("◐", "yellow"), "down": ("○", "red"), "unknown": ("?", "dim")}


def _local_time(iso: str) -> str:
    try:
        return dt.datetime.fromisoformat(iso).astimezone().strftime("%H:%M:%S")
    except ValueError:
        return iso[11:19]


class BoardView:
    """Draws the board. `forwards` (label -> (local port, VM port)) are the
    ports this client forwards: services show addresses that work here, and
    forwards no service covers are pinned on their own."""

    def __init__(
        self,
        console: Console,
        title: str,
        me: str,
        forwards: dict[str, tuple[int, int]] | None = None,
        resolve: Callable[[str], list[str] | None] | None = None,
        on_acted: Callable[[str, int], None] | None = None,
    ):
        """`resolve(key)`: the command this client runs for an action key, or
        None; `on_acted(key, exit code)`: called after it ran."""
        self.console = console
        self.title = title
        self.me = me
        self.forwards = forwards or {}
        self.ports = {vm: local for local, vm in self.forwards.values()}
        self.plain = not console.is_terminal
        self._lock = threading.Lock()
        self.snap: Snapshot | None = None
        self.conn = "connecting"  # connecting, up, lost, legacy (a server without a board)
        self.local: dict[str, tuple[str, str]] = {}  # what only this client knows: key -> (text, style)
        self._spinners: dict[str, Spinner] = {}
        self.resolve = resolve or (lambda key: None)
        self.on_acted = on_acted
        self.chosen: str | None = None  # the waiting item's id
        self.interactive = False  # keys are being read
        self._live: Live | None = None
        self._keys: _KeyReader | None = None

    def live(self) -> AbstractContextManager:
        """Pin the board while the block runs (a no-op when not a terminal).
        Anything printed meanwhile, from any thread, lands above it. With a
        terminal on stdin too, keys choose and act on what's waiting."""
        if self.plain:
            return nullcontext()
        return self._pinned()

    @contextlib.contextmanager
    def _pinned(self) -> Iterator[None]:
        self._live = Live(self, console=self.console, refresh_per_second=4, redirect_stdout=True, redirect_stderr=True)
        with self._live:
            if sys.stdin.isatty():
                self._keys = _KeyReader(self.key)
                self._keys.start()
                self.interactive = True
            try:
                yield
            finally:
                if self._keys is not None:
                    self._keys.close()
                self.interactive = False

    # -- keys ---------------------------------------------------------------------------

    def _waiting(self) -> list[WaitItem]:
        with self._lock:
            return list(self.snap.waiting) if self.snap is not None else []

    def _choice(self, items: list[WaitItem]) -> WaitItem | None:
        if not items:
            return None
        return next((w for w in items if w.id == self.chosen), items[0])

    def key(self, k: str) -> None:
        items = self._waiting()
        cur = self._choice(items)
        if cur is None:
            return
        i = items.index(cur)
        if k in ("up", "k"):
            self.chosen = items[(i - 1) % len(items)].id
        elif k in ("down", "j"):
            self.chosen = items[(i + 1) % len(items)].id
        elif k == "enter":
            self.act(cur)

    def runnable(self, w: WaitItem) -> list[str] | None:
        if w.kind != "action" or w.who != self.me or w.key is None:
            return None
        return self.resolve(w.key)

    def act(self, w: WaitItem) -> None:
        argv = self.runnable(w)
        if argv is None:
            return
        live, keys = self._live, self._keys
        if keys is not None:
            keys.pause()
        if live is not None:
            live.stop()
        self.console.print(Text.assemble(("running: ", "bold"), shlex.join(argv)))
        try:
            code = subprocess.run(argv).returncode
        except OSError as e:
            self.console.print(Text(f"couldn't run it: {e}", "red"))
            code = 127
        self.console.print(Text("done" if code == 0 else f"exit {code}", "green" if code == 0 else "red"))
        if live is not None:
            live.start()
        if keys is not None:
            keys.resume()
        if self.on_acted is not None:
            self.on_acted(w.key or "", code)

    # -- input --------------------------------------------------------------------------

    def connection(self, state: str) -> None:
        with self._lock:
            self.conn = state

    def set_local(self, key: str, text: str, style: str = "") -> None:
        """Pin a line only this client knows (a relay's state, say); a change is also announced in the feed."""
        with self._lock:
            changed = self.local.get(key, ("", ""))[0] != text
            self.local[key] = (text, style)
        if changed:
            self.say(text, style)

    def update(self, snap: Snapshot) -> None:
        with self._lock:
            old, self.snap = self.snap, snap
            if self.conn != "legacy":
                self.conn = "up"
        # Milestones first: what's said about a step comes after the step.
        if old is not None:  # someone attaching late doesn't need the history replayed
            before = {m.id: m.state for m in old.milestones}
            for m in snap.milestones:
                if m.state in ("done", "failed") and before.get(m.id) != m.state:
                    self.say(*self._finished(m))
        for entry in snap.feed:
            self.entry(entry)

    def entry(self, e: FeedEntry) -> None:
        style = "red" if e.event.endswith("failed") else ""
        if e.who == self.me:
            style += " dim"
        self.say(e.message, style.strip(), at=e.real_time)

    def say(self, text: str, style: str = "", at: str | None = None) -> None:
        stamp = _local_time(at) if at else dt.datetime.now().strftime("%H:%M:%S")
        line = Text.assemble((f"[{stamp}] ", "dim"), (text, style))
        if self.plain:
            print(line.plain, file=self.console.file, flush=True)
        else:
            self.console.print(line, highlight=False)

    @staticmethod
    def _finished(m: Milestone) -> tuple[str, str]:
        if m.state == "done":
            return f"✔ {m.label}", "green"
        return f"✘ {m.label}" + (f": {m.detail}" if m.detail else ""), "red"

    # -- drawing -----------------------------------------------------------------------

    def __rich__(self) -> RenderableType:
        with self._lock:
            snap, conn, local = self.snap, self.conn, list(self.local.values())
        parts: list[RenderableType] = [self._header(snap, conn)]
        services = snap.services if snap is not None else []
        covered = {s.port for s in services}
        bare = [(label, local) for label, (local, vm) in self.forwards.items() if vm not in covered]
        if services or bare:
            parts.append(self._services(services, bare))
        if snap is not None and snap.facts:
            facts = Text("  ").join(Text.assemble((f.label + " ", "dim"), f.value) for f in snap.facts)
            facts.pad_left(2)
            parts.append(facts)
        parts += [Text("  " + text, style) for text, style in local]
        if snap is not None and snap.milestones:
            parts.append(self._milestones(snap.milestones))
        if snap is not None and snap.waiting:
            parts.append(self._queue(snap.waiting))
        return Group(*parts)

    def _queue(self, items: list[WaitItem]) -> RenderableType:
        cur = self._choice(items)
        left = Table.grid()
        left.add_column(no_wrap=True, max_width=56)
        for w in items:
            mine = self.runnable(w) is not None
            mark = "▶ " if w is cur and self.interactive else "  "
            style = "bold yellow" if mine else ("yellow" if w.kind == "action" else "")
            left.add_row(Text(mark + w.title, style, overflow="ellipsis"))
        right = Text()
        if cur is not None:
            right.append(cur.title + "\n", "bold")
            if cur.detail:
                right.append(cur.detail + "\n")
            if cur.since:
                right.append(f"since {_local_time(cur.since)}\n", "dim")
            argv = self.runnable(cur)
            if argv is not None:
                right.append("enter: ", "bold green").append(shlex.join(argv), "green")
            elif cur.kind == "action":
                right.append(f"{cur.who}'s to do, on their machine", "dim")
        grid = Table(
            title="waiting", title_justify="left", title_style="bold", show_header=False, box=None, pad_edge=False
        )
        grid.add_column(ratio=1)
        grid.add_column(ratio=1)
        grid.add_row(left, right)
        if self.interactive:
            grid.caption = "↑↓ choose · enter act"
            grid.caption_justify = "left"
            grid.caption_style = "dim"
        return grid

    def _header(self, snap: Snapshot | None, conn: str) -> Text:
        t = Text(self.title, "bold")
        if snap is not None and snap.fake_offset is not None:
            fake = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=snap.fake_offset)
            # Fake dates are set in UTC, so that's how they read; real time is local.
            t.append("  fake ", "dim").append(fake.strftime("%Y-%m-%d %H:%M:%S %a UTC"), "bold magenta")
        t.append("  real ", "dim").append(dt.datetime.now().astimezone().strftime("%H:%M:%S %Z"))
        badge = {
            "connecting": ("  connecting…", "yellow"),
            "lost": ("  lost the fermenter; reconnecting…", "bold red"),
            "legacy": ("  (this fermenter's server has no board: feed only)", "dim"),
        }.get(conn)
        if badge:
            t.append(*badge)
        return t

    def _address(self, s: Service) -> Text:
        if s.port is None:
            return Text("")
        local = self.ports.get(s.port)
        if local is None:
            return Text(f"not forwarded (VM :{s.port})", "dim")
        if s.scheme == "http":
            return Text(f"http://localhost:{local}{s.path if s.path != '/' else ''}", "cyan")
        return Text(f"{s.scheme}://127.0.0.1:{local}", "cyan")

    def _services(self, services: list[Service], bare: list[tuple[str, int]]) -> Table:
        grid = Table.grid(padding=(0, 1))
        grid.add_column(width=1)
        for _ in range(3):
            grid.add_column()
        for s in services:
            dot, style = _SERVICE_DOT.get(s.state, ("?", "dim"))
            grid.add_row(Text(dot, style), Text(s.label), self._address(s), Text(s.detail or "", "dim"))
        for label, local in bare:  # forwarded, but the board doesn't say what's there
            grid.add_row(Text("→", "dim"), Text(label), Text(f"http://localhost:{local}", "cyan"), Text(""))
        return grid

    def _milestones(self, milestones: list[Milestone]) -> Group:
        groups: dict[str, list[Milestone]] = {}
        for m in milestones:
            groups.setdefault(m.group, []).append(m)
        rolled, open_ = Text("  "), []
        for name, ms in groups.items():
            if all(m.state in ("done", "failed") for m in ms):
                done = sum(m.state == "done" for m in ms)
                failed = len(ms) - done
                if len(rolled) > 2:
                    rolled.append("   ")
                rolled.append(f"✔ {name or 'done'} ({done})", "green" if not failed else "yellow")
                if failed:
                    rolled.append(f" ✘{failed}", "red")
            else:
                open_.append((name, ms))
        parts: list[RenderableType] = [rolled] if len(rolled) > 2 else []
        for name, ms in open_:
            parts.append(self._open_group(name, ms))
        return Group(*parts)

    def _open_group(self, name: str, ms: list[Milestone]) -> Group:
        head = Text("  ")
        head.append(name or "steps", "bold")
        for m in ms:
            if m.state == "done":
                head.append(f"  ✔ {m.label}", "green")
            elif m.state == "failed":
                head.append(f"  ✘ {m.label}", "red")
        rows: list[RenderableType] = [head]
        running = Table.grid(padding=(0, 1))
        for _ in range(5):
            running.add_column()
        for m in ms:
            if m.state != "running":
                continue
            spin = self._spinners.setdefault(m.id, Spinner("dots", style="yellow"))
            if m.total:
                bar = ProgressBar(total=100, completed=(m.progress or 0) * 100, width=24)
                count = f"{m.done or 0}/{m.total}" + (f"  {m.detail}" if m.detail else "")
            elif m.progress is None:
                bar, count = ProgressBar(total=None, width=24, pulse=True), f"{m.lines} lines" if m.lines else ""
                if m.detail:
                    count = m.detail
            else:
                bar = ProgressBar(total=100, completed=m.progress * 100, width=24)
                count = f"{m.progress:>4.0%}  {m.lines}/~{m.expected} lines"
            running.add_row(Text("   "), spin, Text(m.label), bar, Text(count, "dim"))
        if running.row_count:
            rows.append(running)
        pending = [m.label for m in ms if m.state == "pending"]
        if pending:
            rows.append(Text("    next: " + ", ".join(pending), "dim"))
        return Group(*rows)


_KEYS = {b"\x1b[A": "up", b"\x1bOA": "up", b"\x1b[B": "down", b"\x1bOB": "down", b"\r": "enter", b"\n": "enter",
         b"k": "k", b"j": "j"}  # fmt: skip


class _KeyReader(threading.Thread):
    """Keys from a terminal on stdin, in cbreak mode (ctrl-c still works),
    handed to `on_key`. Paused while a command has the terminal."""

    def __init__(self, on_key: Callable[[str], None]):
        super().__init__(daemon=True)
        import termios
        import tty

        self.on_key = on_key
        self.fd = sys.stdin.fileno()
        self._termios = termios
        self._saved = termios.tcgetattr(self.fd)
        tty.setcbreak(self.fd)
        self._stop = threading.Event()
        self._paused = threading.Event()
        self._idle = threading.Event()

    def run(self) -> None:
        while not self._stop.is_set():
            if self._paused.is_set():
                self._idle.set()
                self._stop.wait(0.1)
                continue
            self._idle.clear()
            ready, _, _ = select.select([self.fd], [], [], 0.2)
            if not ready or self._paused.is_set():
                continue
            data = os.read(self.fd, 16)
            k = _KEYS.get(data)
            if k is not None:
                try:
                    self.on_key(k)
                except Exception as e:  # a key must never take the view down
                    print(f"key {k}: {e}", file=sys.stderr)

    def pause(self) -> None:
        """Give the terminal back as it was (for a command), and stop reading."""
        self._paused.set()
        if threading.current_thread() is not self:
            self._idle.wait(1)
        self._termios.tcsetattr(self.fd, self._termios.TCSADRAIN, self._saved)

    def resume(self) -> None:
        import tty

        tty.setcbreak(self.fd)
        self._paused.clear()

    def close(self) -> None:
        self._stop.set()
        self._termios.tcsetattr(self.fd, self._termios.TCSADRAIN, self._saved)


class Follower(threading.Thread):
    """Keeps a view current from a fermenter server: its board stream, or, on
    a server that predates boards, its broadcasts. Reconnects on its own and
    picks the feed up where it left off."""

    RETRY = 2.0

    def __init__(self, server_url: str, view: BoardView):
        super().__init__(daemon=True)
        self.url = server_url
        self.view = view
        self.stop = threading.Event()
        self.after: int | None = None

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                if self._stream() == "legacy":
                    self._broadcasts()
                    return
            except (httpx.HTTPError, ValueError):
                pass  # tunnel not up yet, server restarting, ...
            if not self.stop.is_set():
                self.view.connection("lost" if self.view.snap is not None else "connecting")
                self.stop.wait(self.RETRY)

    def _stream(self) -> str | None:
        params = {} if self.after is None else {"after": self.after}
        timeout = httpx.Timeout(5.0, read=KEEPALIVE * 3)
        with httpx.stream("GET", f"{self.url}/board/stream", params=params, timeout=timeout) as r:
            if r.status_code == 404:
                return "legacy"
            r.raise_for_status()
            first, data = True, []
            for line in r.iter_lines():
                if self.stop.is_set():
                    return None
                if line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif not line and data:
                    snap = Snapshot.model_validate_json("\n".join(data))
                    data = []
                    if first and self.after is not None and snap.feed_seq < self.after:
                        # The server restarted and its feed started over: take all of the new one.
                        whole = Snapshot.model_validate_json(httpx.get(f"{self.url}/board", timeout=5).content)
                        snap = snap.model_copy(update={"feed": whole.feed})
                    first = False
                    self.after = snap.feed_seq
                    self.view.update(snap)
        return None

    def _broadcasts(self) -> None:
        self.view.connection("legacy")
        watcher = BroadcastWatcher(self.url, self.view.me)
        while not self.stop.wait(watcher.POLL):
            try:
                entries = watcher.poll()
            except (httpx.HTTPError, ValueError, KeyError):
                continue
            for e in entries:
                self.view.entry(
                    FeedEntry(
                        seq=0,
                        real_time=e.get("real_time", ""),
                        event=e.get("event", ""),
                        message=e.get("message", ""),
                        who=e.get("who"),
                    )
                )


def main(
    server_url: Annotated[str, typer.Argument(help="Fermenter server (or fake board) URL")] = "http://localhost:43099",
    title: Annotated[str, typer.Option(help="What to call it in the header")] = "fermenter",
    forward: Annotated[
        list[str] | None, typer.Option(help="Pretend VM port VM is forwarded to LOCAL: VM:LOCAL (repeatable)")
    ] = None,
) -> None:
    """Watch a fermenter's board (e.g. ferment.fakeboard's) until ctrl-c."""
    from ferment.client import whoami

    forwards = {}
    for f in forward or []:
        vm, local = f.split(":")
        forwards[f":{vm}"] = (int(local), int(vm))
    view = BoardView(Console(stderr=True), title, whoami(), forwards)
    follower = Follower(server_url.rstrip("/"), view)
    follower.start()
    try:
        with view.live():
            follower.join()
    except KeyboardInterrupt:
        follower.stop.set()


if __name__ == "__main__":
    typer.run(main)
