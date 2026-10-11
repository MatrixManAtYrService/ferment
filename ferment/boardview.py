"""A fermenter's board (ferment.board) in a terminal.

The board is pinned at the bottom of the terminal and the feed prints above
it, so the feed scrolls into ordinary scrollback (search, copy, tmux all
work) while what must stay in view does. Milestones print a line as they
finish, then roll up in the pinned area once their whole group is done.

When the output isn't a terminal, the view prints plain lines instead: the
feed, and milestones as they finish.
"""

from __future__ import annotations

import datetime as dt
import threading
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

from ferment.board import FeedEntry, Milestone, Service, Snapshot
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

    def __init__(self, console: Console, title: str, me: str, forwards: dict[str, tuple[int, int]] | None = None):
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

    def live(self) -> AbstractContextManager:
        """Pin the board while the block runs (a no-op when not a terminal).
        Anything printed meanwhile, from any thread, lands above it."""
        if self.plain:
            return nullcontext()
        return Live(self, console=self.console, refresh_per_second=4, redirect_stdout=True, redirect_stderr=True)

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
        return Group(*parts)

    def _header(self, snap: Snapshot | None, conn: str) -> Text:
        t = Text(self.title, "bold")
        if snap is not None and snap.fake_offset is not None:
            fake = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=snap.fake_offset)
            t.append("  fake ", "dim").append(fake.strftime("%Y-%m-%d %H:%M:%S %a"), "bold magenta")
        t.append("  real ", "dim").append(dt.datetime.now().strftime("%H:%M:%S"))
        badge = {
            "connecting": ("  connecting…", "yellow"),
            "lost": ("  lost the fermenter; reconnecting…", "bold red"),
            "legacy": ("  (this fermenter's server predates boards: feed only)", "dim"),
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
            if m.progress is None:
                bar, count = ProgressBar(total=None, width=24, pulse=True), f"{m.lines} lines" if m.lines else ""
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
