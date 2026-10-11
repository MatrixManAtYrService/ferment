"""A fermenter board with nothing behind it: replays a scripted run.

For building and testing renderers (the attach view, the web page) without a
VM. The run is a from-scratch life of a made-up stack: create, people attach,
load data, change the clock, deploy a new build (which fails once).

    python -m ferment.fakeboard --port 43099 --speed 2 --loop
    curl -N localhost:43099/board/stream
"""

from __future__ import annotations

import datetime as dt
import random
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Annotated

import typer
import uvicorn
from fastapi import FastAPI

from ferment.board import Board, Milestone
from ferment.server.board import board_router

Pause = Callable[[float], None]


class Stopped(Exception):
    """The server is shutting down mid-run."""


class Run:
    """Drives a board through the script. `pause(seconds)` paces it; the
    tests pass one that doesn't sleep."""

    TICK = 0.25

    def __init__(self, board: Board, pause: Pause, seed: int = 0):
        self.board = board
        self.pause = pause
        self.rand = random.Random(seed)
        self.attached: list[str] = []

    def say(self, event: str, message: str, who: str | None = None) -> None:
        fake = None
        if self.board.fake_offset is not None:
            at = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=self.board.fake_offset)
            fake = at.isoformat(timespec="seconds")
        self.board.post(event, message, who=who, fake_time=fake)

    def steps(self, seconds: float, *mids: str, fail: str | None = None) -> None:
        """Run milestones side by side for about `seconds`, each writing
        roughly its expected lines (give or take a fifth: estimates are
        estimates). `fail` stops partway and fails."""
        b = self.board
        todo = {}
        for mid in mids:
            m = b.milestones[mid]
            todo[mid] = int((m.expected or 0) * self.rand.uniform(0.8, 1.2))
            b.start(mid)
        ticks = max(1, int(seconds / self.TICK))
        for i in range(1, ticks + 1):
            self.pause(self.TICK)
            for mid, total in todo.items():
                m = b.milestones[mid]
                if m.state != "running":
                    continue
                if m.source:
                    b.count(m.source, total * i // ticks - m.lines)
                if mid == fail and i >= ticks // 2:
                    b.finish(mid, ok=False, detail="exited with status 1 (see its log)")
                elif i == ticks:
                    b.finish(mid)

    def attach(self, who: str, client: str) -> None:
        self.attached.append(who)
        self.board.set_fact("attached", ", ".join(self.attached), "attached")
        self.say("attached", f"{who} attached from {client}", who)

    def services(self, state: str, *names: str) -> None:
        for name in names:
            self.board.set_service(name, state=state)

    def clock(self, at: str) -> None:
        target = dt.datetime.fromisoformat(at)
        self.board.set_clock((target - dt.datetime.now(dt.UTC)).total_seconds())

    def __call__(self) -> None:
        b, say, pause = self.board, self.say, self.pause

        # -- create ----------------------------------------------------------------
        b.set_fact("data", "none yet", "data date")
        b.set_service("fermenter", label="fermenter", port=8400, state="starting")
        b.set_service("grafana", label="grafana", port=3000, state="down")
        b.set_service("scheduler", label="scheduler", port=8080, state="down")
        b.set_service("database", label="database", port=3306, scheme="mysql", state="down")
        create = "create"
        b.plan(
            [
                Milestone(id="vm", label="VM created", group=create),
                Milestone(id="install", label="fermenter installed", group=create, source="bootstrap", expected=600),
                Milestone(id="database", label="database ready", group=create, source="database", expected=120),
                Milestone(id="migrate", label="migrations applied", group=create, source="migrate", expected=300),
                Milestone(id="api", label="api ready", group=create, source="api", expected=400),
                Milestone(id="worker", label="worker ready", group=create, source="worker", expected=250),
                Milestone(id="scheduler", label="scheduler ready", group=create, source="scheduler", expected=200),
            ]
        )
        self.steps(3, "vm")
        self.steps(8, "install")
        self.services("up", "fermenter")
        self.services("starting", "database")
        self.steps(3, "database")
        self.services("up", "database")
        self.steps(4, "migrate")
        self.services("starting", "scheduler", "grafana")
        self.steps(6, "api", "worker", "scheduler")
        self.services("up", "scheduler", "grafana")
        self.clock("2026-10-02T00:00:00+00:00")
        say("ready", "fermenter demo is ready; clock is 2026-10-02 00:00:00")

        # -- people arrive -----------------------------------------------------------
        pause(1)
        self.attach("alice@laptop", "laptop")
        pause(2)
        self.attach("bob@vpnbox", "vpnbox (relaying nexus, fetching from git)")

        # -- data ----------------------------------------------------------------------
        load = "load data 2026-10-02"
        b.plan(
            [
                Milestone(id="fetch", label="snapshot fetched", group=load, source="fetch", expected=80),
                Milestone(id="load", label="snapshot loaded", group=load, source="loader", expected=2000),
            ]
        )
        say("load", "alice@laptop is loading data for 2026-10-02", "alice@laptop")
        self.steps(3, "fetch")
        self.steps(8, "load")
        b.set_fact("data", "2026-10-02", "data date")
        b.set_fact("checkpoint", "loaded (c1)", "last checkpoint")
        say("checkpoint", "alice@laptop checkpointed loaded: loaded 2026-10-02 data", "alice@laptop")

        # -- the clock moves -------------------------------------------------------------
        pause(2)
        tick = "clock to 2026-10-03"
        b.plan(
            [
                Milestone(id="stop", label="workload stopped", group=tick),
                Milestone(id="cp2", label="checkpointed", group=tick),
                Milestone(id="restart", label="workload restarted", group=tick, source="api", expected=150),
            ]
        )
        say("clock_change", "bob@vpnbox is moving the clock to 2026-10-03 (workload restarting)", "bob@vpnbox")
        self.steps(1, "stop")
        self.services("down", "database", "scheduler")
        self.steps(1, "cp2")
        b.set_fact("checkpoint", "c2 (clock change)", "last checkpoint")
        self.clock("2026-10-03T00:00:00+00:00")
        self.services("starting", "database", "scheduler")
        self.steps(4, "restart")
        self.services("up", "database", "scheduler")
        say("clock_changed", "clock is now 2026-10-03 00:00:00 (set by bob@vpnbox)", "bob@vpnbox")

        # -- a deploy that fails, then works -----------------------------------------------
        pause(2)
        dep = "deploy worker @ 3f9c2e1"
        b.plan(
            [
                Milestone(id="build", label="worker built", group=dep, source="build", expected=1500),
                Milestone(id="worker2", label="worker ready", group=dep, source="worker", expected=250),
            ]
        )
        say("deploy", "alice@laptop is deploying worker @ 3f9c2e1", "alice@laptop")
        self.steps(8, "build")
        self.steps(4, "worker2", fail="worker2")
        pause(2)
        b.plan([Milestone(id="worker3", label="worker ready (retry)", group=dep, source="worker", expected=250)])
        say("deploy", "alice@laptop is retrying worker @ 3f9c2e1", "alice@laptop")
        self.steps(4, "worker3")
        say("deployed", "worker @ 3f9c2e1 deployed by alice@laptop; workload ready", "alice@laptop")

        pause(2)
        self.attached.remove("bob@vpnbox")
        b.set_fact("attached", ", ".join(self.attached), "attached")
        say("detached", "bob@vpnbox detached", "bob@vpnbox")


def create_app(speed: float = 1.0, loop: bool = False) -> FastAPI:
    board = Board()
    stop = threading.Event()

    def pause(seconds: float) -> None:
        if stop.wait(seconds / speed):
            raise Stopped

    def script() -> None:
        try:
            while True:
                Run(board, pause)()
                if not loop:
                    return
                pause(5)
                board.reset()
                board.post("reset", "demo run starting over")
        except Stopped:
            pass

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        threading.Thread(target=script, daemon=True).start()
        yield
        stop.set()

    app = FastAPI(title="fake fermenter board", lifespan=lifespan)

    @app.get("/health")
    def health():
        return {"healthy": True}

    app.include_router(board_router(board))
    return app


def main(
    port: Annotated[int, typer.Option(help="Port to serve on (localhost)")] = 43099,
    speed: Annotated[float, typer.Option(help="Run this many times faster than the script")] = 1.0,
    loop: Annotated[bool, typer.Option(help="Start over when the run ends")] = False,
) -> None:
    """Serve a fake fermenter board that replays a scripted run."""
    uvicorn.run(create_app(speed, loop), host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    typer.run(main)
