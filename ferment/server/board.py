"""Serving a fermenter's board (ferment.board) to whoever is watching.

GET /board         the board now (`after`: only feed entries past that seq)
GET /board/stream  server-sent events: one `board` event right away, then one
                   per change. Each carries the whole board except the feed,
                   which holds only entries the stream hasn't sent yet, so a
                   client replaces its state and appends to its feed.

A browser's EventSource reconnects on its own; to pick the feed up where it
left off, a client passes the last `feed_seq` it saw as `after`.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import itertools
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable

from fastapi import APIRouter, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ferment import needs
from ferment.board import Board, Milestone, Service, Snapshot, WaitItem
from ferment.server import logs
from ferment.server.clock import real_now
from ferment.sources import repo_host

# How long the stream sits idle before sending a comment, so proxies and ssh
# tunnels see traffic and a gone client is noticed.
KEEPALIVE = 15.0
# At most this often per watcher: a busy board (log lines counting toward a
# milestone) changes far faster than anyone reads, and it all goes over ssh.
MIN_INTERVAL = 0.25


async def events(
    board: Board,
    after: int | None,
    disconnected: Callable[[], Awaitable[bool]],
    keepalive: float = KEEPALIVE,
    min_interval: float = MIN_INTERVAL,
) -> AsyncIterator[str]:
    version: int | None = None
    while not await disconnected():
        # Board.wait blocks a thread; worth one per watcher, of which a fermenter has few.
        if version is not None and await run_in_threadpool(board.wait, version, keepalive) == version:
            yield ": keepalive\n\n"
            continue
        snap = board.snapshot(after)
        version, after = snap.version, snap.feed_seq
        yield f"id: {snap.version}\nevent: board\ndata: {snap.model_dump_json()}\n\n"
        await asyncio.sleep(min_interval)  # changes meanwhile arrive together in the next one


def board_router(board: Board) -> APIRouter:
    router = APIRouter()

    @router.get("/board", response_model=Snapshot)
    def snapshot(after: int | None = None):
        return board.snapshot(after)

    @router.get("/board/stream")
    def stream(request: Request, after: int | None = None):
        return StreamingResponse(
            events(board, after, request.is_disconnected),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    return router


# -- keeping a fermenter's board current ---------------------------------------------

STEP_LABELS = {
    "prepare": "prepared",
    "stop": "workload stopped",
    "checkpoint": "checkpointed",
    "restore": "data restored",
    "clock": "clock set",
    "start": "workload started",
    "ready": "workload ready",
}

_ids = itertools.count(1)


class Steps:
    """One operation (a clock change, a restore, a deploy) as a group of
    milestones run in order. Call it with a step's key as that step finishes
    (TimeController's `step`); the next one starts."""

    def __init__(self, board: Board, group: str, keys: list[str], labels: dict[str, str] | None = None):
        self.board = board
        n = next(_ids)
        labels = {**STEP_LABELS, **(labels or {})}
        self.keys = keys
        self.ids = {k: f"{n}-{k}" for k in keys}
        board.plan([Milestone(id=self.ids[k], label=labels.get(k, k), group=group) for k in keys])
        board.start(self.ids[keys[0]])

    def __call__(self, key: str) -> None:
        if key not in self.ids:
            return
        self.board.finish(self.ids[key])
        nxt = self.keys.index(key) + 1
        if nxt < len(self.keys):
            self.board.start(self.ids[self.keys[nxt]])

    def note(self, key: str, detail: str) -> None:
        self.board.note(self.ids[key], detail)

    def fail(self, detail: str) -> None:
        """The operation stopped: whatever was running failed with `detail`."""
        for mid in self.ids.values():
            if self.board.milestones[mid].state == "running":
                self.board.finish(mid, ok=False, detail=detail)


def _age(since: str) -> str:
    minutes = int((real_now() - dt.datetime.fromisoformat(since)).total_seconds() // 60)
    return f"{minutes}m" if minutes < 120 else f"{minutes // 60}h"


def waiting_for(store, sessions: list[dict]) -> list[WaitItem]:
    """What's held up on someone, for the board: actions an attached person
    has to take, registered needs, and source and data requests nobody
    attached can serve (or whose server left)."""
    served: dict[str, list[str]] = {}
    for s in sessions:
        for cap in needs.offered(s):
            served.setdefault(cap, []).append(s["who"])
    here = {s["who"] for s in sessions}
    out = []
    for a in store.actions():
        detail = a["detail"] + ("" if a["who"] in here else f"\n({a['who']} isn't attached right now)")
        out.append(
            WaitItem(
                id=f"action-{a['id']}", kind="action", title=a["title"], detail=detail.strip(),
                since=a["raised_at"], who=a["who"], key=a["key"],
            )
        )  # fmt: skip
    for n in store.needs():
        title = f"{n['purpose']} needs {needs.describe(n['capability'])} ({_age(n['since'])})"
        detail = f"asked by {n['who']}"
        if by := served.get(n["capability"]):
            title += f"; {', '.join(by)} serves it, not working yet"
            detail += f"; {', '.join(by)} says it serves {n['capability']}, but it isn't working yet"
        out.append(WaitItem(id=f"need-{n['id']}", kind="need", title=title, detail=detail, since=n["since"]))
    for r in store.claimable_source_requests():
        cap = needs.fetch(repo_host(r["repo"]) or "?")
        if cap not in served:
            title = f"{r['name']} @ {r['ref']} needs {needs.describe(cap)} ({_age(r['requested_at'])})"
            out.append(
                WaitItem(
                    id=f"source-{r['id']}", kind="requests", title=title,
                    detail=f"{r['repo']}, asked by {r['requested_by']}", since=r["requested_at"],
                )
            )  # fmt: skip
    unserved: dict[str, list[dict]] = {}
    for r in store.claimable_data_requests():
        if r["capability"] not in served:
            unserved.setdefault(r["capability"], []).append(r)
    for cap, rows in unserved.items():  # a pull asks for every table at once: one line, counted
        title = f"{len(rows)} data requests need {needs.describe(cap)} ({_age(rows[0]['requested_at'])})"
        who = ", ".join(sorted({r["requested_by"] for r in rows}))
        out.append(
            WaitItem(
                id=f"data-{cap}", kind="requests", title=title, detail=f"asked by {who}",
                since=rows[0]["requested_at"],
            )
        )  # fmt: skip
    return out


# -- tasks: long work that reports its own progress ------------------------------------

# A client's task not heard from in this long has lost its client.
TASK_TIMEOUT = 120.0


class Tasks:
    """Long-running work shown as a milestone with counted progress (`done`
    of `total`, plus what it's on now). Clients open one over HTTP and keep
    it current; one whose client stops reporting fails. The server's own
    (a load) don't lapse."""

    def __init__(self, board: Board, now: Callable[[], float] = time.monotonic):
        self.board = board
        self.now = now
        self._lock = threading.Lock()
        self._seen: dict[str, float | None] = {}  # milestone -> last report (None: never lapses)

    def open(self, label: str, total: int | None = None, group: str | None = None, lease: bool = True) -> str:
        mid = f"task-{next(_ids)}"
        self.board.plan([Milestone(id=mid, label=label, group=group or label, total=total, done=0 if total else None)])
        self.board.start(mid)
        with self._lock:
            self._seen[mid] = self.now() if lease else None
        return mid

    def update(self, mid: str, done: int | None = None, total: int | None = None, detail: str | None = None) -> bool:
        with self._lock:
            if mid not in self._seen:
                return False
            if self._seen[mid] is not None:
                self._seen[mid] = self.now()
        if done is not None:
            self.board.set_progress(mid, done, total, detail)
        elif detail is not None:
            self.board.note(mid, detail)
        return True

    def finish(self, mid: str, ok: bool = True, detail: str | None = None) -> bool:
        with self._lock:
            if mid not in self._seen:
                return False
            del self._seen[mid]
        self.board.finish(mid, ok=ok, detail=detail)
        return True

    def lapse(self) -> None:
        cutoff = self.now() - TASK_TIMEOUT
        with self._lock:
            gone = [m for m, t in self._seen.items() if t is not None and t < cutoff]
        for mid in gone:
            self.finish(mid, ok=False, detail="its client stopped reporting")


class NewTask(BaseModel):
    label: str
    total: int | None = None
    group: str | None = None


class TaskUpdate(BaseModel):
    done: int | None = None
    total: int | None = None
    detail: str | None = None


class TaskDone(BaseModel):
    ok: bool = True
    detail: str | None = None


def tasks_router(tasks: Tasks) -> APIRouter:
    router = APIRouter()

    @router.post("/tasks")
    def open_task(req: NewTask):
        return {"id": tasks.open(req.label, req.total, req.group)}

    @router.patch("/tasks/{task_id}")
    def update_task(task_id: str, req: TaskUpdate):
        if not tasks.update(task_id, req.done, req.total, req.detail):
            raise HTTPException(404, "no running task with that id")
        return {"ok": True}

    @router.post("/tasks/{task_id}/finish")
    def finish_task(task_id: str, req: TaskDone):
        if not tasks.finish(task_id, req.ok, req.detail):
            raise HTTPException(404, "no running task with that id")
        return {"ok": True}

    return router


class Publisher(threading.Thread):
    """Keeps a fermenter server's board current: the clock, who's attached,
    the workload's readiness, the state of its services (from `services`,
    the concrete fermenter's list of things one can open), milestones waiting
    on a process to become ready, and builds."""

    INTERVAL = 2.0
    GONE = {"Completed", "Error", "Skipped", "Disabled", "Foreground"}  # not coming up on its own

    def __init__(self, board: Board, controller, services: Callable[[], list[Service]] | None = None):
        super().__init__(daemon=True)
        self.board = board
        self.controller = controller
        self.services = services
        self.stopping = threading.Event()
        self.awaiting: dict[str, str | None] = {}  # milestone -> process (None: the whole workload)
        self.tasks: Tasks | None = None  # clients' long tasks, lapsed when they stop reporting
        self._lock = threading.Lock()
        self._error = ""

    def await_ready(self, mid: str, process: str | None = None) -> None:
        """Finish milestone `mid` once `process` (or the whole workload) is ready."""
        with self._lock:
            self.awaiting[mid] = process

    def post(self, event: str, who: str, message: str, fake_time: str | None, fields: dict) -> None:
        """A broadcast: into the feed, and builds become milestones."""
        self.board.post(event, message, who=who, fake_time=fake_time)
        if "build_id" not in fields:
            return
        mid = f"build-{fields['build_id']}"
        if event == "build_started":
            group = f"build {fields.get('name', '?')} @ {str(fields.get('commit', ''))[:12]}"
            self.board.plan([Milestone(id=mid, label=f"build {fields['build_id']}", group=group)])
            self.board.start(mid)
        elif mid in self.board.milestones and event in ("build_succeeded", "build_failed"):
            self.board.finish(
                mid, ok=event == "build_succeeded", detail=None if event == "build_succeeded" else message
            )

    def run(self) -> None:
        while not self.stopping.wait(self.INTERVAL):
            self.refresh()

    def stop(self) -> None:
        self.stopping.set()

    def refresh(self) -> None:
        try:
            self._refresh()
            self._error = ""
        except Exception as e:  # keep publishing; say so once, not every tick
            if str(e) != self._error:
                self._error = str(e)
                logs.log.warning("board_refresh_failed", error=str(e))

    def _refresh(self) -> None:
        b, c = self.board, self.controller
        b.set_clock(c.clock.offset.total_seconds())
        sessions = c.store.sessions()
        who = sorted({s["who"] for s in sessions})
        b.set_fact("attached", ", ".join(who) or "nobody", "attached")
        waiting = waiting_for(c.store, sessions)
        b.set_waiting(waiting)
        if waiting:  # also as a fact, for renderers that predate `waiting`
            b.set_fact("waiting", "; ".join(w.title for w in waiting), "waiting")
        else:
            b.drop_fact("waiting")
        if self.tasks is not None:
            self.tasks.lapse()

        sup = c.supervisor
        procs = {p["name"]: p for p in sup.processes()}
        waiting = sorted(n for n, p in procs.items() if not sup.is_ready(p))
        if not procs:
            b.set_fact("workload", "stopped", "workload")
        else:
            ready = f"{len(procs) - len(waiting)}/{len(procs)} ready"
            b.set_fact("workload", ready + (f" (waiting: {', '.join(waiting)})" if waiting else ""), "workload")

        if self.services is not None:
            out = []
            for s in self.services():
                p = procs.get(s.name)
                if p is not None:
                    state = "up" if sup.is_ready(p) else "down" if p["status"] in self.GONE else "starting"
                    s = s.model_copy(update={"state": state})
                elif s.state == "unknown":
                    s = s.model_copy(update={"state": "down"})
                out.append(s)
            b.set_services(out)

        with self._lock:
            awaiting = dict(self.awaiting)
        for mid, name in awaiting.items():
            m = b.milestones.get(mid)
            outcome = None
            if m is None or m.state != "running":
                outcome = "gone"
            elif name is None:
                outcome = "ready" if sup.all_ready(list(procs.values())) else None
            elif name in procs:
                p = procs[name]
                outcome = "ready" if sup.is_ready(p) else "failed" if p["status"] in self.GONE else None
            if outcome == "ready":
                b.finish(mid)
            elif outcome == "failed":
                b.finish(mid, ok=False, detail=f"{name} is {procs[name]['status']} (see its log)")
            if outcome is not None:
                with self._lock:
                    self.awaiting.pop(mid, None)
