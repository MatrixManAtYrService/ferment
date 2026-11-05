"""The generic part of a fermenter server: health, status, the clock,
checkpoints, who's attached, and what they've done.

A concrete fermenter builds on `create_app` and adds its own routes.
"""

from __future__ import annotations

import datetime as dt
import shlex
import subprocess
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from ferment import measure as measure_mod
from ferment import needs as needs_mod
from ferment.board import Board, Service
from ferment.client import WHO_HEADER
from ferment.databases import Database
from ferment.server import clock as clock_mod
from ferment.server import logs, telemetry
from ferment.server.board import Publisher, Steps, Tasks, board_router, tasks_router
from ferment.server.builds import Builder
from ferment.server.data import DataStore, Loader, data_root, data_router
from ferment.server.sources import SourceStore, SourceStoreError
from ferment.server.time_control import CheckpointError, TimeController


class SetTime(BaseModel):
    at: dt.datetime


class ImportReq(BaseModel):
    prefix: str


class NewMark(BaseModel):
    label: str


class NewCheckpoint(BaseModel):
    message: str
    name: str | None = None
    # A stage's checkpoint: the step's name, a hash of its inputs, what it returned.
    stage: str | None = None
    fingerprint: str | None = None
    value: Any = None


class Adopt(BaseModel):
    stage: str
    fingerprint: str
    value: Any = None


class NewSession(BaseModel):
    client: str
    relays: list[str] = []
    fetches: list[str] = []  # git hosts this client can fetch from on the VM's behalf
    provides: list[str] = []  # other capabilities it serves (ferment.needs), e.g. data:<source>
    client_time: dt.datetime | None = None  # the client's (real) time: corrects the VM's idea of real time


class Heartbeat(BaseModel):
    client_time: dt.datetime | None = None
    # What the client serves now, when that changed since it attached (None: as before).
    fetches: list[str] | None = None
    provides: list[str] | None = None


class NewNeed(BaseModel):
    capability: str  # ferment.needs: relay:<host:port> | fetch:<git host>
    purpose: str  # what's waiting, e.g. "fmnt apply"


class NewAction(BaseModel):
    key: str  # what to do, in the raiser's terms: it alone maps keys to commands
    title: str
    detail: str = ""


class NewSourceRequest(BaseModel):
    name: str
    repo: str
    ref: str


class FinishSourceRequest(BaseModel):
    commit: str | None = None
    error: str | None = None


class NewBuild(BaseModel):
    name: str
    commit: str
    command: str
    artifact: str | None = None  # glob, relative to the checkout
    inputs: dict[str, str] = {}  # other sources checked out for it: name -> commit (FERMENT_INPUT_<NAME>)


class Deploy(BaseModel):
    build_id: int


class DeployMany(BaseModel):
    builds: dict[str, int]  # service name -> build id; all start in one workload restart


class Sql(BaseModel):
    sql: str
    database: str | None = None


def who(request: Request) -> str:
    """Who's asking: clients send X-Ferment-User (attribution, not auth: anyone
    who can reach the server already got through ssh with a trusted key)."""
    return request.headers.get(WHO_HEADER, "unknown")


def create_app(
    controller: TimeController,
    title: str = "fermenter",
    database: Database | None = None,
    before_deploy: Callable[[str, dict, Callable[[str], None]], None] | None = None,
    services: Callable[[], list[Service]] | None = None,
    measure: dict[str, Callable[[], dict]] | None = None,
    loki_url: str | None = None,
    log_files: Callable[[], dict[str, tuple[dict[str, str], Path]]] | None = None,
) -> FastAPI:
    """`before_deploy(name, build, say)` runs before a service is (re)deployed,
    e.g. its schema migrations; raising stops the deploy. `services` lists
    what people can open or connect to, for the board (ferment.board); their
    up/down state comes from the workload process of the same name.
    `measure`: VM name -> a sample of it (ferment.measure), taken every
    interval and kept; default, this machine as "vm". `loki_url`: ship the
    workload's process logs there, and `log_files()` (stream name -> (labels,
    path)), stamped with real time (ferment.server.telemetry)."""
    store = controller.store
    log_path = controller.state_dir / "logs" / "fermenter.jsonl"
    logs.configure(log_path)
    # The VM's own clock reads the fake date: the board is stamped in real time.
    board = Board(now=clock_mod.real_now)
    publisher = Publisher(board, controller, services)
    if measure is None:
        cpu = measure_mod.CpuMeter()
        remote = getattr(controller.supervisor, "measure_now", None)  # the workload agent, on its own VM
        if remote is not None:  # split: this VM by its largest processes, the workload's by the agent
            measure = {"control": lambda: measure_mod.collect(cpu, {"root": Path("/")}), "workload": remote}
        else:
            measure = {"vm": lambda: measure_mod.collect(cpu, {"root": Path("/")}, controller.supervisor.processes())}
    latest: dict[str, tuple[float, dict]] = {}  # VM -> (real unix time, its last sample), for /metrics

    def keep_sample(vm: str, sample: dict) -> None:
        now = clock_mod.real_now()
        store.add_sample(now.isoformat(), vm, sample)
        latest[vm] = (now.timestamp(), sample)

    sampler = measure_mod.Sampler(measure, keep_sample)
    shipper = None
    if loki_url:
        workload_role = "workload" if getattr(controller.supervisor, "measure_now", None) is not None else "vm"

        def log_sources() -> dict[str, tuple[dict[str, str], telemetry.Reader]]:
            sup = controller.supervisor
            out: dict[str, tuple[dict[str, str], telemetry.Reader]] = {
                f"process/{p['name']}": (
                    {"source": "process", "vm": workload_role, "process": p["name"]},
                    lambda offset, name=p["name"]: sup.log(name, offset),
                )
                for p in sup.processes()
                if p.get("name")
            }
            for name, (labels, path) in (log_files() if log_files else {}).items():
                out[name] = (labels, telemetry.file_reader(path))
            return out

        shipper = telemetry.LogShipper(
            loki_url, log_sources, controller.state_dir / "logship.json",
            fake_now=controller.clock.now, real_now=clock_mod.real_now,
        )  # fmt: skip

    def broadcast(event: str, who_: str, message: str, **fields) -> None:
        logs.broadcast(event, who_, message, **fields)
        publisher.post(event, who_, message, controller.clock.now().isoformat(timespec="seconds"), fields)

    def steps(group: str, keys: list[str], **labels: str) -> Steps:
        return Steps(board, group, keys, labels)

    sources = SourceStore(controller.state_dir)
    builder = Builder(store, sources, controller.state_dir / "logs" / "builds", broadcast=broadcast)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        controller.resume()
        started = steps("server started", ["ready"])
        publisher.await_ready(started.ids["ready"])
        publisher.refresh()
        publisher.start()
        sampler.start()
        if shipper is not None:
            shipper.start()
        yield
        if shipper is not None:
            shipper.stop()
        sampler.stop()
        publisher.stop()
        controller.stop()

    app = FastAPI(title=title, lifespan=lifespan)
    app.state.controller = controller
    app.state.board = board
    app.state.publisher = publisher
    app.include_router(board_router(board))
    tasks = Tasks(board)
    publisher.tasks = tasks
    app.include_router(tasks_router(tasks))
    datastore = DataStore(data_root(controller.state_dir))
    loader = None
    if database is not None and hasattr(database, "client_args"):  # MySQL; MariaDB is the no-archive fallback
        loader = Loader(datastore, database.client_args, tasks, broadcast)
    app.include_router(data_router(store, datastore, loader, who, broadcast))
    app.state.datastore, app.state.loader = datastore, loader

    # Every state-changing request lands in the event log, with who sent it.
    @app.middleware("http")
    async def log_events(request: Request, call_next):
        response = await call_next(request)
        if request.method not in ("GET", "HEAD", "OPTIONS") and not request.url.path.endswith("/heartbeat"):
            fake = controller.clock.now().isoformat()
            store.add_event(
                who(request),
                f"{request.method} {request.url.path}",
                status=response.status_code,
                fake_time=fake,
            )
        return response

    @app.exception_handler(CheckpointError)
    @app.exception_handler(SourceStoreError)
    async def bad_request(_request: Request, exc: Exception):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.get("/status")
    def status():
        return controller.status()

    # Plain `def`: these block (stop, snapshot, relaunch, wait for readiness),
    # so FastAPI runs them in a worker thread. The controller's lock
    # serializes concurrent requests from multiple attached clients.
    @app.post("/time")
    def set_time(req: SetTime, request: Request):
        if req.at.tzinfo is None:
            raise HTTPException(422, "give a timezone, e.g. 2026-10-03T00:00:00Z")
        w = who(request)
        broadcast("clock_change", w, f"{w} is moving the clock to {req.at.isoformat()} (workload restarting)")
        keys = ["stop", *(["checkpoint"] if controller.versioned else []), "clock", "start", "ready"]
        s = steps(f"clock to {req.at.isoformat()[:19].replace('T', ' ')} ({w})", keys)
        out = _stepped(s, lambda: controller.set_time(req.at, who=w, step=s))
        broadcast("clock_changed", w, f"clock is now {out['fake_now'][:19]} (set by {w})")
        return out

    @app.get("/checkpoints")
    def checkpoints(limit: int = 50):
        head = store.head()
        return [{**c, "head": c["id"] == head} for c in store.checkpoints(limit)]

    @app.post("/checkpoints")
    def checkpoint(req: NewCheckpoint, request: Request):
        w = who(request)
        stage = None
        if req.stage is not None:
            if not req.fingerprint:
                raise HTTPException(422, "a stage's checkpoint needs its fingerprint")
            stage = {"stage": req.stage, "fingerprint": req.fingerprint, "value": req.value}
        cp = controller.checkpoint(req.message, req.name, who=w, stage=stage)
        label = f"{cp['id']}" + (f" [{cp['name']}]" if cp["name"] else "") + (f" (stage {req.stage})" if stage else "")
        broadcast("checkpoint", w, f"{w} checkpointed {label}: {req.message}")
        return cp

    @app.post("/checkpoints/{ref}/stage")
    def adopt(ref: str, req: Adopt, request: Request):
        """Mark an existing checkpoint as `stage`'s snapshot (made by hand, before the stage was)."""
        cp = store.checkpoint(ref)
        if cp is None:
            raise HTTPException(404, f"no checkpoint {ref!r}")
        w = who(request)
        out = store.adopt(cp["id"], req.stage, req.fingerprint, req.value)
        broadcast("checkpoint", w, f"{w} adopted checkpoint {cp['id']} as stage {req.stage}")
        return out

    @app.delete("/checkpoints/{ref}")
    def drop_checkpoint(ref: str, request: Request, forever: bool = False):
        """Free a checkpoint's data (archived first, with a shelf). Not named, stage or current
        ones, unless `forever`: then named and stage ones too, and from the shelf as well."""
        try:
            out = controller.drop(ref, forever=forever)
        except CheckpointError as e:
            raise HTTPException(409, str(e)) from e
        how = "for good" if forever else "(kept on the shelf)" if out.get("archive") else ""
        broadcast("checkpoint", who(request), f"{who(request)} dropped checkpoint {out['id']}'s data {how}".strip())
        return out

    @app.post("/checkpoints/{ref}/archive")
    def archive_checkpoint(ref: str, request: Request):
        """Put a checkpoint's data on the shelf (relative to its nearest ancestor that's here)."""
        try:
            out = controller.archive(ref)
        except CheckpointError as e:
            raise HTTPException(409, str(e)) from e
        broadcast("checkpoint", who(request), f"{who(request)} archived checkpoint {out['checkpoint_id']}")
        return out

    @app.post("/checkpoints/{ref}/unarchive")
    def unarchive_checkpoint(ref: str, request: Request):
        """Bring a dropped checkpoint's data back from the shelf (restorable again)."""
        try:
            out = controller.unarchive(ref)
        except CheckpointError as e:
            raise HTTPException(409, str(e)) from e
        broadcast("checkpoint", who(request), f"{who(request)} brought checkpoint {out['id']} back from the shelf")
        return out

    @app.post("/checkpoints/import")
    def import_checkpoint(req: ImportReq, request: Request):
        """Make a checkpoint here from one archived on the shelf (by any fermenter),
        its base first; the workload stays where it is."""
        try:
            out = controller.import_archive(req.prefix, who(request))
        except CheckpointError as e:
            raise HTTPException(409, str(e)) from e
        broadcast("checkpoint", who(request), f"{who(request)} imported checkpoint {out['id']} from {req.prefix}")
        return out

    @app.get("/shelf")
    def shelf_list(prefix: str = ""):
        """What's on the shelf under `prefix` (keys), with each object's metadata."""
        if controller.shelf is None:
            raise HTTPException(404, "this fermenter has no shelf")
        return [{"key": k, "meta": controller.shelf.meta(k)} for k in controller.shelf.list(prefix)]

    @app.post("/checkpoints/prune")
    def prune_checkpoints(request: Request, kind: str = "before-restore", keep: int = 3):
        """Drop all but the newest `keep` restorable checkpoints of `kind` (the automatic
        ones taken before each restore pile up and fill the disk)."""
        dropped = controller.prune(kind, keep)
        broadcast("checkpoint", who(request), f"{who(request)} pruned {len(dropped)} {kind} checkpoint(s)")
        return {"dropped": dropped}

    # -- what the VMs used (ferment.measure) ---------------------------------------

    def _window(start: str | None, end: str | None, mark: int | None) -> tuple[str, str]:
        now = clock_mod.real_now().isoformat()
        if mark is not None:
            m = store.mark(mark)
            if m is None:
                raise HTTPException(404, f"no mark {mark}")
            return m["started"], m["ended"] or now
        return start or "0000", end or now

    @app.get("/measure")
    def measure_samples(start: str | None = None, end: str | None = None, mark: int | None = None,
                        vm: str | None = None):  # fmt: skip
        """Samples (real time) between `start` and `end` (ISO), or over mark `mark`."""
        return store.samples(*_window(start, end, mark), vm)

    @app.get("/measure/summary")
    def measure_summary(start: str | None = None, end: str | None = None, mark: int | None = None):
        """Per VM and number: peak, p95, mean, n; over a window or a mark."""
        lo, hi = _window(start, end, mark)
        return {"start": lo, "end": hi, "vms": measure_mod.summarize(store.samples(lo, hi))}

    @app.post("/measure/marks")
    def start_mark(req: NewMark, request: Request):
        """Name a stretch of time from now (`POST /measure/marks/{id}/end` ends it)."""
        return store.add_mark(req.label, who(request), clock_mod.real_now().isoformat())

    @app.post("/measure/marks/{mark_id}/end")
    def end_mark(mark_id: int):
        out = store.end_mark(mark_id, clock_mod.real_now().isoformat())
        if out is None:
            raise HTTPException(404, f"no mark {mark_id}")
        return out

    @app.get("/measure/marks")
    def list_marks(label: str | None = None, limit: int = 100):
        return store.marks(label, limit)

    @app.get("/metrics")
    def metrics():
        """The latest sample of each VM and the clock, for Prometheus (ferment.server.telemetry)."""
        text = telemetry.render_metrics(latest, controller.clock.offset.total_seconds(), controller.clock.now())
        return Response(text, media_type="text/plain; version=0.0.4")

    @app.get("/clock/offsets")
    def clock_offsets(start: str = "0000", end: str = "9999"):
        """The guest clock's offset (fake - real, s) in force over a real-time window: the one
        at `start`, then each change. Converting a fake time to real needs the offset then."""
        return store.offsets(start, end)

    @app.get("/stages/{stage}")
    def stage(stage: str, fingerprint: str | None = None):
        """A stage's checkpoints, newest first (only those with `fingerprint`, if given)."""
        return store.stage(stage, fingerprint)

    @app.post("/checkpoints/{ref}/restore")
    def restore(ref: str, request: Request):
        w = who(request)
        broadcast("restore", w, f"{w} is restoring checkpoint {ref} (workload restarting)")
        s = steps(f"restore {ref} ({w})", ["stop", "checkpoint", "restore", "clock", "start", "ready"])
        out = _stepped(s, lambda: controller.restore(ref, who=w, step=s))
        broadcast("restored", w, f"restored checkpoint {ref}; clock is now {out['fake_now'][:19]} (by {w})")
        return out

    @app.get("/broadcasts")
    def broadcasts(after: int = -1):
        """Broadcast log entries after byte offset `after` (-1: just return
        the current end, to start following from now)."""
        entries, offset = logs.broadcasts(log_path, after)
        return {"entries": entries, "offset": offset}

    @app.get("/events")
    def events(limit: int = 50):
        return store.events(limit)

    @app.get("/sessions")
    def sessions(include_gone: bool = False):
        return store.sessions(include_gone)

    @app.post("/sessions")
    def open_session(req: NewSession, request: Request):
        w = who(request)
        if req.client_time is not None:
            controller.clock.observe(req.client_time, w)
        serves = ", ".join(req.relays + req.fetches + req.provides) or "nothing"
        broadcast("attached", w, f"{w} attached from {req.client} (serving: {serves})")
        return {"id": store.open_session(w, req.client, req.relays, req.fetches, req.provides)}

    @app.post("/sessions/{session_id}/heartbeat")
    def heartbeat(session_id: int, request: Request, req: Heartbeat | None = None):
        fetches, provides = (req.fetches, req.provides) if req is not None else (None, None)
        if not store.touch_session(session_id, fetches, provides):
            raise HTTPException(404, "no open session with that id")
        if req is not None and req.client_time is not None:
            controller.clock.observe(req.client_time, who(request))
        return {"ok": True}

    @app.delete("/sessions/{session_id}")
    def close_session(session_id: int, request: Request):
        w = who(request)
        store.close_session(session_id)
        broadcast("detached", w, f"{w} detached")
        return {"ok": True}

    @app.get("/reach")
    def reach(url: str):
        """Can the VM reach `url` (an http(s) URL)? Any HTTP answer counts: it
        means DNS, the connection, and TLS all worked (e.g. through a relay)."""
        if not url.startswith(("http://", "https://")):
            raise HTTPException(422, "http(s) URLs only")
        try:
            r = httpx.get(url, timeout=10, follow_redirects=False)
            return {"url": url, "reachable": True, "status": r.status_code}
        except httpx.HTTPError as e:
            return {"url": url, "reachable": False, "error": f"{type(e).__name__}: {e}"}

    # Work waiting for something only an attached client provides
    # (ferment.needs). The waiting client keeps its need fresh; the board says
    # what's waiting, and whether anyone attached serves it.

    # Actions: something only one attached person can do (sign in to a data
    # source). The client that raised one resolves it once it's done.

    @app.get("/actions")
    def actions():
        return store.actions()

    @app.post("/actions")
    def raise_action(req: NewAction, request: Request):
        w = who(request)
        fresh = not any(a["key"] == req.key and a["who"] == w for a in store.actions())
        a = store.raise_action(req.key, req.title, req.detail, w)
        if fresh:
            broadcast("action_needed", w, f"waiting on {w}: {req.title}")
        return a

    @app.delete("/actions/{action_id}")
    def resolve_action(action_id: int, request: Request):
        a = store.resolve_action(action_id)
        if a is None:
            raise HTTPException(404, f"no action {action_id}")
        broadcast("action_done", who(request), f"done: {a['title']}")
        return a

    @app.get("/needs")
    def needs():
        offered: dict[str, list[str]] = {}
        for s in store.sessions():
            for cap in needs_mod.offered(s):
                offered.setdefault(cap, []).append(s["who"])
        return [{**n, "served_by": offered.get(n["capability"], [])} for n in store.needs()]

    @app.post("/needs")
    def add_need(req: NewNeed, request: Request):
        w = who(request)
        n = store.add_need(req.capability, req.purpose, w)
        broadcast("waiting", w, f"{req.purpose} ({w}) is waiting for {needs_mod.describe(req.capability)}")
        return n

    @app.post("/needs/{need_id}/heartbeat")
    def need_heartbeat(need_id: int):
        if not store.touch_need(need_id):
            raise HTTPException(404, "no open need with that id")
        return {"ok": True}

    @app.delete("/needs/{need_id}")
    def close_need(need_id: int, request: Request, met: bool = True):
        n = store.close_need(need_id, met)
        if n is None:
            raise HTTPException(404, f"no need {need_id}")
        outcome = "got it" if met else "gave up"
        broadcast("waited", who(request), f"{n['purpose']} ({n['who']}) {outcome}: {n['capability']}")
        return n

    # -- source and builds ------------------------------------------------------

    @app.get("/sources/{name}")
    def source(name: str):
        return {"commits": sources.commits(name)}

    @app.get("/sources/{name}/bundle")
    def download_source(name: str, commit: str, have: list[str] = Query(default=[])):  # noqa: B008
        """`commit` as a git bundle (less what `have` covers): for a machine that
        can reach the fermenter but not the repo (e.g. off the VPN)."""
        try:
            data = sources.bundle(name, commit, have)
        except SourceStoreError as e:
            raise HTTPException(404, str(e)) from e
        return Response(content=data, media_type="application/octet-stream")

    @app.post("/sources/{name}")
    async def upload_source(name: str, request: Request):
        """Body: a git bundle (ferment.sources.bundle)."""
        data = await request.body()
        return {"received": await run_in_threadpool(sources.receive, name, data)}

    # Code the VM needs but can't fetch: an attached client that can claims
    # the request, fetches it, and uploads a bundle to /sources/{name}.

    @app.post("/source-requests")
    def request_source(req: NewSourceRequest, request: Request):
        w = who(request)
        r = store.request_source(req.name, req.repo, req.ref, w)
        if r["requested_by"] == w and r["status"] == "pending":
            broadcast(
                "source_requested",
                w,
                f"{w} asked for {req.name} @ {req.ref} (needs an attached client that can fetch it)",
            )
        return r

    @app.get("/source-requests")
    def source_requests():
        return store.claimable_source_requests()

    @app.get("/source-requests/{request_id}")
    def source_request(request_id: int):
        r = store.source_request(request_id)
        if r is None:
            raise HTTPException(404, f"no source request {request_id}")
        return r

    @app.post("/source-requests/{request_id}/claim")
    def claim_source_request(request_id: int, request: Request):
        w = who(request)
        if not store.claim_source_request(request_id, w):
            raise HTTPException(409, "already claimed")
        r = store.source_request(request_id)
        broadcast("source_claimed", w, f"{w} is fetching {r['name']} @ {r['ref']} for {r['requested_by']}")
        return r

    @app.post("/source-requests/{request_id}/finish")
    def finish_source_request(request_id: int, req: FinishSourceRequest):
        if (req.commit is None) == (req.error is None):
            raise HTTPException(422, "give exactly one of commit or error")
        existing = store.source_request(request_id)
        if existing is None:
            raise HTTPException(404, f"no source request {request_id}")
        if req.commit is not None and not sources.has(existing["name"], req.commit):
            raise HTTPException(422, f"commit {req.commit} hasn't been uploaded")
        store.finish_source_request(request_id, commit=req.commit, error=req.error)
        r = store.source_request(request_id)
        if req.error is None:
            broadcast(
                "source_fetched",
                r["claimed_by"],
                f"{r['name']} @ {r['ref']} is {req.commit[:12]} (fetched by {r['claimed_by']})",
            )
        else:
            broadcast(
                "source_failed",
                r["claimed_by"],
                f"{r['claimed_by']} couldn't fetch {r['name']} @ {r['ref']}: {req.error}",
            )
        return r

    @app.post("/builds")
    def request_build(req: NewBuild, request: Request):
        missing = [f"{n} @ {c}" for n, c in req.inputs.items() if not sources.has(n, c)]
        if missing:
            raise HTTPException(422, f"not uploaded: {', '.join(missing)}")
        return builder.request(req.name, req.commit, req.command, req.artifact, who(request), inputs=req.inputs)

    @app.get("/builds")
    def builds(limit: int = 50):
        return store.builds(limit)

    @app.get("/builds/{build_id}")
    def build(build_id: int):
        b = store.build(build_id)
        if b is None:
            raise HTTPException(404, f"no build {build_id}")
        return b

    # Which build of each service the workload runs. Changing it restarts the
    # workload (same clock) so the stack picks it up.

    @app.get("/deployments")
    def deployments():
        return store.deployments()

    # Deploys run in the background (migrations, then a workload restart);
    # clients follow /deployments/{name}/progress and the process's log.
    progress: dict[str, dict] = {}

    def run_deploy(chosen: list[tuple[str, dict]], w: str) -> None:
        """Prepare each service (in order), record them all, then restart the
        workload once: every service of a template comes up together."""
        names = [name for name, _ in chosen]
        what = ", ".join(f"{name} @ {b['commit_sha'][:12]}" for name, b in chosen)
        said: list[str] = []
        keys = [*(["prepare"] if before_deploy is not None else []), "stop", "start", "ready"]
        s = steps(f"deploy {what} ({w})", keys, ready=f"{', '.join(names)} ready")

        def update(**fields) -> None:
            for name in names:
                progress[name] = {**progress[name], **fields}

        def say(msg: str) -> None:
            said.append(msg)
            update(steps=list(said))
            s.note("prepare", msg)

        try:
            if before_deploy is not None:
                update(state="preparing")
                for name, b in chosen:
                    before_deploy(name, b, say)
                s("prepare")
            for name, b in chosen:
                store.deploy(name, b["id"], w)
            update(state="starting")
            controller.relaunch(wait_ready=False, step=s)
            publisher.await_ready(s.ids["ready"], names[0] if len(names) == 1 else None)
            update(state="started")
            broadcast("deployed", w, f"{what} deployed by {w}; starting")
        except Exception as e:  # report it rather than lose it in a thread
            s.fail(str(e))
            update(state="failed", error=str(e))
            broadcast("deploy_failed", w, f"deploying {', '.join(names)} failed: {e}")

    def start_deploy(builds: dict[str, int], request: Request) -> dict[str, dict]:
        chosen = []
        for name, build_id in builds.items():
            b = store.build(build_id)
            if b is None or b["name"] != name or b["status"] != "succeeded":
                raise HTTPException(422, f"build {build_id} isn't a successful build of {name}")
            if progress.get(name, {}).get("state") in ("preparing", "starting"):
                raise HTTPException(409, f"{name} is already being deployed")
            chosen.append((name, b))
        if not chosen:
            raise HTTPException(422, "nothing to deploy")
        w = who(request)
        for name, b in chosen:
            progress[name] = {"name": name, "build_id": b["id"], "state": "queued", "steps": [], "who": w}
        what = ", ".join(f"{name} @ {b['commit_sha'][:12]}" for name, b in chosen)
        broadcast("deploying", w, f"{w} is deploying {what}")
        accepted = {name: dict(progress[name]) for name, _ in chosen}  # the thread updates progress from here on
        threading.Thread(target=run_deploy, args=(chosen, w), daemon=True).start()
        return accepted

    @app.put("/deployments/{name}", status_code=202)
    def deploy(name: str, req: Deploy, request: Request):
        return start_deploy({name: req.build_id}, request)[name]

    @app.put("/deployments", status_code=202)
    def deploy_many(req: DeployMany, request: Request):
        """Deploy several services with one workload restart."""
        return start_deploy(req.builds, request)

    @app.get("/deployments/{name}/progress")
    def deploy_progress(name: str):
        if name not in progress:
            raise HTTPException(404, f"no deploy of {name} since the server started")
        return progress[name]

    @app.get("/processes/{name}/log")
    def process_log(name: str, offset: int = 0):
        """A workload process's output from byte `offset` on."""
        text, next_offset = controller.supervisor.log(name, offset)
        return {"text": text, "offset": next_offset}

    def undeploy_names(names: list[str], request: Request) -> dict:
        deployed = {d["name"] for d in store.deployments()}
        missing = [n for n in names if n not in deployed]
        if missing or not names:
            raise HTTPException(404, f"{', '.join(missing) or 'nothing'} isn't deployed")
        for name in names:
            store.undeploy(name)
        w = who(request)
        what = ", ".join(names)
        broadcast("undeployed", w, f"{w} undeployed {what} (workload restarting)")
        s = steps(f"undeploy {what} ({w})", ["stop", "start", "ready"])
        return _stepped(s, lambda: controller.relaunch(step=s))

    @app.delete("/deployments/{name}")
    def undeploy(name: str, request: Request):
        return undeploy_names([name], request)

    @app.delete("/deployments")
    def undeploy_many(request: Request, names: Annotated[list[str] | None, Query()] = None):
        """Undeploy several services (default: all) with one workload restart."""
        return undeploy_names(names or [d["name"] for d in store.deployments()], request)

    @app.get("/builds/{build_id}/log")
    def build_log(build_id: int, offset: int = 0):
        text, next_offset = builder.log(build_id, offset)
        return {"text": text, "offset": next_offset}

    if database is not None:
        _mount_database(app, controller, database)

    return app


def _stepped(s: Steps, run: Callable[[], dict]) -> dict:
    """Run a controller operation reported through `s`: an error, or a
    workload that never got ready, fails the step it stopped at."""
    try:
        out = run()
    except Exception as e:
        s.fail(str(e))
        raise
    if out.get("ready") is False:
        s.fail("not ready in time (see fmnt status)")
    return out


def _mount_database(app: FastAPI, controller: TimeController, database: Database) -> None:
    @app.get("/db/changes")
    def db_changes(from_ref: str, to_ref: str | None = None):
        """What the binlog says was written between two checkpoints (names or
        ids; to: now if omitted), table by table (ferment.binlog)."""

        def position(ref: str) -> str:
            cp = controller.store.checkpoint(ref)
            if cp is None:
                raise HTTPException(404, f"no checkpoint {ref!r}")
            pos = cp["parts"].get("volume:binlog")
            if pos is None:
                raise HTTPException(422, f"checkpoint {ref!r} has no binlog position (taken before they were kept)")
            return pos

        changes = getattr(controller.supervisor, "db_changes", None)
        if changes is None:
            raise HTTPException(501, "this fermenter's workload agent can't read its binlog")
        return changes(position(from_ref), position(to_ref) if to_ref else None)

    @app.get("/db")
    def db_info():
        return {"kind": database.kind, "versioned": bool(controller.versioned), "client": database.client}

    # Runs the stock CLI client server-side, so callers need no driver, no
    # route to the database port, and no opinion about the fake clock.
    @app.post("/db/sql")
    def db_sql(req: Sql):
        cmd = f"{database.client} --batch --table"
        if req.database:
            cmd += f" --database={shlex.quote(req.database)}"
        result = subprocess.run(["bash", "-c", cmd], input=req.sql, text=True, capture_output=True, timeout=300)
        if result.returncode != 0:
            raise HTTPException(400, result.stderr.strip())
        return {"output": result.stdout}
