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

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ferment.archive import DoltArchive
from ferment.board import Board, Service
from ferment.client import WHO_HEADER
from ferment.databases import Database
from ferment.server import clock as clock_mod
from ferment.server import logs
from ferment.server.board import Publisher, Steps, board_router
from ferment.server.builds import Builder
from ferment.server.sources import SourceStore, SourceStoreError
from ferment.server.time_control import CheckpointError, TimeController


class SetTime(BaseModel):
    at: dt.datetime


class NewCheckpoint(BaseModel):
    message: str
    name: str | None = None


class NewSession(BaseModel):
    client: str
    relays: list[str] = []
    fetches: list[str] = []  # git hosts this client can fetch from on the VM's behalf
    client_time: dt.datetime | None = None  # the client's (real) time: corrects the VM's idea of real time


class Heartbeat(BaseModel):
    client_time: dt.datetime | None = None


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


class Deploy(BaseModel):
    build_id: int


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
) -> FastAPI:
    """`before_deploy(name, build, say)` runs before a service is (re)deployed,
    e.g. its schema migrations; raising stops the deploy. `services` lists
    what people can open or connect to, for the board (ferment.board); their
    up/down state comes from the workload process of the same name."""
    store = controller.store
    log_path = controller.state_dir / "logs" / "fermenter.jsonl"
    logs.configure(log_path)
    # The VM's own clock reads the fake date: the board is stamped in real time.
    board = Board(now=clock_mod.real_now)
    publisher = Publisher(board, controller, services)

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
        yield
        publisher.stop()
        controller.stop()

    app = FastAPI(title=title, lifespan=lifespan)
    app.state.controller = controller
    app.state.board = board
    app.state.publisher = publisher
    app.include_router(board_router(board))

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
        return store.checkpoints(limit)

    @app.post("/checkpoints")
    def checkpoint(req: NewCheckpoint, request: Request):
        w = who(request)
        cp = controller.checkpoint(req.message, req.name, who=w)
        label = f"{cp['id']}" + (f" [{cp['name']}]" if cp["name"] else "")
        broadcast("checkpoint", w, f"{w} checkpointed {label}: {req.message}")
        return cp

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
        serves = ", ".join(req.relays + req.fetches) or "nothing"
        broadcast("attached", w, f"{w} attached from {req.client} (serving: {serves})")
        return {"id": store.open_session(w, req.client, req.relays, req.fetches)}

    @app.post("/sessions/{session_id}/heartbeat")
    def heartbeat(session_id: int, request: Request, req: Heartbeat | None = None):
        if not store.touch_session(session_id):
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

    # -- source and builds ------------------------------------------------------

    @app.get("/sources/{name}")
    def source(name: str):
        return {"commits": sources.commits(name)}

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
        return builder.request(req.name, req.commit, req.command, req.artifact, who(request))

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

    def run_deploy(name: str, b: dict, w: str) -> None:
        said: list[str] = []
        keys = [*(["prepare"] if before_deploy is not None else []), "stop", "start", "ready"]
        s = steps(f"deploy {name} @ {b['commit_sha'][:12]} ({w})", keys, ready=f"{name} ready")

        def say(msg: str) -> None:
            said.append(msg)
            progress[name] = {**progress[name], "steps": list(said)}
            s.note("prepare", msg)

        try:
            if before_deploy is not None:
                progress[name]["state"] = "preparing"
                before_deploy(name, b, say)
                s("prepare")
            store.deploy(name, b["id"], w)
            progress[name]["state"] = "starting"
            controller.relaunch(wait_ready=False, step=s)
            publisher.await_ready(s.ids["ready"], name)
            progress[name]["state"] = "started"
            broadcast("deployed", w, f"{name} @ {b['commit_sha'][:12]} deployed by {w}; starting")
        except Exception as e:  # report it rather than lose it in a thread
            s.fail(str(e))
            progress[name] = {**progress[name], "state": "failed", "error": str(e)}
            broadcast("deploy_failed", w, f"deploying {name} failed: {e}")

    @app.put("/deployments/{name}", status_code=202)
    def deploy(name: str, req: Deploy, request: Request):
        b = store.build(req.build_id)
        if b is None or b["name"] != name or b["status"] != "succeeded":
            raise HTTPException(422, f"build {req.build_id} isn't a successful build of {name}")
        if progress.get(name, {}).get("state") in ("preparing", "starting"):
            raise HTTPException(409, f"{name} is already being deployed")
        w = who(request)
        progress[name] = {"name": name, "build_id": b["id"], "state": "queued", "steps": [], "who": w}
        broadcast("deploying", w, f"{w} is deploying {name} @ {b['commit_sha'][:12]}")
        accepted = dict(progress[name])  # the thread updates progress[name] from here on
        threading.Thread(target=run_deploy, args=(name, b, w), daemon=True).start()
        return accepted

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

    @app.delete("/deployments/{name}")
    def undeploy(name: str, request: Request):
        if not store.undeploy(name):
            raise HTTPException(404, f"{name} isn't deployed")
        w = who(request)
        broadcast("undeployed", w, f"{w} undeployed {name} (workload restarting)")
        s = steps(f"undeploy {name} ({w})", ["stop", "start", "ready"])
        return _stepped(s, lambda: controller.relaunch(step=s))

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
    archive = next((v for v in controller.versioned if isinstance(v, DoltArchive)), None)

    @app.get("/db")
    def db_info():
        return {"kind": database.kind, "versioned": archive is not None, "client": database.client}

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

    if archive is not None:

        @app.get("/db/diff")
        def db_diff(from_ref: str, to_ref: str = "HEAD"):
            """Refs: checkpoint names, ckpt-<id>, or dolt commits (in the archive)."""
            return archive.diff(from_ref, to_ref)

        @app.get("/db/diff/{database_name}/{table}")
        def db_row_diff(database_name: str, table: str, from_ref: str, to_ref: str = "HEAD"):
            """Row by row: what changed in one table between two checkpoints."""
            return archive.row_diff(database_name, table, from_ref, to_ref)
