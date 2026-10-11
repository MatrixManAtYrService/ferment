"""The generic part of a fermenter server: health, status, the clock,
checkpoints, who's attached, and what they've done.

A concrete fermenter builds on `create_app` and adds its own routes.
"""

from __future__ import annotations

import datetime as dt
import shlex
import subprocess
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ferment.client import WHO_HEADER
from ferment.databases import Database, Dolt
from ferment.server import logs
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


class Sql(BaseModel):
    sql: str
    database: str | None = None


def who(request: Request) -> str:
    """Who's asking: clients send X-Ferment-User (attribution, not auth: anyone
    who can reach the server already got through ssh with a trusted key)."""
    return request.headers.get(WHO_HEADER, "unknown")


def create_app(controller: TimeController, title: str = "fermenter", database: Database | None = None) -> FastAPI:
    store = controller.store
    log_path = controller.state_dir / "logs" / "fermenter.jsonl"
    logs.configure(log_path)

    def broadcast(event: str, who_: str, message: str, **fields) -> None:
        fake = controller.state.clock.now().isoformat() if controller.state else None
        logs.broadcast(event, who_, message, fake_time=fake, **fields)

    sources = SourceStore(controller.state_dir)
    builder = Builder(store, sources, controller.state_dir / "logs" / "builds", broadcast=broadcast)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        controller.resume()
        yield
        controller.stop()

    app = FastAPI(title=title, lifespan=lifespan)
    app.state.controller = controller

    # Every state-changing request lands in the event log, with who sent it.
    @app.middleware("http")
    async def log_events(request: Request, call_next):
        response = await call_next(request)
        if request.method not in ("GET", "HEAD", "OPTIONS") and not request.url.path.endswith("/heartbeat"):
            fake = controller.state.clock.now().isoformat() if controller.state else None
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
        out = controller.set_time(req.at, who=w)
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
        out = controller.restore(ref, who=w)
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
        serves = ", ".join(req.relays + req.fetches) or "nothing"
        broadcast("attached", w, f"{w} attached from {req.client} (serving: {serves})")
        return {"id": store.open_session(w, req.client, req.relays, req.fetches)}

    @app.post("/sessions/{session_id}/heartbeat")
    def heartbeat(session_id: int):
        if not store.touch_session(session_id):
            raise HTTPException(404, "no open session with that id")
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

    @app.get("/builds/{build_id}/log")
    def build_log(build_id: int, offset: int = 0):
        text, next_offset = builder.log(build_id, offset)
        return {"text": text, "offset": next_offset}

    if database is not None:
        _mount_database(app, database)

    return app


def _mount_database(app: FastAPI, database: Database) -> None:
    @app.get("/db")
    def db_info():
        return {"kind": database.kind, "versioned": database.versioned, "client": database.client}

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

    if isinstance(database, Dolt):

        @app.get("/db/diff")
        def db_diff(from_ref: str, to_ref: str = "WORKING"):
            """Refs: dolt hashes, checkpoint names, or ckpt-<id>."""
            return database.diff(from_ref, to_ref)
