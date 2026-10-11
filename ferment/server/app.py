"""The generic part of a fermenter server: health, status, the clock,
checkpoints, who's attached, and what they've done.

A concrete fermenter builds on `create_app` and adds its own routes.
"""

from __future__ import annotations

import datetime as dt
import shlex
import subprocess
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ferment.client import WHO_HEADER
from ferment.databases import Database, Dolt
from ferment.server.time_control import CheckpointError, TimeController


class SetTime(BaseModel):
    at: dt.datetime


class NewCheckpoint(BaseModel):
    message: str
    name: str | None = None


class NewSession(BaseModel):
    client: str
    relays: list[str] = []


class Sql(BaseModel):
    sql: str
    database: str | None = None


def who(request: Request) -> str:
    """Who's asking: clients send X-Ferment-User (attribution, not auth: anyone
    who can reach the server already got through ssh with a trusted key)."""
    return request.headers.get(WHO_HEADER, "unknown")


def create_app(controller: TimeController, title: str = "fermenter", database: Database | None = None) -> FastAPI:
    store = controller.store

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
    async def checkpoint_error(_request: Request, exc: CheckpointError):
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
        return controller.set_time(req.at, who=who(request))

    @app.get("/checkpoints")
    def checkpoints(limit: int = 50):
        return store.checkpoints(limit)

    @app.post("/checkpoints")
    def checkpoint(req: NewCheckpoint, request: Request):
        return controller.checkpoint(req.message, req.name, who=who(request))

    @app.post("/checkpoints/{ref}/restore")
    def restore(ref: str, request: Request):
        return controller.restore(ref, who=who(request))

    @app.get("/events")
    def events(limit: int = 50):
        return store.events(limit)

    @app.get("/sessions")
    def sessions(include_gone: bool = False):
        return store.sessions(include_gone)

    @app.post("/sessions")
    def open_session(req: NewSession, request: Request):
        return {"id": store.open_session(who(request), req.client, req.relays)}

    @app.post("/sessions/{session_id}/heartbeat")
    def heartbeat(session_id: int):
        if not store.touch_session(session_id):
            raise HTTPException(404, "no open session with that id")
        return {"ok": True}

    @app.delete("/sessions/{session_id}")
    def close_session(session_id: int):
        store.close_session(session_id)
        return {"ok": True}

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
