"""The generic part of a fermenter server: health, status, and the clock.

A concrete fermenter builds on `create_app` and adds its own routes.
"""

from __future__ import annotations

import datetime as dt
import shlex
import subprocess
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ferment.databases import Database, Dolt
from ferment.server.time_control import TimeController


class SetTime(BaseModel):
    at: dt.datetime


class Checkpoint(BaseModel):
    message: str
    tag: str | None = None


class Restore(BaseModel):
    ref: str


class Sql(BaseModel):
    sql: str
    database: str | None = None


def create_app(controller: TimeController, title: str = "fermenter", database: Database | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        controller.resume()
        yield
        controller.stop()

    app = FastAPI(title=title, lifespan=lifespan)
    app.state.controller = controller

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.get("/status")
    def status():
        return controller.status()

    # Plain `def`: the restart blocks (stop, relaunch, wait for readiness), so
    # FastAPI runs it in a worker thread. The controller's lock serializes
    # concurrent requests from multiple attached clients.
    @app.post("/time")
    def set_time(req: SetTime):
        if req.at.tzinfo is None:
            raise HTTPException(422, "give a timezone, e.g. 2026-10-03T00:00:00Z")
        return controller.set_time(req.at)

    if database is not None:
        _mount_database(app, controller, database)

    return app


def _mount_database(app: FastAPI, controller: TimeController, database: Database) -> None:
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

    if not isinstance(database, Dolt):
        return

    @app.get("/db/checkpoints")
    def db_log(limit: int = 20):
        return database.log(limit)

    # Writes take the controller's lock: never commit or reset in the middle
    # of a workload restart.
    @app.post("/db/checkpoints")
    def db_checkpoint(req: Checkpoint):
        with controller.lock:
            return database.checkpoint(req.message, req.tag)

    @app.get("/db/diff")
    def db_diff(from_ref: str, to_ref: str = "WORKING"):
        return database.diff(from_ref, to_ref)

    @app.post("/db/restore")
    def db_restore(req: Restore):
        with controller.lock:
            return database.restore(req.ref)
