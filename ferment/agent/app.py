"""The workload agent's HTTP API (ferment.agent).

GET    /health
GET    /clock              {now, boot_id}: the machine's clock (the workload's time)
PUT    /clock              {at}: set it
GET    /workload           {running, processes}: each process with a `ready` verdict
PUT    /workload           {processes}: (re)start the workload as exactly these
DELETE /workload           stop it
GET    /logs/{name}        a process's output from byte `offset` on
POST   /db/offline         run mysqld on the live data dir (workload stopped): {port}
DELETE /db/offline         stop it
GET    /probes             clock probe readings (ferment.clockcheck), `since` a fake time

Blocking handlers are plain `def`s (FastAPI runs them in a thread); one lock
serializes everything that starts or stops processes.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ferment.clockcheck import read_probes
from ferment.databases import MySQL
from ferment.procompose import Process, WorkloadSupervisor
from ferment.server.clock import boot_id, sudo_date


class SetClock(BaseModel):
    at: dt.datetime


class Launch(BaseModel):
    processes: list[dict]


def create_agent_app(
    supervisor: WorkloadSupervisor,
    *,
    database: MySQL | None = None,
    probes: Path | None = None,
    set_system_time: Callable[[dt.datetime], None] = sudo_date,
    system_now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    current_boot: Callable[[], str] = boot_id,
) -> FastAPI:
    lock = threading.Lock()
    offline = contextlib.ExitStack()
    offline_port: list[int] = []  # the offline mysqld's port while it runs

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        yield
        with lock:
            offline.close()
            supervisor.stop()

    app = FastAPI(title="ferment workload agent", lifespan=lifespan)

    def clock() -> dict:
        return {"now": system_now().isoformat(), "boot_id": current_boot()}

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.get("/clock")
    def get_clock():
        return clock()

    @app.put("/clock")
    def put_clock(req: SetClock):
        if req.at.tzinfo is None:
            raise HTTPException(422, "give a timezone")
        set_system_time(req.at)
        return clock()

    @app.get("/workload")
    def workload():
        procs = supervisor.processes()
        return {
            "running": supervisor.running,
            "processes": [{**p, "ready": supervisor.is_ready(p)} for p in procs],
        }

    @app.put("/workload")
    def launch(req: Launch):
        with lock:
            supervisor.launch([Process(**p) for p in req.processes])
        return {"running": supervisor.running}

    @app.delete("/workload")
    def stop():
        with lock:
            supervisor.stop()
        return {"running": False}

    @app.get("/logs/{name}")
    def log(name: str, offset: int = 0):
        text, next_offset = supervisor.log(name, offset)
        return {"text": text, "offset": next_offset}

    @app.post("/db/offline")
    def start_offline():
        if database is None:
            raise HTTPException(404, "this workload has no database the agent manages")
        with lock:
            if supervisor.running:
                raise HTTPException(409, "the workload is running; stop it first")
            if not offline_port:
                offline_port.append(offline.enter_context(database.offline_server()))
            return {"port": offline_port[0]}

    @app.delete("/db/offline")
    def stop_offline():
        with lock:
            offline.close()
            offline_port.clear()
        return {"ok": True}

    @app.get("/probes")
    def get_probes(since: dt.datetime | None = None):
        if probes is None:
            return []
        return read_probes(probes, since=since)

    return app
