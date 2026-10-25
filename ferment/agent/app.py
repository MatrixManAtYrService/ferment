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
GET    /volume             the data volume (ferment.volume): its snapshots, what it takes on disk
POST   /volume/snapshots/{id}           snapshot the database's data dir (a checkpoint)
POST   /volume/snapshots/{id}/restore   put it back (workload stopped)
DELETE /volume/snapshots/{id}           drop one
GET    /db/changes         what the binlog has between start and stop (file:position; ferment.binlog)
GET    /probes             clock probe readings (ferment.clockcheck), `since` a fake time
GET    /artifacts/{sha256}/{filename}   {path, exists}: where a built artifact is (or would be) kept
PUT    /artifacts/{sha256}/{filename}   upload it (body: the file); checked against its hash

Blocking handlers are plain `def`s (FastAPI runs them in a thread); one lock
serializes everything that starts or stops processes.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import re
import threading
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from ferment.clockcheck import read_probes
from ferment.databases import MySQL
from ferment.procompose import Process, WorkloadSupervisor
from ferment.server.clock import boot_id, sudo_date
from ferment.volume import Volume, VolumeError


class SetClock(BaseModel):
    at: dt.datetime


class Launch(BaseModel):
    processes: list[dict]


SHA256 = re.compile(r"^[0-9a-f]{64}$")
FILENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,200}$")


def create_agent_app(
    supervisor: WorkloadSupervisor,
    *,
    database: MySQL | None = None,
    volume: Volume | None = None,
    probes: Path | None = None,
    artifacts: Path | None = None,
    set_system_time: Callable[[dt.datetime], None] = sudo_date,
    system_now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC),
    current_boot: Callable[[], str] = boot_id,
) -> FastAPI:
    lock = threading.Lock()
    offline = contextlib.ExitStack()
    offline_port: list[int] = []  # the offline mysqld's port while it runs

    # The data volume is made ready before any workload runs: the data dir
    # moves onto it once (a copy: minutes for a big one), and a reboot left
    # it unmounted. In the background, so /health answers meanwhile; whatever
    # needs the data waits for it.
    volume_ready = threading.Event()
    volume_error: list[str] = []

    def prepare_volume() -> None:
        try:
            for what in volume.ensure(workload_running=supervisor.running):
                print(f"volume: {what}", flush=True)
        except Exception as e:
            volume_error.append(str(e))
            print(f"volume: {e}", flush=True)
        finally:
            volume_ready.set()

    def await_volume() -> None:
        if volume is None:
            return
        if not volume_ready.wait(3600):
            raise HTTPException(503, "the data volume is still being prepared")
        if volume_error:
            raise HTTPException(500, f"the data volume isn't usable: {volume_error[0]}")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if volume is not None:
            threading.Thread(target=prepare_volume, daemon=True).start()
        yield
        with lock:
            offline.close()
            supervisor.stop()

    app = FastAPI(title="ferment workload agent", lifespan=lifespan)

    def clock() -> dict:
        return {"now": system_now().isoformat(), "boot_id": current_boot()}

    @app.get("/health")
    def health():
        out: dict = {"healthy": True}
        if volume is not None:
            out["volume"] = "error" if volume_error else "ready" if volume_ready.is_set() else "preparing"
        return out

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
        await_volume()
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
        await_volume()
        with lock:
            if supervisor.running:
                raise HTTPException(409, "the workload is running; stop it first")
            if not offline_port:
                offline_port.append(offline.enter_context(database.offline_server()))
            return {"port": offline_port[0]}

    def _volume() -> Volume:
        if volume is None:
            raise HTTPException(404, "this workload keeps no data volume")
        await_volume()
        return volume

    @app.get("/volume")
    def get_volume():
        return _volume().usage()

    @app.post("/volume/snapshots/{sid}")
    def snapshot(sid: str):
        v = _volume()
        binlog = None
        try:
            if database is not None:
                binlog = database.before_snapshot() if supervisor.running else database.binlog_position()
        except Exception as e:  # warmth and the binlog coordinate are nice to have, the snapshot isn't
            print(f"volume: before snapshot {sid}: {e}", flush=True)
        try:
            return {"id": v.snapshot(sid), "binlog": binlog}
        except VolumeError as e:
            raise HTTPException(409, str(e))

    @app.post("/volume/snapshots/{sid}/restore")
    def restore(sid: str):
        with lock:
            if supervisor.running:
                raise HTTPException(409, "the workload is running; stop it first")
            try:
                _volume().restore(sid)
            except VolumeError as e:
                raise HTTPException(409, str(e))
        return {"ok": True}

    @app.delete("/volume/snapshots/{sid}")
    def drop(sid: str):
        try:
            _volume().delete(sid)
        except VolumeError as e:
            raise HTTPException(409, str(e))
        return {"ok": True}

    @app.get("/db/changes")
    def db_changes(start: str, stop: str | None = None):
        if database is None:
            raise HTTPException(404, "this workload has no database the agent manages")
        from ferment.binlog import summarize

        valid = re.compile(r"^[A-Za-z0-9_.-]+:\d+$")
        if not valid.match(start) or (stop is not None and not valid.match(stop)):
            raise HTTPException(422, "positions are file:offset")
        try:
            tables = summarize(database.binlog_lines(start, stop), database.columns)
        except LookupError as e:
            raise HTTPException(404, str(e))
        return {"start": start, "stop": stop, "tables": tables}

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

    # Built artifacts (jars, ...) come from the control VM, which builds them;
    # kept by content hash, so a re-deploy of the same build sends nothing.

    def artifact_path(sha256: str, filename: str) -> Path:
        if artifacts is None:
            raise HTTPException(404, "this agent doesn't keep artifacts")
        if not SHA256.match(sha256) or not FILENAME.match(filename):
            raise HTTPException(422, "expected /artifacts/<sha256 hex>/<plain file name>")
        return artifacts / sha256 / filename

    @app.get("/artifacts/{sha256}/{filename}")
    def artifact(sha256: str, filename: str):
        path = artifact_path(sha256, filename)
        return {"path": str(path), "exists": path.is_file()}

    @app.put("/artifacts/{sha256}/{filename}")
    async def upload_artifact(sha256: str, filename: str, request: Request):
        path = artifact_path(sha256, filename)
        if path.is_file():
            return {"path": str(path), "exists": True}
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{filename}.part")
        digest = hashlib.sha256()
        with tmp.open("wb") as f:
            async for chunk in request.stream():
                digest.update(chunk)
                f.write(chunk)
        if digest.hexdigest() != sha256:
            tmp.unlink()
            raise HTTPException(422, f"upload doesn't match its hash (got {digest.hexdigest()})")
        tmp.replace(path)
        return {"path": str(path), "exists": True}

    return app
