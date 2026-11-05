"""The workload agent's HTTP API (ferment.agent).

GET    /health
GET    /clock              {now, boot_id}: the machine's clock (the workload's time)
PUT    /clock              {at}: set it
GET    /workload           {running, processes}: each process with a `ready` verdict
PUT    /workload           {processes}: (re)start the workload as exactly these
DELETE /workload           stop it
GET    /logs/{name}        a process's output from byte `offset` on
GET    /logtrees/{tree}    files under a log tree (e.g. Airflow's task logs) written lately; /file: one, from `offset`
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

import asyncio
import contextlib
import datetime as dt
import hashlib
import re
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ferment import measure
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


VOLUME_REFIT_S = 60.0


def create_agent_app(
    supervisor: WorkloadSupervisor,
    *,
    database: MySQL | None = None,
    volume: Volume | None = None,
    probes: Path | None = None,
    artifacts: Path | None = None,
    log_trees: dict[str, Path] | None = None,
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
        # The cap follows the disk: room freed (snapshots dropped) lets the volume
        # grow, room taken (logs) shrinks it if what's in it fits. A load that fills
        # the volume between refits fails; the next refit gives it the room back.
        while not volume_error:
            time.sleep(VOLUME_REFIT_S)
            try:
                for what in volume.fit():
                    print(f"volume: {what}", flush=True)
            except Exception as e:  # noqa: BLE001 - a refit that fails is retried next round
                print(f"volume: refit: {e}", flush=True)

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

    def _tree(tree: str) -> Path:
        root = (log_trees or {}).get(tree)
        if root is None:
            raise HTTPException(404, f"no log tree {tree!r}")
        return root

    @app.get("/logtrees/{tree}")
    def tree_files(tree: str, within_s: float = 86400):
        """The files under a log tree (e.g. Airflow's task logs) written in the
        last `within_s` seconds of this machine's clock, or later than now (the
        clock went back): {path (relative), size}."""
        root = _tree(tree)
        cutoff = system_now().timestamp() - within_s
        out = []
        for f in root.rglob("*") if root.is_dir() else []:
            with contextlib.suppress(OSError):
                st = f.stat()
                if f.is_file() and st.st_mtime >= cutoff:
                    out.append({"path": str(f.relative_to(root)), "size": st.st_size})
        return out

    @app.get("/logtrees/{tree}/file")
    def tree_file(tree: str, path: str, offset: int = 0):
        root = _tree(tree).resolve()
        f = (root / path).resolve()
        if not f.is_relative_to(root):
            raise HTTPException(400, "outside the tree")
        if not f.is_file():
            return {"text": "", "offset": offset}
        offset = min(offset, f.stat().st_size)
        with f.open("rb") as fh:
            fh.seek(offset)
            data = fh.read(1_000_000)
        return {"text": data.decode(errors="replace"), "offset": offset + len(data)}

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

    cpu = measure.CpuMeter()

    @app.get("/measure/now")
    def measure_now():
        """This VM now: CPU, memory, pressure, disks, and each workload process's tree's memory."""
        disks = {"root": Path("/")} | ({"volume": volume.mount} if volume is not None and volume.mounted() else {})
        return measure.collect(cpu, disks, supervisor.processes() if supervisor.running else [])

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

    @app.get("/volume/snapshots/{sid}/send")
    def send(sid: str, parent: str | None = None):
        """Snapshot `sid` as a stream (relative to `parent`): for the shelf."""
        try:
            return StreamingResponse(_volume().send(sid, parent), media_type="application/zstd")
        except VolumeError as e:
            raise HTTPException(404, str(e))

    @app.put("/volume/snapshots/{sid}/receive")
    async def receive(sid: str, request: Request, source: str):
        """Make snapshot `sid` again from a stream `send` gave; `source`: the
        snapshot's name where it was sent from."""
        try:
            r = _volume().receive(sid, source)
        except VolumeError as e:
            raise HTTPException(409, str(e))
        try:
            async for chunk in request.stream():
                await asyncio.to_thread(r.write, chunk)
            await asyncio.to_thread(r.close)
        except VolumeError as e:
            raise HTTPException(422, str(e))
        except BaseException:
            r.abort()
            raise
        return {"id": sid}

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
