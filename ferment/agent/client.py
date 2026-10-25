"""The fermenter server's side of the workload agent (ferment.agent.app).

AgentClient has WorkloadSupervisor's interface, so the TimeController drives
a workload on another machine the same way it drove one in-process, and its
clock methods plug into GuestClock (set_system_time, system_now,
current_boot).

Reading the workload's clock is frequent (every event, every board refresh),
so `now` asks the agent only every so often and counts forward from the last
reading on this machine's monotonic clock in between: two clocks ticking at
the same rate, one of them at a fake date.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import subprocess
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ferment.procompose import Process
from ferment.vm.base import WORKLOAD_SSH_HOST


class AgentError(RuntimeError):
    pass


class AgentClient:
    RESAMPLE = 30.0  # seconds between clock readings from the agent

    def __init__(self, url: str, timeout: float = 600.0, connect_for: float = 60.0, http: httpx.Client | None = None):
        """`connect_for`: how long to keep retrying while the agent isn't up
        yet (it starts alongside the server). `http`: a ready-made client
        (tests pass the agent app's TestClient)."""
        self.http = http or httpx.Client(base_url=url, timeout=timeout)
        self.connect_for = connect_for
        # On this machine: it reads our files where they are (see `place`).
        self.local = urlsplit(url).hostname in ("127.0.0.1", "localhost")
        self._digests: dict[tuple[str, int, int], str] = {}
        self._copied: set[str] = set()  # store paths already on the workload's machine
        self._lock = threading.Lock()
        self._sample: tuple[dt.datetime, float, str] | None = None  # (agent now, our monotonic, boot id)

    def _request(self, method: str, path: str, **kw) -> dict | list:
        deadline = time.monotonic() + self.connect_for
        while True:
            try:
                r = self.http.request(method, path, **kw)
                break
            except httpx.ConnectError:
                if time.monotonic() > deadline:
                    raise AgentError(f"the workload agent at {self.http.base_url} isn't answering") from None
                time.sleep(0.5)
        if r.status_code >= 400:
            try:
                detail = r.json().get("detail", r.text)
            except ValueError:
                detail = r.text
            raise AgentError(f"agent {method} {path}: {r.status_code} {detail}")
        return r.json()

    # -- clock (for GuestClock) ------------------------------------------------------

    def _take(self, reading: dict) -> None:
        self._sample = (dt.datetime.fromisoformat(reading["now"]), time.monotonic(), reading["boot_id"])

    def _fresh(self) -> tuple[dt.datetime, float, str]:
        with self._lock:
            if self._sample is None or time.monotonic() - self._sample[1] > self.RESAMPLE:
                self._take(self._request("GET", "/clock"))
            return self._sample

    def now(self) -> dt.datetime:
        at, mono, _ = self._fresh()
        return at + dt.timedelta(seconds=time.monotonic() - mono)

    def set_clock(self, when: dt.datetime) -> None:
        reading = self._request("PUT", "/clock", json={"at": when.isoformat()})
        with self._lock:
            self._take(reading)

    def boot_id(self) -> str:
        with self._lock:
            self._take(self._request("GET", "/clock"))  # fresh: a reboot is what this detects
            return self._sample[2]

    # -- the workload (WorkloadSupervisor's interface) ----------------------------------

    def launch(self, processes: list[Process]) -> None:
        self._request("PUT", "/workload", json={"processes": [p.to_dict() for p in processes]})

    def stop(self) -> None:
        self._request("DELETE", "/workload")

    def _workload(self) -> dict:
        return self._request("GET", "/workload")

    @property
    def running(self) -> bool:
        return self._workload()["running"]

    def processes(self) -> list[dict]:
        try:
            return self._workload()["processes"]
        except AgentError:
            return []

    def is_ready(self, p: dict) -> bool:
        return bool(p.get("ready"))  # decided by the agent, which knows which processes have probes

    def all_ready(self, procs: list[dict]) -> bool:
        return bool(procs) and all(self.is_ready(p) for p in procs)

    def wait_ready(self, timeout: float = 300.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.all_ready(self.processes()):
                return True
            time.sleep(1)
        return False

    def log(self, name: str, offset: int = 0) -> tuple[str, int]:
        if "/" in name:
            return "", offset
        out = self._request("GET", f"/logs/{name}", params={"offset": offset})
        return out["text"], out["offset"]

    # -- what only the workload's machine can read -------------------------------------

    @contextlib.contextmanager
    def offline_db(self) -> Iterator[int]:
        """mysqld on the live data dir while the workload is stopped (e.g. to read a checkpoint's data)."""
        port = self._request("POST", "/db/offline")["port"]
        try:
            yield port
        finally:
            self._request("DELETE", "/db/offline")

    def snapshot_volume(self, sid: str) -> dict:
        """For ferment.volume.VolumeSnapshots: {id, binlog (file:position, or None)}."""
        return self._request("POST", f"/volume/snapshots/{sid}")

    def restore_volume(self, sid: str) -> None:
        self._request("POST", f"/volume/snapshots/{sid}/restore")

    def db_changes(self, start: str, stop: str | None = None) -> dict:
        params = {"start": start} | ({"stop": stop} if stop else {})
        return self._request("GET", "/db/changes", params=params)

    def volume(self) -> dict:
        return self._request("GET", "/volume")

    def place(self, local_path: str) -> str:
        """Where the workload's machine has this file (a built artifact),
        uploading it first if it lacks it. A nix store path is copied with its
        closure (`nix copy`, over the control VM's ssh to the workload VM) and
        has the same path there."""
        if self.local:
            return local_path
        if local_path.startswith("/nix/store/"):
            if local_path not in self._copied:
                subprocess.run(
                    ["nix", "--extra-experimental-features", "nix-command", "copy", "--no-check-sigs",
                     "--to", f"ssh-ng://{WORKLOAD_SSH_HOST}", local_path],
                    check=True, capture_output=True, text=True,
                )  # fmt: skip
                self._copied.add(local_path)
            return local_path
        path = Path(local_path)
        st = path.stat()
        key = (str(path), st.st_size, st.st_mtime_ns)
        if key not in self._digests:
            digest = hashlib.sha256()
            with path.open("rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    digest.update(chunk)
            self._digests[key] = digest.hexdigest()
        where = f"/artifacts/{self._digests[key]}/{path.name}"
        found = self._request("GET", where)
        if not found["exists"]:
            with path.open("rb") as f:
                found = self._request("PUT", where, content=iter(lambda: f.read(1 << 20), b""))
        return found["path"]

    def probes(self, since: dt.datetime | None = None) -> list[dict]:
        return self._request("GET", "/probes", params={"since": since.isoformat()} if since else None)
