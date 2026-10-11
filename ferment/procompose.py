"""process-compose: one supervisor process as the parent of the whole workload.

`Process` is the small slice of process-compose's schema ferment needs;
`render` turns a set of them into a config; `WorkloadSupervisor` runs one
process-compose instance and talks to its REST API. It runs wherever the
workload does: under the workload agent (ferment.agent), which the fermenter
server drives through ferment.agent.client.AgentClient (same interface).
"""

from __future__ import annotations

import dataclasses
import os
import shlex
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import yaml


@dataclass
class Process:
    name: str
    command: str
    env: dict[str, str] = field(default_factory=dict)
    working_dir: str | None = None
    depends_on: list[str] = field(default_factory=list)  # wait for these to be healthy
    readiness_exec: str | None = None  # shell command; exit 0 = ready
    # How long a slow starter (a JVM service) gets before it counts as failing:
    # initial delay + period * failures.
    readiness_initial_delay: int = 1
    readiness_period: int = 2
    readiness_failures: int = 60
    restart: str = "on_failure"

    def to_pc(self) -> dict:
        entry: dict = {
            "command": self.command,
            "environment": [f"{k}={v}" for k, v in self.env.items()],
            "availability": {"restart": self.restart, "backoff_seconds": 2},
        }
        if self.working_dir:
            entry["working_dir"] = self.working_dir
        if self.depends_on:
            entry["depends_on"] = {d: {"condition": "process_healthy"} for d in self.depends_on}
        if self.readiness_exec:
            entry["readiness_probe"] = {
                "exec": {"command": self.readiness_exec},
                "initial_delay_seconds": self.readiness_initial_delay,
                "period_seconds": self.readiness_period,
                "failure_threshold": self.readiness_failures,
            }
        return entry

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)


def render(processes: list[Process], log_dir: Path) -> dict:
    """process-compose config. Each process also appends its output, as it
    happens, to <log_dir>/<name>.log (process-compose's own per-process log
    files are written in 8 KB chunks: useless for watching a service start)."""
    log_dir.mkdir(parents=True, exist_ok=True)
    processes_cfg = {}
    for p in processes:
        entry = p.to_pc()
        log = shlex.quote(str(log_dir / f"{p.name}.log"))
        entry["command"] = f"exec > >(tee -a {log}) 2>&1\n{entry['command']}"
        processes_cfg[p.name] = entry
    return {
        "version": "0.5",
        # Explicit: the log tee above needs bash.
        "shell": {"shell_command": "bash", "shell_argument": "-c"},
        "log_location": str(log_dir / "process-compose.log"),
        "processes": processes_cfg,
    }


class WorkloadSupervisor:
    """Owns one process-compose instance: start it with a config, stop it,
    ask it what's running."""

    def __init__(
        self, state_dir: Path, port: int, log_dir: Path | None = None, process_compose: str = "process-compose"
    ):
        self.state_dir = state_dir
        self.port = port
        self.log_dir = log_dir or state_dir / "logs"
        self.binary = process_compose
        self.proc: subprocess.Popen | None = None
        self.probed: set[str] = set()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def launch(self, processes: list[Process]) -> None:
        """(Re)start the workload as exactly these processes."""
        self.start(render(processes, self.log_dir))

    def start(self, config: dict) -> None:
        self.stop()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = self.state_dir / "workload.yaml"
        cfg_path.write_text(yaml.safe_dump(config, sort_keys=False))
        # Processes that only count as ready once their probe says so.
        self.probed = {n for n, p in config.get("processes", {}).items() if "readiness_probe" in p}
        log = (self.state_dir / "supervisor.log").open("ab")
        self.proc = subprocess.Popen(
            [self.binary, "up", "-f", str(cfg_path), "--tui=false", "--port", str(self.port)],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,  # its own process group, so stop() reaps the whole tree
        )

    def stop(self, timeout: float = 60.0) -> None:
        if self.proc is None or self.proc.poll() is not None:
            self.proc = None
            return
        # process-compose shuts its children down on SIGTERM.
        os.killpg(self.proc.pid, signal.SIGTERM)
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
        self.proc = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def processes(self) -> list[dict]:
        if not self.running:
            return []
        try:
            data = httpx.get(f"{self.url}/processes", timeout=5).json()
        except httpx.HTTPError:
            return []
        return [
            {
                "name": p.get("name"),
                "status": p.get("status"),
                "is_ready": p.get("is_ready"),
                "restarts": p.get("restarts"),
                "pid": p.get("pid"),
            }
            for p in data.get("data", [])
        ]

    def log(self, name: str, offset: int = 0, limit: int = 1_000_000) -> tuple[str, int]:
        """A process's output from byte `offset` on (<log_dir>/<name>.log), and
        the offset to ask for next."""
        path = self.log_dir / f"{name}.log"
        if "/" in name or not path.exists():
            return "", offset
        offset = min(offset, path.stat().st_size)
        with path.open("rb") as f:
            f.seek(offset)
            data = f.read(limit)
        return data.decode(errors="replace"), offset + len(data)

    def is_ready(self, p: dict) -> bool:
        """One process (from `processes`): running, and ready if it has a probe.
        "-" means no probe, but also "probe hasn't run yet" (initial delay),
        so probed processes must say Ready."""
        return p["status"] == "Running" and (p["is_ready"] == "Ready" or p["name"] not in self.probed)

    def all_ready(self, procs: list[dict]) -> bool:
        return bool(procs) and all(self.is_ready(p) for p in procs)

    def wait_ready(self, timeout: float = 300.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.all_ready(self.processes()):
                return True
            time.sleep(1)
        return False
