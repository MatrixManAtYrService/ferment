"""process-compose: one supervisor process as the parent of the whole workload.

`Process` is the small slice of process-compose's schema ferment needs;
`render` turns a set of them into a config; `WorkloadSupervisor` runs one
process-compose instance and talks to its REST API.
"""

from __future__ import annotations

import os
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
    faked: bool = True  # run under the workload's fake clock
    restart: str = "on_failure"

    def to_pc(self, extra_env: dict[str, str]) -> dict:
        env = {**(extra_env if self.faked else {}), **self.env}
        entry: dict = {
            "command": self.command,
            "environment": [f"{k}={v}" for k, v in env.items()],
            "availability": {"restart": self.restart, "backoff_seconds": 2},
        }
        if self.working_dir:
            entry["working_dir"] = self.working_dir
        if self.depends_on:
            entry["depends_on"] = {d: {"condition": "process_healthy"} for d in self.depends_on}
        if self.readiness_exec:
            entry["readiness_probe"] = {
                "exec": {"command": self.readiness_exec},
                "initial_delay_seconds": 1,
                "period_seconds": 2,
                "failure_threshold": 60,
            }
        return entry


def render(processes: list[Process], fake_env: dict[str, str], log_dir: Path) -> dict:
    return {
        "version": "0.5",
        "log_location": str(log_dir / "process-compose.log"),
        "processes": {p.name: {**p.to_pc(fake_env), "log_location": str(log_dir / f"{p.name}.log")} for p in processes},
    }


class WorkloadSupervisor:
    """Owns one process-compose instance: start it with a config, stop it,
    ask it what's running."""

    def __init__(self, state_dir: Path, port: int, process_compose: str = "process-compose"):
        self.state_dir = state_dir
        self.port = port
        self.binary = process_compose
        self.proc: subprocess.Popen | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self, config: dict) -> None:
        self.stop()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = self.state_dir / "workload.yaml"
        cfg_path.write_text(yaml.safe_dump(config, sort_keys=False))
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

    def wait_ready(self, timeout: float = 300.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            procs = self.processes()
            if procs and all(p["status"] == "Running" and p["is_ready"] in ("Ready", "-") for p in procs):
                return True
            time.sleep(1)
        return False
