"""Server-side time control: the workload runs under one fake clock, and
changing the date means restarting the whole workload tree with a new one.

The server that owns this runs on real time, outside the faked tree, so it
survives the restart it performs (as does sshd, so attached clients keep
their tunnel).
"""

from __future__ import annotations

import datetime as dt
import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ferment.faketime import FakeClock
from ferment.procompose import Process, WorkloadSupervisor, render


class Stack(Protocol):
    """What a concrete fermenter provides: the processes of its workload."""

    def processes(self) -> list[Process]: ...


@dataclass
class TimeState:
    offset_seconds: int
    set_to: str  # the fake datetime requested
    set_at: str  # real time when it was requested

    @property
    def clock(self) -> FakeClock:
        return FakeClock(self.offset_seconds)


class TimeController:
    def __init__(self, stack: Stack, supervisor: WorkloadSupervisor, libfaketime: str, state_dir: Path):
        self.stack = stack
        self.supervisor = supervisor
        self.libfaketime = libfaketime
        self.state_dir = state_dir
        self.state_file = state_dir / "time.json"
        self.lock = threading.Lock()
        self.state: TimeState | None = None
        # Real time the current workload was launched (by resume or a date
        # change). Anything the workload wrote before this is from its
        # predecessor.
        self.launched_at: dt.datetime | None = None
        if self.state_file.exists():
            self.state = TimeState(**json.loads(self.state_file.read_text()))

    def _launch(self, clock: FakeClock) -> None:
        config = render(self.stack.processes(), clock.env(self.libfaketime), self.state_dir / "logs")
        self.launched_at = dt.datetime.now(dt.UTC)
        self.supervisor.start(config)

    def resume(self) -> None:
        """On server start: bring the workload back with the clock it had.

        The offset persists, so fake time keeps advancing in step with real
        time, including across a server or VM restart.
        """
        if self.state is not None:
            with self.lock:
                self._launch(self.state.clock)

    def set_time(self, target: dt.datetime, wait_ready: bool = True) -> dict:
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt.UTC)
        with self.lock:
            self.supervisor.stop()
            now = dt.datetime.now(dt.UTC)
            clock = FakeClock.starting_at(target, now=now)
            self.state = TimeState(clock.offset_seconds, target.isoformat(), now.isoformat())
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self.state_file.write_text(json.dumps(self.state.__dict__, indent=2))
            self._launch(clock)
            ready = self.supervisor.wait_ready() if wait_ready else None
        return {**self.status(), "ready": ready}

    def stop(self) -> None:
        with self.lock:
            self.supervisor.stop()

    def status(self) -> dict:
        real = dt.datetime.now(dt.UTC)
        return {
            "real_now": real.isoformat(),
            "fake_now": self.state.clock.now().isoformat() if self.state else None,
            "offset_seconds": self.state.offset_seconds if self.state else None,
            "set_to": self.state.set_to if self.state else None,
            "set_at": self.state.set_at if self.state else None,
            "launched_at": self.launched_at.isoformat() if self.launched_at else None,
            "workload": self.supervisor.processes(),
        }
