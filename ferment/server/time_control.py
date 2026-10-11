"""Server-side time control: the workload runs at the VM's clock, and changing
the date means stopping the workload, setting the guest's clock, and starting
the workload again.

It also owns checkpoints: one external checkpoint is a snapshot of every
versioned store (each its own internal commit), recorded in the Store. A
clock change checkpoints automatically, while the workload is stopped, so
every store is cut at the same instant. Restoring one puts the data back and
the clock back to the checkpoint's date.
"""

from __future__ import annotations

import datetime as dt
import re
import threading
from pathlib import Path
from typing import Protocol

from ferment.procompose import Process, WorkloadSupervisor, render
from ferment.server import clock as clock_mod
from ferment.server.clock import GuestClock
from ferment.server.store import Store

CHECKPOINT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")


class CheckpointError(ValueError):
    pass


class Versioned(Protocol):
    """A store that can be one part of a checkpoint (e.g. ferment.archive.DoltArchive)."""

    def snapshot(self, message: str, tags: list[str], *, running: bool) -> dict[str, str]: ...

    def restore(self, parts: dict[str, str]) -> None: ...


class Stack(Protocol):
    """What a concrete fermenter provides: the processes of its workload."""

    def processes(self) -> list[Process]: ...


class TimeController:
    def __init__(
        self,
        stack: Stack,
        supervisor: WorkloadSupervisor,
        state_dir: Path,
        *,
        versioned: list[Versioned] | None = None,
        store: Store | None = None,
        clock: GuestClock | None = None,
    ):
        self.stack = stack
        self.versioned = versioned or []
        self.clock = clock or GuestClock(state_dir / "time.json")
        clock_mod.use(self.clock)
        self.store = store or Store(state_dir / "fermenter.db")
        self.supervisor = supervisor
        self.state_dir = state_dir
        self.lock = threading.Lock()
        # When the current workload was launched (by resume or a date
        # change). Anything the workload wrote before this is from its
        # predecessor.
        self.launched_at: dt.datetime | None = None

    def _launch(self) -> None:
        config = render(self.stack.processes(), self.state_dir / "logs")
        self.launched_at = self.clock.now()
        self.supervisor.start(config)

    def resume(self) -> None:
        """On server start: bring the workload back on the clock it had (after
        a VM reboot, the guest is on real time again: put the date back)."""
        with self.lock:
            self.clock.ensure()
            self._launch()

    def _checkpoint(self, *, message: str, kind: str, who: str, name: str | None, running: bool) -> dict:
        """Snapshot every versioned store and record it. Caller holds the lock."""
        if name is not None:
            if not CHECKPOINT_NAME.match(name):
                raise CheckpointError(f"bad checkpoint name {name!r}: start with a letter; letters, digits, . _ -")
            if self.store.name_taken(name):
                raise CheckpointError(f"a checkpoint named {name!r} already exists")
        cid = self.store.next_checkpoint_id()
        tags = [f"ckpt-{cid}"] + ([name] if name else [])
        parts: dict[str, str] = {}
        for store in self.versioned:
            parts.update(store.snapshot(message, tags, running=running))
        return self.store.add_checkpoint(
            name=name,
            message=message,
            kind=kind,
            who=who,
            fake_time=self.clock.now().isoformat(),
            offset_seconds=round(self.clock.offset.total_seconds()),
            parts=parts,
        )

    def set_time(self, target: dt.datetime, wait_ready: bool = True, who: str = "unknown") -> dict:
        """Restart the workload with the clock reading `target`; first
        checkpoint where it was (if anything is versioned)."""
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt.UTC)
        checkpoint = None
        with self.lock:
            self.supervisor.stop()
            if self.versioned:
                try:
                    checkpoint = self._checkpoint(
                        message=f"before clock change to {target.isoformat()}",
                        kind="clock-change",
                        who=who,
                        name=None,
                        running=False,
                    )
                except Exception:
                    self._launch()  # don't leave the workload down
                    raise
            self.clock.set(target)
            self._launch()
            ready = self.supervisor.wait_ready() if wait_ready else None
        return {**self.status(), "ready": ready, "checkpoint": checkpoint}

    def checkpoint(self, message: str, name: str | None = None, who: str = "unknown") -> dict:
        """Checkpoint now, without stopping the workload."""
        with self.lock:
            return self._checkpoint(message=message, kind="manual", who=who, name=name, running=self.supervisor.running)

    def restore(self, ref: str, wait_ready: bool = True, who: str = "unknown") -> dict:
        """Put every versioned store back to checkpoint `ref` (id or name) and
        the clock back to the date it was taken at. The state being left is
        checkpointed first, so a restore can itself be undone."""
        cp = self.store.checkpoint(ref)
        if cp is None:
            raise CheckpointError(f"no checkpoint {ref!r}")
        if not cp["parts"]:
            raise CheckpointError(f"checkpoint {ref!r} has no versioned data to restore (only a clock reading)")
        with self.lock:
            self.supervisor.stop()
            left = self._checkpoint(
                message=f"before restoring checkpoint {cp['id']}", kind="before-restore", who=who, name=None,
                running=False,
            )  # fmt: skip
            for store in self.versioned:
                store.restore(cp["parts"])
            self.clock.set(dt.datetime.fromisoformat(cp["fake_time"]))
            self._launch()
            ready = self.supervisor.wait_ready() if wait_ready else None
        return {**self.status(), "ready": ready, "restored": cp, "previous": left}

    def relaunch(self, wait_ready: bool = True, timeout: float = 600.0) -> dict:
        """Restart the workload on the same clock (e.g. after the set of
        deployed services changed)."""
        with self.lock:
            self.supervisor.stop()
            self._launch()
            ready = self.supervisor.wait_ready(timeout) if wait_ready else None
        return {**self.status(), "ready": ready}

    def stop(self) -> None:
        with self.lock:
            self.supervisor.stop()

    def status(self) -> dict:
        return {
            "real_now": self.clock.real_now().isoformat(),
            "fake_now": self.clock.now().isoformat(),
            "offset_seconds": round(self.clock.offset.total_seconds()),
            "set_to": self.clock.set_to,
            "set_at": self.clock.set_at,
            "launched_at": self.launched_at.isoformat() if self.launched_at else None,
            "workload": self.supervisor.processes(),
        }
