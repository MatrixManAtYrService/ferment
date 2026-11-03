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
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from ferment.procompose import Process
from ferment.server import clock as clock_mod
from ferment.server.clock import GuestClock
from ferment.server.store import Store
from ferment.shelf import Shelf

CHECKPOINT_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{0,63}$")


class CheckpointError(ValueError):
    pass


class Versioned(Protocol):
    """A store that can be one part of a checkpoint (e.g. ferment.volume.VolumeSnapshots)."""

    def snapshot(self, message: str, tags: list[str], *, running: bool) -> dict[str, str]: ...

    def restore(self, parts: dict[str, str]) -> None: ...

    # Optional: `drop(parts)` frees a checkpoint's part for good; for the shelf,
    # `archive_name` (its object's name), `send(parts, base_parts)` (its data as
    # a stream, relative to an earlier checkpoint's) and `receive(parts, chunks)`
    # (made again from one).


# Told each step of a clock change, restore, or relaunch as it finishes:
# stop, checkpoint, restore, clock, start, ready (only those that apply).
Step = Callable[[str], None]


def _nothing(_step: str) -> None:
    pass


class Stack(Protocol):
    """What a concrete fermenter provides: the processes of its workload."""

    def processes(self) -> list[Process]: ...


class Supervisor(Protocol):
    """Runs the workload: ferment.procompose.WorkloadSupervisor in-process, or
    ferment.agent.client.AgentClient for one run by the workload agent."""

    running: bool

    def launch(self, processes: list[Process]) -> None: ...

    def stop(self) -> None: ...

    def processes(self) -> list[dict]: ...

    def is_ready(self, p: dict) -> bool: ...

    def all_ready(self, procs: list[dict]) -> bool: ...

    def wait_ready(self, timeout: float = 300.0) -> bool: ...

    def log(self, name: str, offset: int = 0) -> tuple[str, int]: ...


class TimeController:
    def __init__(
        self,
        stack: Stack,
        supervisor: Supervisor,
        state_dir: Path,
        *,
        versioned: list[Versioned] | None = None,
        store: Store | None = None,
        clock: GuestClock | None = None,
        shelf: Shelf | None = None,
        shelf_prefix: str = "snapshots",
        provenance: str = "unknown",
    ):
        """`shelf`: where checkpoints' data goes before it's dropped (under
        `shelf_prefix`, labelled `provenance`), so it can be made again."""
        self.stack = stack
        self.shelf, self.shelf_prefix, self.provenance = shelf, shelf_prefix, provenance
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
        self.launched_at = self.clock.now()
        self.supervisor.launch(self.stack.processes())

    def resume(self) -> None:
        """On server start: bring the workload back on the clock it had (after
        a VM reboot, the guest is on real time again: put the date back)."""
        with self.lock:
            self.clock.ensure()
            self._launch()

    def _checkpoint(
        self, *, message: str, kind: str, who: str, name: str | None, running: bool, stage: dict | None = None
    ) -> dict:
        """Snapshot every versioned store and record it. Caller holds the lock.
        `stage`: {stage, fingerprint, value}, for a checkpoint a named step made."""
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
            parent=self.store.head(),
            **(stage or {}),
        )

    def set_time(
        self, target: dt.datetime, wait_ready: bool = True, who: str = "unknown", step: Step = _nothing
    ) -> dict:
        """Restart the workload with the clock reading `target`; first
        checkpoint where it was (if anything is versioned)."""
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt.UTC)
        checkpoint = None
        with self.lock:
            self.supervisor.stop()
            step("stop")
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
                step("checkpoint")
            self.clock.set(target)
            step("clock")
            self._launch()
            step("start")
            ready = self._wait_ready(step) if wait_ready else None
        return {**self.status(), "ready": ready, "checkpoint": checkpoint}

    def checkpoint(
        self, message: str, name: str | None = None, who: str = "unknown", stage: dict | None = None
    ) -> dict:
        """Checkpoint now, without stopping the workload."""
        with self.lock:
            return self._checkpoint(
                message=message, kind="stage" if stage else "manual", who=who, name=name,
                running=self.supervisor.running, stage=stage,
            )  # fmt: skip

    def _present_ancestor(self, cp: dict) -> dict | None:
        """The nearest checkpoint `cp` descends from whose data is here: what
        it's archived relative to (the least that differs)."""
        parent = cp.get("parent")
        while parent is not None:
            p = self.store.checkpoint(str(parent))
            if p is None:
                return None
            if p["parts"]:
                return p
            parent = p.get("parent")
        return None

    def archive(self, ref: str) -> dict:
        """Put checkpoint `ref`'s data on the shelf (once): relative to the
        nearest ancestor that's here, so it's only what changed since."""
        with self.lock:
            return self._archive(self._get(ref))

    def _get(self, ref: str) -> dict:
        cp = self.store.checkpoint(ref)
        if cp is None:
            raise CheckpointError(f"no checkpoint {ref!r}")
        return cp

    def _archive(self, cp: dict) -> dict:
        """Caller holds the lock (nothing it's sent relative to may go meanwhile)."""
        if cp["archive"]:
            return cp["archive"]
        if self.shelf is None:
            raise CheckpointError("this fermenter has no shelf to archive to")
        if not cp["parts"]:
            raise CheckpointError(f"checkpoint {cp['id']}'s data was dropped before shelves: nothing to archive")
        base = self._present_ancestor(cp)
        prefix = f"{self.shelf_prefix}/{cp['id']}"
        about = {k: cp.get(k) for k in ("id", "name", "message", "kind", "fake_time", "parent", "stage", "fingerprint")}
        size = 0
        for store in self.versioned:
            send = getattr(store, "send", None)
            if send is None:
                continue
            meta = {
                "checkpoint": about,
                "parts": cp["parts"],
                "base": base and base["id"],
                "provenance": self.provenance,
            }
            key = f"{prefix}/{store.archive_name}"  # type: ignore[attr-defined]
            size += self.shelf.put(key, send(cp["parts"], base and base["parts"]), meta)["bytes"]
        return self.store.add_archive(
            cp["id"], shelf=self.shelf.url, prefix=prefix, base=base and base["id"], parts=cp["parts"], size=size
        )

    def _unarchive(self, cp: dict) -> dict:
        """Make a dropped checkpoint's data again from the shelf (its base first,
        if that was dropped too). Caller holds the lock."""
        archive = cp["archive"]
        if archive is None or self.shelf is None:
            raise CheckpointError(f"checkpoint {cp['id']} has no versioned data to restore (only a clock reading)")
        if archive["shelf"] != self.shelf.url:
            raise CheckpointError(f"checkpoint {cp['id']} is archived on {archive['shelf']}, not {self.shelf.url}")
        if archive["base"] is not None:
            base = self._get(str(archive["base"]))
            if not base["parts"]:
                self._unarchive(base)
        for store in self.versioned:
            receive = getattr(store, "receive", None)
            if receive is not None:
                receive(archive["parts"], self.shelf.get(f"{archive['prefix']}/{store.archive_name}"))  # type: ignore[attr-defined]
        self.store.set_parts(cp["id"], archive["parts"])
        return self._get(str(cp["id"]))

    def unarchive(self, ref: str) -> dict:
        """Bring a dropped checkpoint's data back from the shelf (without restoring it)."""
        with self.lock:
            cp = self._get(ref)
            return cp if cp["parts"] else self._unarchive(cp)

    def drop(self, ref: str) -> dict:
        """Free checkpoint `ref`'s data (a volume snapshot holds what's changed
        since, and they add up). Named and stage checkpoints are kept, and so is
        the one the workload is on. With a shelf, it's archived first and can
        be restored still; without one it stays in the tree, unrestorable."""
        cp = self.store.checkpoint(ref)
        if cp is None:
            raise CheckpointError(f"no checkpoint {ref!r}")
        if cp["name"] or cp.get("stage"):
            raise CheckpointError(f"checkpoint {cp['id']} is named or a stage's: keeping it")
        with self.lock:
            if cp["id"] == self.store.head():
                raise CheckpointError(f"checkpoint {cp['id']} is where the workload is: keeping it")
            if self.shelf is not None and cp["parts"]:
                cp = {**cp, "archive": self._archive(cp)}
            for store in self.versioned:
                drop = getattr(store, "drop", None)
                if drop is not None and cp["parts"]:
                    drop(cp["parts"])
            self.store.drop_parts(cp["id"])
        return {**cp, "parts": {}}

    def prune(self, kind: str = "before-restore", keep: int = 3) -> list[int]:
        """Drop every restorable checkpoint of `kind` but the newest `keep`
        (named, stage and current ones are kept anyway); the ids dropped."""
        cps = [c for c in self.store.checkpoints(limit=1_000_000) if c["kind"] == kind and c["parts"]]
        dropped = []
        for cp in cps[keep:]:  # newest first
            try:
                self.drop(str(cp["id"]))
            except CheckpointError:
                continue
            dropped.append(cp["id"])
        return dropped

    def restore(self, ref: str, wait_ready: bool = True, who: str = "unknown", step: Step = _nothing) -> dict:
        """Put every versioned store back to checkpoint `ref` (id or name) and
        the clock back to the date it was taken at. The state being left is
        checkpointed first, so a restore can itself be undone."""
        cp = self._get(ref)
        with self.lock:
            if not cp["parts"]:
                cp = self._unarchive(cp)  # from the shelf, or it says there's nothing to restore
            self.supervisor.stop()
            step("stop")
            left = self._checkpoint(
                message=f"before restoring checkpoint {cp['id']}", kind="before-restore", who=who, name=None,
                running=False,
            )  # fmt: skip
            step("checkpoint")
            for store in self.versioned:
                store.restore(cp["parts"])
            self.store.set_head(cp["id"])  # what's next descends from it: a branch
            step("restore")
            self.clock.set(dt.datetime.fromisoformat(cp["fake_time"]))
            step("clock")
            self._launch()
            step("start")
            ready = self._wait_ready(step) if wait_ready else None
        return {**self.status(), "ready": ready, "restored": cp, "previous": left}

    def relaunch(self, wait_ready: bool = True, timeout: float = 600.0, step: Step = _nothing) -> dict:
        """Restart the workload on the same clock (e.g. after the set of
        deployed services changed)."""
        with self.lock:
            self.supervisor.stop()
            step("stop")
            self._launch()
            step("start")
            ready = self._wait_ready(step, timeout) if wait_ready else None
        return {**self.status(), "ready": ready}

    def _wait_ready(self, step: Step, timeout: float = 300.0) -> bool:
        ready = self.supervisor.wait_ready(timeout)
        if ready:
            step("ready")
        return ready

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
            "real_time_checked": self.clock.checked,  # the last attached client's report, if any
            "launched_at": self.launched_at.isoformat() if self.launched_at else None,
            "workload": self.supervisor.processes(),
        }
