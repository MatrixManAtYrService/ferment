"""Checkpoints as filesystem snapshots of the workload database's data dir.

Dumping a database at every checkpoint (ferment.archive) costs time in
proportion to its size: fine for an empty stack, an hour a checkpoint for a
prod load. Instead the data dir lives on a btrfs filesystem, and:

- a checkpoint is a read-only snapshot of it: instant, and copy-on-write, so
  it takes only the space of what changes after it;
- a restore swaps a writable snapshot of a checkpoint back in (database
  stopped): instant too, then the database starts as usual.

A snapshot taken with the database running is what a crash would have left,
which InnoDB recovers from (its logs are on the same filesystem, snapshotted
in the same instant). Clock changes and restores stop the workload anyway.

The filesystem is an image file on the VM's own disk, loop-mounted at
`<state>/volume` (sparse: it takes what's written), with subvolumes
`live` (the data dir; `<data dir>` becomes a link to it) and
`snapshots/<id>`. `Volume` runs where the database does (the workload
agent); `VolumeSnapshots` is the checkpoint store on the server's side,
calling it through the agent.

What this doesn't give is diffs between checkpoints (`fmnt db diff`): that's
the Dolt archive's job, for small stacks or, later, for chosen tables.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from collections.abc import Callable
from pathlib import Path

SNAPSHOT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
PART = "volume:data"


class VolumeError(RuntimeError):
    pass


def _sh(argv: list[str]) -> str:
    p = subprocess.run(argv, capture_output=True, text=True)
    if p.returncode != 0:
        raise VolumeError(f"{' '.join(argv)}: {(p.stderr or p.stdout).strip()}")
    return p.stdout


class Volume:
    def __init__(
        self,
        state: Path,
        data_dir: Path,
        btrfs_bin: str | None = None,
        run: Callable[[list[str]], str] = _sh,
        size_bytes: Callable[[], int] | None = None,
    ):
        """`data_dir`: the database's (moved onto the volume by `ensure`).
        `btrfs_bin`: btrfs-progs' bin dir (FERMENT_BTRFS_BIN): by path,
        because sudo resets PATH."""
        self.state = state
        self.data_dir = data_dir
        self.bin = btrfs_bin or os.environ.get("FERMENT_BTRFS_BIN", "")
        self.run = run
        self.image = state / "volume.btrfs"
        self.mount = state / "volume"
        self.live = self.mount / "live"
        self.snapshots = self.mount / "snapshots"
        # The image may grow to the size of the disk it's on (it's sparse).
        self.size_bytes = size_bytes or (lambda: os.statvfs(state).f_blocks * os.statvfs(state).f_frsize)

    def _btrfs(self, *args: str) -> str:
        return self.run(["sudo", "-n", f"{self.bin}/btrfs" if self.bin else "btrfs", *args])

    def mounted(self) -> bool:
        return self.mount.is_mount()

    def ensure(self, database_running: bool = False) -> list[str]:
        """Make the volume ready and the data dir live on it; what it did.

        Idempotent (and re-run after a reboot, which unmounts it). Moving an
        existing data dir onto it copies it once, so the database mustn't be
        running then."""
        done = []
        if not self.image.exists():
            self.state.mkdir(parents=True, exist_ok=True)
            self.run(["truncate", "-s", str(self.size_bytes()), str(self.image)])
            mkfs = f"{self.bin}/mkfs.btrfs" if self.bin else "mkfs.btrfs"
            self.run([mkfs, "-q", str(self.image)])
            done.append(f"made {self.image}")
        if not self.mounted():
            self.mount.mkdir(parents=True, exist_ok=True)
            self.run(["sudo", "-n", "mount", "-o", "loop,noatime", str(self.image), str(self.mount)])
            self.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(self.mount)])
            done.append(f"mounted it at {self.mount}")
        if not self.live.exists():
            self._btrfs("subvolume", "create", str(self.live))
            self.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(self.live)])
            done.append("made the live subvolume")
        self.snapshots.mkdir(exist_ok=True)
        if self.data_dir.is_symlink():
            if Path(os.readlink(self.data_dir)) != self.live:
                raise VolumeError(f"{self.data_dir} links to {os.readlink(self.data_dir)}, not {self.live}")
            return done
        if self.data_dir.exists():
            if database_running:
                raise VolumeError(f"{self.data_dir} needs moving onto the volume, but the database is running")
            # Copied, not moved in place: a different filesystem. Once, ever.
            self.run(["cp", "-a", f"{self.data_dir}/.", f"{self.live}/"])
            shutil.rmtree(self.data_dir)
            done.append(f"moved {self.data_dir} onto it")
        self.data_dir.symlink_to(self.live)
        return done

    def _snapshot_path(self, sid: str) -> Path:
        if not SNAPSHOT_ID.match(sid):
            raise VolumeError(f"bad snapshot id {sid!r}")
        return self.snapshots / sid

    def snapshot(self, sid: str) -> str:
        path = self._snapshot_path(sid)
        if path.exists():
            raise VolumeError(f"snapshot {sid} exists")
        self._btrfs("subvolume", "snapshot", "-r", str(self.live), str(path))
        return sid

    def restore(self, sid: str) -> None:
        """Make the live data what snapshot `sid` holds (database stopped)."""
        path = self._snapshot_path(sid)
        if not path.exists():
            raise VolumeError(f"no snapshot {sid}")
        self._btrfs("subvolume", "delete", str(self.live))
        self._btrfs("subvolume", "snapshot", str(path), str(self.live))

    def delete(self, sid: str) -> None:
        path = self._snapshot_path(sid)
        if path.exists():
            self._btrfs("subvolume", "delete", str(path))

    def list(self) -> list[str]:
        return sorted(p.name for p in self.snapshots.iterdir()) if self.snapshots.exists() else []

    def usage(self) -> dict:
        """What the volume takes on the VM's disk, and what the VM's disk has left."""
        st = os.stat(self.image) if self.image.exists() else None
        fs = os.statvfs(self.state)
        return {
            "image_bytes": st.st_blocks * 512 if st else 0,  # allocated, not apparent: it's sparse
            "disk_free_bytes": fs.f_bavail * fs.f_frsize,
            "snapshots": self.list(),
        }


class VolumeSnapshots:
    """A ferment.server.time_control.Versioned store: checkpoints as volume
    snapshots, taken through the workload agent (ferment.agent.client)."""

    def __init__(self, agent):
        self.agent = agent

    def snapshot(self, message: str, tags: list[str], *, running: bool) -> dict[str, str]:
        return {PART: self.agent.snapshot_volume(tags[0])}

    def restore(self, parts: dict[str, str]) -> None:
        if PART not in parts:
            raise VolumeError("that checkpoint has no volume snapshot (it was taken before snapshots were on)")
        self.agent.restore_volume(parts[PART])
