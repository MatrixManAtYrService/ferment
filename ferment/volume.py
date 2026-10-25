"""Checkpoints as filesystem snapshots of the workload's data.

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
`live` and `snapshots/<id>`. `live` holds every directory of state the
workload keeps (the database's data dir, a scheduler's metadata: `dirs`),
each at `live/<name>` with its usual path a link there, so one snapshot cuts
them all in the same instant. `Volume` runs where the database does (the workload
agent); `VolumeSnapshots` is the checkpoint store on the server's side,
calling it through the agent.

What this doesn't give yet is diffs between checkpoints. They can come from
the snapshots themselves: a mysqld on a writable copy of each, CHECKSUM
TABLE to find the tables that differ, then a keyed row diff of just those.
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
BINLOG_PART = "volume:binlog"  # where the binlog was at the snapshot: file:position


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
        dirs: dict[str, Path],
        btrfs_bin: str | None = None,
        run: Callable[[list[str]], str] = _sh,
        size_bytes: Callable[[], int] | None = None,
    ):
        """`dirs`: name -> where the workload expects it (moved onto the volume
        by `ensure`; the first is the database's data dir). `btrfs_bin`:
        btrfs-progs' bin dir (FERMENT_BTRFS_BIN): by path, because sudo resets
        PATH."""
        self.state = state
        self.dirs = dirs
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

    def _relayout(self) -> bool:
        """Snapshots from before `dirs` had the first one's contents at the root
        of `live`: move them into `live/<first>` (renames: instant)."""
        first = next(iter(self.dirs))
        loose = [p for p in self.live.iterdir() if p.name not in self.dirs]
        if (self.live / first).exists() or not loose:
            return False
        (self.live / first).mkdir()
        for p in loose:
            p.rename(self.live / first / p.name)
        return True

    def ensure(self, workload_running: bool = False) -> list[str]:
        """Make the volume ready and every dir live on it; what it did.

        Idempotent (and re-run after a reboot, which unmounts it). Moving an
        existing dir onto it copies it once, so the workload mustn't be
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
        if self._relayout():
            done.append(f"moved the old layout into live/{next(iter(self.dirs))}")
        for name, path in self.dirs.items():
            target = self.live / name
            if path.is_symlink():
                linked = Path(os.readlink(path))
                if linked == target:
                    continue
                if linked != self.live:  # the old layout's link is the one we may replace
                    raise VolumeError(f"{path} links to {linked}, not {target}")
                path.unlink()
            elif path.exists():
                if workload_running:
                    raise VolumeError(f"{path} needs moving onto the volume, but the workload is running")
                # Copied, not moved in place: a different filesystem. Once, ever.
                target.mkdir(exist_ok=True)
                self.run(["cp", "-a", f"{path}/.", f"{target}/"])
                shutil.rmtree(path)
                done.append(f"moved {path} onto it")
            target.mkdir(exist_ok=True)
            path.symlink_to(target)
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
        self._relayout()  # a checkpoint from before `dirs`
        for name in self.dirs:  # ...or from before one of them was kept here
            (self.live / name).mkdir(exist_ok=True)

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
        """The snapshot, and where the binlog was: the rows that changed between
        two checkpoints on one lineage are the binlog between their positions."""
        snap = self.agent.snapshot_volume(tags[0])
        parts = {PART: snap["id"]}
        if snap.get("binlog"):
            parts[BINLOG_PART] = snap["binlog"]
        return parts

    def restore(self, parts: dict[str, str]) -> None:
        if PART not in parts:
            raise VolumeError("that checkpoint has no volume snapshot (it was taken before snapshots were on)")
        self.agent.restore_volume(parts[PART])
