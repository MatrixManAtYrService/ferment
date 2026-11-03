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
`live` and `snapshots/<id>`. The image is capped so that, full, it still
leaves the VM's disk `reserve` bytes: a volume that fills fails the
workload's writes, but not the VM's (logs, the journal, the server). A
directory `scratch/` beside the subvolumes holds what needs room but no
snapshots (`scratch` dirs: the database's temp files), each linked from its
usual path like `dirs`. `live` holds every directory of state the
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

import contextlib
import os
import re
import shutil
import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path

GIB = 1 << 30
RESERVE = 15 * GIB  # what a full volume still leaves on the VM's disk
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
        disk: Callable[[], tuple[int, int]] | None = None,
        scratch: dict[str, Path] | None = None,
        reserve: int = RESERVE,
    ):
        """`dirs`: name -> where the workload expects it (moved onto the volume
        by `ensure`; the first is the database's data dir). `scratch`: the
        same, for dirs kept out of snapshots (their contents are dropped on the
        move). `btrfs_bin`: btrfs-progs' bin dir (FERMENT_BTRFS_BIN): by path,
        because sudo resets PATH. `disk`: the VM disk's (total, free) bytes."""
        self.state = state
        self.dirs = dirs
        self.scratch_dirs = scratch or {}
        self.reserve = reserve
        self.bin = btrfs_bin or os.environ.get("FERMENT_BTRFS_BIN", "")
        self.run = run
        self.image = state / "volume.btrfs"
        self.mount = state / "volume"
        self.live = self.mount / "live"
        self.snapshots = self.mount / "snapshots"
        self.scratch = self.mount / "scratch"
        self.disk = disk or (lambda: _disk(state))

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
            self.run(["truncate", "-s", str(self._cap()), str(self.image)])
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
        self.scratch.mkdir(exist_ok=True)
        if self._relayout():
            done.append(f"moved the old layout into live/{next(iter(self.dirs))}")
        done += self._fit()
        for name, path in self.scratch_dirs.items():
            target = self.scratch / name
            target.mkdir(exist_ok=True)
            if path.is_symlink() and Path(os.readlink(path)) == target:
                continue
            if workload_running and path.is_dir() and not path.is_symlink() and any(path.iterdir()):
                done.append(f"{path} moves onto it when the workload is stopped (in use now)")
                continue
            if path.is_symlink() or path.is_file():
                path.unlink()
            elif path.exists():
                shutil.rmtree(path)  # scratch: nothing in it outlives the process using it
            path.symlink_to(target)
            done.append(f"{path} is on it now (scratch: not snapshotted)")
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

    def fit(self) -> list[str]:
        """Refit the image to the disk now (it's only done by `ensure` otherwise): as
        the disk's free space changes (snapshots dropped, logs grown), so does the cap."""
        return self._fit() if self.mounted() and self.image.exists() else []

    def _cap(self) -> int:
        """The image's size: what it has, plus what the disk has left beyond
        the reserve (so it can never take the reserve), in whole GiB."""
        _, free = self.disk()
        allocated = os.stat(self.image).st_blocks * 512 if self.image.exists() else 0
        return max(0, allocated + free - self.reserve) // GIB * GIB

    def _fit(self) -> list[str]:
        """Resize the mounted image to its cap: smaller when the disk filled up
        around it (if what's in it fits), bigger when room came back."""
        size, cap = os.stat(self.image).st_size, self._cap()
        if abs(size - cap) < GIB:
            return []
        loop = self.run(["losetup", "-j", str(self.image)]).split(":", 1)[0]
        if not loop:
            return [f"not resized: no loop device for {self.image}"]
        try:
            if cap < size:  # the filesystem first, then the file under it
                self._btrfs("filesystem", "resize", str(cap), str(self.mount))
                self.run(["truncate", "-s", str(cap), str(self.image)])
                self.run(["sudo", "-n", "losetup", "-c", loop])
            else:  # the file first, then the filesystem over it
                self.run(["truncate", "-s", str(cap), str(self.image)])
                self.run(["sudo", "-n", "losetup", "-c", loop])
                self._btrfs("filesystem", "resize", "max", str(self.mount))
        except VolumeError as e:
            return [f"not resized to {cap // GIB} GiB (the disk keeps {self.reserve // GIB} GiB free): {e}"]
        return [f"resized from {size // GIB} to {cap // GIB} GiB (the disk keeps {self.reserve // GIB} GiB free)"]

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

    # -- to and from the shelf ---------------------------------------------------------

    def _zstd(self) -> str:
        return os.environ.get("FERMENT_ZSTD", "zstd")

    def send(self, sid: str, parent: str | None = None, chunk: int = 1 << 20) -> Iterator[bytes]:
        """Snapshot `sid` as a stream (`btrfs send`, zstd'd) that `receive` makes
        it again from: whole, or only what differs from snapshot `parent`, which
        the receiving volume must then have (it or one received from it).
        Fails before the first byte if either snapshot is missing; a failure
        partway ends the stream short, so the receiver sees it broken."""
        path = self._snapshot_path(sid)
        base = self._snapshot_path(parent) if parent else None
        for p in (path, base):
            if p is not None and not p.exists():
                raise VolumeError(f"no snapshot {p.name}")
        btrfs = f"{self.bin}/btrfs" if self.bin else "btrfs"
        argv = ["sudo", "-n", btrfs, "send", "-q", *(["-p", str(base)] if base else []), str(path)]
        return self._stream(argv, chunk)

    def _stream(self, argv: list[str], chunk: int) -> Iterator[bytes]:
        send = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        z = subprocess.Popen([self._zstd(), "-q", "-1", "-T0", "-c"], stdin=send.stdout, stdout=subprocess.PIPE)
        assert send.stdout is not None and z.stdout is not None and send.stderr is not None
        send.stdout.close()  # z holds it now; a dead z then stops send
        try:
            while b := z.stdout.read(chunk):
                yield b
        finally:
            z.stdout.close()
            z.wait()
            send.wait()
            err = send.stderr.read().decode().strip()
            send.stderr.close()
        if send.returncode or z.returncode:
            raise VolumeError(f"{' '.join(argv)}: {err or f'zstd exited {z.returncode}'}")

    def receive(self, sid: str) -> Receiver:
        """Where a stream from `send` goes to make snapshot `sid` here again
        (read-only, like any snapshot): write its chunks, then `close()`."""
        if self._snapshot_path(sid).exists():
            raise VolumeError(f"snapshot {sid} exists")
        btrfs = f"{self.bin}/btrfs" if self.bin else "btrfs"
        made = self._snapshot_path(sid)
        receive = ["sudo", "-n", btrfs, "receive", "-q", str(self.snapshots)]
        return Receiver(self._zstd(), receive, made, drop=lambda: self.delete(sid))

    def usage(self) -> dict:
        """What the volume takes on the VM's disk, what it may grow to, and what the VM's disk has left."""
        st = os.stat(self.image) if self.image.exists() else None
        return {
            "image_bytes": st.st_blocks * 512 if st else 0,  # allocated, not apparent: it's sparse
            "image_cap_bytes": st.st_size if st else 0,
            "disk_free_bytes": self.disk()[1],
            "snapshots": self.list(),
        }


class Receiver:
    """`zstd -d | btrfs receive`, fed a chunk at a time. A stream that ends
    short or fails leaves no snapshot behind."""

    def __init__(self, zstd: str, receive: list[str], made: Path, drop: Callable[[], None]):
        self.made, self.drop = made, drop
        self.recv = subprocess.Popen(receive, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
        self.unz = subprocess.Popen([zstd, "-q", "-d", "-c"], stdin=subprocess.PIPE, stdout=self.recv.stdin)
        assert self.recv.stdin is not None
        self.recv.stdin.close()  # unz holds it now

    def write(self, chunk: bytes) -> None:
        assert self.unz.stdin is not None
        self.unz.stdin.write(chunk)

    def close(self) -> None:
        assert self.unz.stdin is not None and self.recv.stderr is not None
        with contextlib.suppress(BrokenPipeError):  # zstd already gave up: said below
            self.unz.stdin.close()
        self.unz.wait()
        self.recv.wait()
        err = self.recv.stderr.read().decode().strip()
        if self.unz.returncode or self.recv.returncode or not self.made.exists():
            self.abort()
            raise VolumeError(f"receiving {self.made.name}: {err or f'zstd exited {self.unz.returncode}'}")

    def abort(self) -> None:
        for p in (self.unz, self.recv):
            if p.poll() is None:
                p.kill()
                p.wait()
        if self.made.exists():  # half received: btrfs left a subvolume, writable and incomplete
            self.drop()


def _disk(path: Path) -> tuple[int, int]:
    fs = os.statvfs(path)
    return fs.f_blocks * fs.f_frsize, fs.f_bavail * fs.f_frsize


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

    def drop(self, parts: dict[str, str]) -> None:
        """Free what the checkpoint's snapshot holds (it can't be restored after)."""
        if PART in parts:
            self.agent.drop_volume(parts[PART])

    def restore(self, parts: dict[str, str]) -> None:
        if PART not in parts:
            raise VolumeError("that checkpoint has no volume snapshot (it was taken before snapshots were on)")
        self.agent.restore_volume(parts[PART])

    # Optional parts of Versioned, for the shelf: a checkpoint's part as a
    # stream (relative to an earlier checkpoint's, which the receiver must
    # have), and made again from one.
    archive_name = "volume.btrfs.zst"

    def send(self, parts: dict[str, str], base: dict[str, str] | None) -> Iterator[bytes]:
        if PART not in parts:
            raise VolumeError("that checkpoint has no volume snapshot")
        return self.agent.send_volume(parts[PART], (base or {}).get(PART))

    def receive(self, parts: dict[str, str], chunks: Iterator[bytes]) -> None:
        self.agent.receive_volume(parts[PART], chunks)
