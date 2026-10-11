"""Builds on the VM: run a command in a checkout of a commit, keep the result.

Builds run on real time, outside the faked workload: they talk TLS to package
mirrors, and certificate checks under a fake clock fail. A successful build
is reused for the same (source, commit, command), so re-applying a template
is cheap; failed builds are retried.

Each build is a self-contained script that outlives the server: it records
its pid, appends all output to the build's log, and writes its exit status
to a file when done. On a systemd guest it runs as its own transient unit
(outside the server's cgroup), so redeploying or restarting the server
doesn't kill a twenty-minute build; the restarted server adopts it and picks
up the result. The server only watches for exit files.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Protocol

from ferment.server.sources import SourceStore
from ferment.server.store import Store

# Server env that must not leak into builds: the workload's fake clock.
_NOT_FOR_BUILDS = ("LD_PRELOAD", "FAKETIME", "FAKETIME_DONT_FAKE_MONOTONIC")


def _silent(event: str, who: str, message: str, **fields) -> None:
    pass


class Launcher(Protocol):
    """Starts a build script so that it survives the server."""

    def launch(self, build_id: int, script: Path) -> None: ...


class SystemdLauncher:
    """A transient system unit per build, run as the server's own user."""

    def launch(self, build_id: int, script: Path) -> None:
        subprocess.run(
            [
                "sudo", "systemd-run", "--quiet", "--collect",
                f"--unit=ferment-build-{build_id}",
                f"--uid={os.getuid()}", f"--gid={os.getgid()}",
                f"--description=ferment build {build_id}",
                shutil.which("bash") or "/bin/sh", str(script),
            ],
            check=True, capture_output=True, text=True,
        )  # fmt: skip


class DetachedLauncher:
    """No systemd (tests, macOS): a detached process. Survives the server
    object, though not a service manager that kills the whole tree."""

    def launch(self, build_id: int, script: Path) -> None:
        subprocess.Popen(
            ["bash", str(script)], start_new_session=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )  # fmt: skip


def default_launcher() -> Launcher:
    if Path("/run/systemd/system").is_dir() and shutil.which("systemd-run") and shutil.which("sudo"):
        return SystemdLauncher()
    return DetachedLauncher()


def _alive(pid: int) -> bool:
    try:  # our own child (a detached launch)? reap it if it's a zombie
        if os.waitpid(pid, os.WNOHANG) != (0, 0):
            return False
    except ChildProcessError:
        pass  # not ours (a systemd unit): fall through
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


class Builder:
    POLL = 2.0

    def __init__(
        self,
        store: Store,
        sources: SourceStore,
        log_dir: Path,
        broadcast=_silent,
        launcher: Launcher | None = None,
        watch: bool = True,
    ):
        self.broadcast = broadcast
        self.store = store
        self.sources = sources
        self.log_dir = log_dir
        self.launcher = launcher or default_launcher()
        self.lock = threading.Lock()
        store.fail_unlaunched_builds()
        # Running builds with no script file were children of an older server
        # (before builds outlived it); that server's exit took them along.
        for b in store.running_builds():
            if not self._file(b["id"], "sh").exists():
                self.store.update_build(b["id"], status="failed", error="lost when the server restarted (old build)")
        self.stopping = threading.Event()
        if watch:
            threading.Thread(target=self._watch, daemon=True).start()

    # -- files a build leaves behind ---------------------------------------------

    def _file(self, build_id: int, suffix: str) -> Path:
        return self.log_dir / f"build-{build_id}.{suffix}"

    def _script(self, b: dict, tree: Path) -> Path:
        env = {k: v for k, v in os.environ.items() if k not in _NOT_FOR_BUILDS}
        # Reproducible-build tools (Maven's outputTimestamp, zip, jar) stamp
        # outputs with SOURCE_DATE_EPOCH. Nix shells set it to 1980-01-01,
        # which some tools reject; the commit's own time is what it's meant to
        # be, and makes outputs reproducible per commit.
        env["SOURCE_DATE_EPOCH"] = subprocess.run(
            ["git", "log", "-1", "--format=%ct"], cwd=tree, text=True, capture_output=True, check=True
        ).stdout.strip()
        q = shlex.quote
        pid, exit_, log = self._file(b["id"], "pid"), self._file(b["id"], "exit"), b["log_path"]
        lines = ["#!/usr/bin/env bash", f"echo $$ > {q(str(pid))}"]
        lines += [f"export {k}={q(v)}" for k, v in env.items() if k.isidentifier()]
        lines += [
            f"cd {q(str(tree))}",
            f"exec >>{q(log)} 2>&1",
            f"printf '%s\\n' {q('$ ' + b['command'])} {q('# in ' + str(tree))}",
            f"bash -c {q(b['command'])}",
            "rc=$?",
            f'echo "$rc" > {q(str(exit_))}.tmp && mv {q(str(exit_))}.tmp {q(str(exit_))}',
        ]
        script = self._file(b["id"], "sh")
        script.write_text("\n".join(lines) + "\n")
        return script

    # -- requests ----------------------------------------------------------------

    def request(self, name: str, commit: str, command: str, artifact_glob: str | None, who: str) -> dict:
        """Start (or reuse) a build; returns its row immediately."""
        existing = self.store.find_build(name, commit, command)
        if existing is not None:
            return existing
        tree = self.sources.worktree(name, commit)  # fail fast if the commit isn't here
        self.log_dir.mkdir(parents=True, exist_ok=True)
        build_id = self.store.add_build(
            name=name, commit=commit, command=command, artifact_glob=artifact_glob, who=who, log_path=""
        )
        log_path = self.log_dir / f"build-{build_id}-{name}.log"
        self.store.update_build(build_id, log_path=str(log_path))
        b = self.store.build(build_id)
        assert b is not None
        try:
            self.launcher.launch(build_id, self._script(b, tree))
        except (OSError, subprocess.CalledProcessError) as e:
            detail = getattr(e, "stderr", "") or str(e)
            self.store.update_build(build_id, status="failed", error=f"couldn't start: {detail.strip()}")
            return self.store.build(build_id)  # type: ignore[return-value]
        self.store.update_build(build_id, status="running")
        self.broadcast("build_started", who, f"{who} started build {build_id}: {name} @ {commit[:12]}")
        return self.store.build(build_id)  # type: ignore[return-value]

    # -- watching ----------------------------------------------------------------

    def _watch(self) -> None:
        while not self.stopping.wait(self.POLL):
            self.reap()

    def reap(self) -> None:
        """Finish every running build whose script has exited (including ones
        started by a previous server)."""
        with self.lock:
            for b in self.store.running_builds():
                exit_file = self._file(b["id"], "exit")
                if exit_file.exists():
                    self._finish(b, int(exit_file.read_text().strip() or 1))
                    continue
                pid_file = self._file(b["id"], "pid")
                started = pid_file.exists() and pid_file.read_text().strip().isdigit()
                if started and not _alive(int(pid_file.read_text())):
                    # Re-check: it may have written its exit status since we looked.
                    if exit_file.exists():
                        self._finish(b, int(exit_file.read_text().strip() or 1))
                    else:
                        self._fail(b, "build process ended without reporting an exit status (killed?)")

    def _fail(self, b: dict, error: str) -> None:
        self.store.update_build(b["id"], status="failed", error=error)
        self._announce(b["id"])

    def _finish(self, b: dict, rc: int) -> None:
        if rc != 0:
            return self._fail(b, f"exit {rc}")
        artifact = None
        if b["artifact_glob"]:
            tree = self.sources.worktree(b["name"], b["commit_sha"])
            matches = sorted(p for p in tree.glob(b["artifact_glob"]) if p.is_file())
            if len(matches) != 1:
                return self._fail(b, f"artifact glob {b['artifact_glob']!r} matched {len(matches)} files, expected 1")
            artifact = str(matches[0])
        self.store.update_build(b["id"], status="succeeded", artifact=artifact)
        self._announce(b["id"])

    def _announce(self, build_id: int) -> None:
        done = self.store.build(build_id)
        assert done is not None
        outcome = done["artifact"] if done["status"] == "succeeded" else done["error"]
        self.broadcast(
            f"build_{done['status']}",
            done["who"],
            f"build {build_id} ({done['name']} @ {done['commit_sha'][:12]}) {done['status']}: {outcome}",
        )

    def log(self, build_id: int, offset: int = 0) -> tuple[str, int]:
        """Log text from `offset` on, and the offset to ask from next time."""
        b = self.store.build(build_id)
        if b is None or not b["log_path"] or not Path(b["log_path"]).exists():
            return "", offset
        with open(b["log_path"], "rb") as f:
            f.seek(offset)
            data = f.read()
        return data.decode(errors="replace"), offset + len(data)
