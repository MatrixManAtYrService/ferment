"""Everything ferment does to a VM after it exists goes through plain OpenSSH.

Providers differ only in the SshTarget they hand back (a lima ssh config, an
IAP ProxyCommand, ...). Running commands, pushing source, and the tunnel a
client holds open while attached are the same code for all of them.
"""

from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ferment.vm.base import SshTarget

# Never push these: build output, caches, and local environments that mean
# nothing on the VM.
PUSH_EXCLUDES = [".git", ".venv", ".direnv", "__pycache__", "*.pyc", ".pytest_cache", ".ruff_cache", "result"]


def argv(target: SshTarget, forwards: list[Forward] | None = None) -> list[str]:
    """ssh to the VM, carrying `forwards` for as long as the session lasts.

    A forward that can't be set up (say another session already holds the
    same -R port) is a warning, not a failure: the other session's forward
    leads to the same place.
    """
    args = target.argv()
    if not forwards:
        return args
    host = args.pop()
    args += ["-o", "ControlMaster=no", "-o", "ControlPath=none"]
    for fwd in forwards:
        args += fwd.ssh_args()
    args.append(host)
    return args


def run(
    target: SshTarget,
    command: str,
    *,
    check: bool = True,
    capture: bool = False,
    forwards: list[Forward] | None = None,
) -> subprocess.CompletedProcess:
    """Run a shell command on the VM (as the ssh user; prefix sudo yourself),
    with `forwards` held open while it runs."""
    return subprocess.run(
        [*argv(target, forwards), "--", command],
        check=check,
        text=True,
        capture_output=capture,
    )


def push_dir(target: SshTarget, local: Path, remote: str, *, excludes: list[str] = PUSH_EXCLUDES) -> None:
    """Replace `remote` on the VM with the contents of `local` (tar over ssh).

    No rsync on either side required. The remote directory is cleared first
    so files deleted locally don't linger.
    """
    tar_cmd = ["tar", "-C", str(local), "-czf", "-"]
    for pattern in excludes:
        tar_cmd += ["--exclude", pattern]
    tar_cmd.append(".")
    q = shlex.quote(remote)
    unpack = f"rm -rf {q} && mkdir -p {q} && tar -C {q} -xzf -"
    # COPYFILE_DISABLE keeps macOS tar from adding ._* AppleDouble files.
    tar = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, env={**os.environ, "COPYFILE_DISABLE": "1"})
    try:
        subprocess.run([*target.argv(), "--", unpack], stdin=tar.stdout, check=True)
    finally:
        assert tar.stdout is not None
        tar.stdout.close()
        tar.wait()
    if tar.returncode != 0:
        raise subprocess.CalledProcessError(tar.returncode, tar_cmd)


@dataclass
class Forward:
    """One port carried by the tunnel.

    local:  the client-side port.
    remote: the VM-side port.
    reverse=False (-L): client localhost:local -> VM localhost:remote
                        (a browser on the laptop reaching grafana on the VM).
    reverse=True  (-R): VM localhost:remote -> client-side dest
                        (the VM reaching something only the client can, e.g.
                        a package mirror behind a VPN). `dest` is
                        "host:port" as seen from the client.
    """

    local: int
    remote: int
    reverse: bool = False
    dest: str | None = None
    label: str = ""

    def ssh_args(self) -> list[str]:
        if self.reverse:
            return ["-R", f"127.0.0.1:{self.remote}:{self.dest}"]
        return ["-L", f"127.0.0.1:{self.local}:127.0.0.1:{self.remote}"]


def tunnel_argv(target: SshTarget, forwards: list[Forward], *, strict: bool = True) -> list[str]:
    """`ssh -N` holding `forwards`; strict: exit if any of them can't be set up."""
    args = target.argv()
    host = args.pop()  # ssh options must precede the destination
    args += [
        "-N",
        # A dedicated connection, never a shared control master (lima's ssh
        # config multiplexes): through a master, `ssh -N -L` hands the forward
        # over and exits, leaving it alive after the tunnel "closes".
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPath=none",
        # Nobody is there to answer a password prompt (and attach's display
        # would hide it): a key the VM doesn't trust fails right away.
        "-o",
        "BatchMode=yes",
        "-o",
        f"ExitOnForwardFailure={'yes' if strict else 'no'}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=4",
    ]
    for fwd in forwards:
        args += fwd.ssh_args()
    args.append(host)
    return args


class _Supervised(threading.Thread):
    """Keeps `forwards` held by its own ssh, retrying until it can.

    For relays: the VM's relay port goes to the first session that binds it.
    If that's another session (or a dead one sshd hasn't reaped yet), ssh
    exits on the bind failure (ExitOnForwardFailure) and we try again, so we
    take over as soon as the port is free, and the user hears about it.
    """

    RETRY = 15.0

    def __init__(
        self,
        target: SshTarget,
        forwards: list[Forward],
        label: str,
        report: Callable[[str, str, str], None] | None = None,
    ):
        super().__init__(daemon=True)
        self.argv = tunnel_argv(target, forwards, strict=True)
        self.label = label
        self.report = report
        self.stopping = threading.Event()
        self.proc: subprocess.Popen | None = None
        self.state = ""

    def _say(self, state: str, msg: str) -> None:
        if state != self.state:
            self.state = state
            if self.report is not None:
                self.report(self.label, state, msg)
            else:
                print(f"{self.label}: {msg}", file=sys.stderr)

    def run(self) -> None:
        while not self.stopping.is_set():
            started = time.monotonic()
            self.proc = subprocess.Popen(self.argv, start_new_session=True, stderr=subprocess.PIPE, text=True)
            # Forwards are up once ssh has survived a moment (bind failures exit fast).
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._say("up", "up")
                self.proc.wait()
            if self.stopping.is_set():
                return
            err = self.proc.stderr.read() if self.proc.stderr else ""
            if "forwarding failed" in err:
                self._say("busy", f"port held by another session; retrying every {self.RETRY:.0f}s")
            elif time.monotonic() - started > 5:
                self._say("lost", f"lost ({err.strip() or 'ssh exited'}); retrying")
            else:
                self._say("failed", f"couldn't start ({err.strip() or 'ssh exited'}); retrying")
            self.stopping.wait(self.RETRY)

    def stop(self) -> None:
        self.stopping.set()
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait()


def hold_tunnel(
    target: SshTarget,
    forwards: list[Forward],
    best_effort: list[Forward] | None = None,
    on_detach: Callable[[], None] | None = None,
    report: Callable[[str, str, str], None] | None = None,
) -> int:
    """Run the tunnel in the foreground until ssh exits (or ctrl-c).

    `best_effort` forwards (relays) ride their own supervised ssh: one that
    can't be set up doesn't take `forwards` down, and it keeps retrying until
    it can, reporting as it goes. `on_detach` runs on ctrl-c/SIGTERM/SIGHUP
    while the tunnel is still up (e.g. to say goodbye to the server through it).
    `report(label, state, message)` hears the best-effort forwards' states
    (up, busy, lost, failed) instead of their being printed.
    """
    # Own sessions: a terminal's ctrl-c reaches only us, so the tunnel is
    # still there while on_detach runs; then we stop it.
    proc = subprocess.Popen(tunnel_argv(target, forwards), start_new_session=True)
    side = (
        _Supervised(target, best_effort, "relay " + ", ".join(f.label or f.dest or "" for f in best_effort), report)
        if best_effort
        else None
    )
    if side is not None:
        side.start()

    # Closing the terminal (SIGHUP) or being killed (SIGTERM) is a detach too:
    # route both through the same cleanup as ctrl-c.
    def _detach(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _detach)
    signal.signal(signal.SIGHUP, _detach)
    try:
        return proc.wait()
    except KeyboardInterrupt:
        if on_detach is not None:
            try:
                on_detach()
            except Exception as e:  # best effort; never block detaching
                print(f"on detach: {e}", file=sys.stderr)
        print("tunnel closed", file=sys.stderr)
        return 0
    finally:
        if side is not None:
            side.stop()
        if proc.poll() is None:
            proc.terminate()
            proc.wait()
