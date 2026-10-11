"""Builds on the VM: run a command in a checkout of a commit, keep the result.

Builds run on real time, outside the faked workload: they talk TLS to package
mirrors, and certificate checks under a fake clock fail. A successful build
is reused for the same (source, commit, command), so re-applying a template
is cheap; failed builds are retried.
"""

from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path

from ferment.server.sources import SourceStore
from ferment.server.store import Store


class Builder:
    def __init__(self, store: Store, sources: SourceStore, log_dir: Path):
        self.store = store
        self.sources = sources
        self.log_dir = log_dir
        store.fail_unfinished_builds()

    def request(self, name: str, commit: str, command: str, artifact_glob: str | None, who: str) -> dict:
        """Start (or reuse) a build; returns its row immediately."""
        existing = self.store.find_build(name, commit, command)
        if existing is not None:
            return existing
        self.sources.worktree(name, commit)  # fail fast if the commit isn't here
        self.log_dir.mkdir(parents=True, exist_ok=True)
        build_id = self.store.add_build(
            name=name, commit=commit, command=command, artifact_glob=artifact_glob, who=who, log_path=""
        )
        log_path = self.log_dir / f"build-{build_id}-{name}.log"
        self.store.update_build(build_id, log_path=str(log_path))
        threading.Thread(target=self._run, args=(build_id,), daemon=True).start()
        return self.store.build(build_id)  # type: ignore[return-value]

    def _run(self, build_id: int) -> None:
        b = self.store.build(build_id)
        assert b is not None
        self.store.update_build(build_id, status="running")
        try:
            tree = self.sources.worktree(b["name"], b["commit_sha"])
            with open(b["log_path"], "ab") as log:
                log.write(f"$ {b['command']}\n# in {tree}\n".encode())
                log.flush()
                env = {k: v for k, v in os.environ.items() if k not in ("LD_PRELOAD", "FAKETIME")}
                # Reproducible-build tools (Maven's outputTimestamp, zip, jar)
                # stamp outputs with SOURCE_DATE_EPOCH. Nix shells set it to
                # 1980-01-01, which some tools reject; the commit's own time is
                # what it's meant to be, and makes outputs reproducible per commit.
                env["SOURCE_DATE_EPOCH"] = subprocess.run(
                    ["git", "log", "-1", "--format=%ct"], cwd=tree, text=True, capture_output=True, check=True
                ).stdout.strip()
                rc = subprocess.run(
                    ["bash", "-c", b["command"]], cwd=tree, stdout=log, stderr=subprocess.STDOUT, env=env
                ).returncode
            if rc != 0:
                self.store.update_build(build_id, status="failed", error=f"exit {rc}")
                return
            artifact = None
            if b["artifact_glob"]:
                matches = sorted(p for p in tree.glob(b["artifact_glob"]) if p.is_file())
                if len(matches) != 1:
                    self.store.update_build(
                        build_id,
                        status="failed",
                        error=f"artifact glob {b['artifact_glob']!r} matched {len(matches)} files, expected 1",
                    )
                    return
                artifact = str(matches[0])
            self.store.update_build(build_id, status="succeeded", artifact=artifact)
        except Exception as e:  # anything: record it rather than lose it in a thread
            self.store.update_build(build_id, status="failed", error=str(e))

    def log(self, build_id: int, offset: int = 0) -> tuple[str, int]:
        """Log text from `offset` on, and the offset to ask from next time."""
        b = self.store.build(build_id)
        if b is None or not b["log_path"] or not Path(b["log_path"]).exists():
            return "", offset
        with open(b["log_path"], "rb") as f:
            f.seek(offset)
            data = f.read()
        return data.decode(errors="replace"), offset + len(data)
