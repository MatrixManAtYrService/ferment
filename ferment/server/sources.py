"""Server side of shipping source to the VM (client side: ferment.sources).

One bare repo per named source under <state>/sources/<name>.git, filled from
uploaded bundles; checkouts are git worktrees under <state>/worktrees, one
per (name, commit), so builds see a real .git.
"""

from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path

NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SHA = re.compile(r"^[0-9a-f]{40}$")


class SourceStoreError(ValueError):
    pass


def _git(*args: str, cwd: Path) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise SourceStoreError(f"git {' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


class SourceStore:
    def __init__(self, root: Path):
        self.root = root

    def _repo(self, name: str) -> Path:
        if not NAME.match(name):
            raise SourceStoreError(f"bad source name {name!r}")
        return self.root / "sources" / f"{name}.git"

    def commits(self, name: str) -> list[str]:
        """Commits we hold refs for: what a client can leave out of a bundle."""
        repo = self._repo(name)
        if not (repo / "HEAD").exists():
            return []
        out = _git("for-each-ref", "--format=%(objectname)", "refs/ferment/", cwd=repo)
        return sorted(set(out.split()))

    def receive(self, name: str, data: bytes) -> list[str]:
        """Unpack a bundle; returns the commits it delivered."""
        repo = self._repo(name)
        if not (repo / "HEAD").exists():
            repo.mkdir(parents=True, exist_ok=True)
            _git("init", "--bare", "--quiet", ".", cwd=repo)
        with tempfile.NamedTemporaryFile(suffix=".bundle") as f:
            f.write(data)
            f.flush()
            _git("bundle", "verify", "--quiet", f.name, cwd=repo)
            _git("fetch", "--quiet", f.name, "+refs/ferment/*:refs/ferment/*", cwd=repo)
            heads = _git("bundle", "list-heads", f.name, cwd=repo)
        return [line.split()[0] for line in heads.splitlines() if line.strip()]

    def worktree(self, name: str, commit: str) -> Path:
        """A checkout of `commit` (made once, then reused)."""
        if not SHA.match(commit):
            raise SourceStoreError(f"not a full commit sha: {commit!r}")
        repo = self._repo(name)
        path = self.root / "worktrees" / f"{name}-{commit[:12]}"
        if not path.exists():
            if commit not in self.commits(name):
                raise SourceStoreError(f"{name} has no commit {commit}; upload a bundle first")
            path.parent.mkdir(parents=True, exist_ok=True)
            _git("worktree", "add", "--quiet", "--detach", str(path), commit, cwd=repo)
        return path
