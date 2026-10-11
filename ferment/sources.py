"""Getting source code onto a VM that can't reach where it lives.

The client can reach the repo (say, behind a VPN); the VM can't. So the
client resolves refs and fetches into a local mirror, then ships the VM a git
bundle with just the commits it lacks. On the VM the code arrives as a real
git repo (history included), so builds that inspect .git keep working.

Client side (this module):
    commit = resolve(repo, "development")
    mirror = fetch(repo, commit)
    data = bundle(mirror, commit, have=[...commits the VM already has])
Server side: ferment.server.sources.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
import tempfile
from pathlib import Path

from ferment.config import state_dir

SHA = re.compile(r"^[0-9a-f]{40}$")


class SourceError(RuntimeError):
    pass


def _git(*args: str, cwd: Path | None = None) -> str:
    result = subprocess.run(["git", *args], cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        raise SourceError(f"git {' '.join(args)}: {result.stderr.strip() or result.stdout.strip()}")
    return result.stdout


def mirror_path(repo: str) -> Path:
    """Where this client keeps its mirror of `repo` (long-lived: fetches are incremental)."""
    tail = re.sub(r"[^A-Za-z0-9._-]+", "-", repo.rstrip("/").split("/")[-1].removesuffix(".git"))
    digest = hashlib.sha256(repo.encode()).hexdigest()[:12]
    return state_dir() / "repos" / f"{tail}-{digest}.git"


def resolve(repo: str, ref: str) -> str:
    """Branch/tag -> the commit it points at right now (a commit resolves to itself)."""
    if SHA.match(ref):
        return ref
    out = _git("ls-remote", repo, ref, f"refs/heads/{ref}", f"refs/tags/{ref}^{{}}", f"refs/tags/{ref}")
    lines = [line.split() for line in out.splitlines() if line.strip()]
    if not lines:
        raise SourceError(f"{repo} has no branch or tag named {ref!r}")
    # A peeled annotated tag (^{}) names the commit; prefer it over the tag object.
    peeled = [sha for sha, name in lines if name.endswith("^{}")]
    return peeled[0] if peeled else lines[0][0]


def fetch(repo: str, commit: str) -> Path:
    """Make sure the local mirror of `repo` contains `commit`; returns the mirror."""
    mirror = mirror_path(repo)
    if not (mirror / "HEAD").exists():
        mirror.parent.mkdir(parents=True, exist_ok=True)
        _git("init", "--bare", "--quiet", str(mirror))
    if not _has(mirror, commit):
        _git("fetch", "--quiet", repo, "+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*", cwd=mirror)
    if not _has(mirror, commit):
        # Not on any branch or tag (e.g. a PR commit): ask for it directly.
        _git("fetch", "--quiet", repo, commit, cwd=mirror)
    if not _has(mirror, commit):
        raise SourceError(f"{repo} doesn't have commit {commit}")
    return mirror


def _has(mirror: Path, commit: str) -> bool:
    return (
        subprocess.run(["git", "cat-file", "-e", f"{commit}^{{commit}}"], cwd=mirror, capture_output=True).returncode
        == 0
    )


def bundle(mirror: Path, commit: str, have: list[str] = ()) -> bytes:  # type: ignore[assignment]
    """A git bundle carrying `commit` as refs/ferment/<commit>, minus history
    reachable from `have` (commits the receiver already holds)."""
    ref = f"refs/ferment/{commit}"
    _git("update-ref", ref, commit, cwd=mirror)
    exclude = [f"^{c}" for c in have if SHA.match(c) and _has(mirror, c) and c != commit]
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "x.bundle"
        result = subprocess.run(
            ["git", "bundle", "create", "--quiet", str(out), ref, *exclude], cwd=mirror, capture_output=True, text=True
        )
        if result.returncode != 0:
            raise SourceError(f"git bundle create: {result.stderr.strip()}")
        return out.read_bytes()
