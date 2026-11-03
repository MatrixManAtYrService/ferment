"""The shelf: long-term storage for what outlives a fermenter.

A fermenter's disks are working memory: its volume holds the live data and
the snapshots in use, and is sized for that. What's worth keeping beyond
them goes on the shelf, a store of objects by key, each with a little JSON
beside it (its metadata):

- `datasets/`: data fragments and dataset manifests;
- `results/`: what experiments report and make;
- `snapshots/`: checkpoints' data, as streams that make them again on any
  fermenter that has what they were taken relative to.

The backend is a directory (on the Lima host, e.g. an external drive,
mounted into the control VM) or, later, a cloud bucket: the same keys and
metadata either way, so moving between them is a copy.

Every object says where its data came from (`provenance`: "synthetic", or
anything else, e.g. "prod"). A shelf may refuse what it mustn't hold: a
bucket that promises synthetic data only refuses the rest
(`synthetic_only`).
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Protocol

KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$")
META_SUFFIX = ".meta.json"
SYNTHETIC = "synthetic"


class ShelfError(RuntimeError):
    pass


class Shelf(Protocol):
    url: str

    def put(self, key: str, chunks: Iterable[bytes], meta: dict) -> dict: ...

    def get(self, key: str, chunk: int = 1 << 20) -> Iterator[bytes]: ...

    def meta(self, key: str) -> dict | None: ...

    def list(self, prefix: str = "") -> list[str]: ...

    def delete(self, key: str) -> None: ...


def check_key(key: str) -> str:
    if not KEY.match(key) or key.endswith(META_SUFFIX):
        raise ShelfError(f"bad shelf key {key!r}")
    return key


class DirShelf:
    """A shelf in a directory: an object is a file at its key, its metadata
    beside it (`<key>.meta.json`). A put lands whole or not at all."""

    def __init__(self, root: Path, synthetic_only: bool = False):
        self.root = root
        self.url = f"file://{root}"
        self.synthetic_only = synthetic_only

    def _path(self, key: str) -> Path:
        return self.root / check_key(key)

    def put(self, key: str, chunks: Iterable[bytes], meta: dict) -> dict:
        if self.synthetic_only and meta.get("provenance") != SYNTHETIC:
            raise ShelfError(f"{self.url} holds synthetic data only; {key} is {meta.get('provenance', 'unlabelled')}")
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.partial")
        size = 0
        try:
            with tmp.open("wb") as f:
                for c in chunks:
                    f.write(c)
                    size += len(c)
                f.flush()
                os.fsync(f.fileno())
            meta = {**meta, "key": key, "bytes": size}
            path.with_name(path.name + META_SUFFIX).write_text(json.dumps(meta, indent=2, default=str))
            tmp.rename(path)
        finally:
            tmp.unlink(missing_ok=True)
        return meta

    def get(self, key: str, chunk: int = 1 << 20) -> Iterator[bytes]:
        path = self._path(key)
        if not path.exists():
            raise ShelfError(f"nothing on the shelf at {key}")
        with path.open("rb") as f:
            while b := f.read(chunk):
                yield b

    def meta(self, key: str) -> dict | None:
        path = self._path(key)
        side = path.with_name(path.name + META_SUFFIX)
        return json.loads(side.read_text()) if path.exists() and side.exists() else None

    def list(self, prefix: str = "") -> list[str]:
        if not self.root.exists():
            return []
        keys = (
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file() and not p.name.endswith(META_SUFFIX) and not p.name.startswith(".")
        )
        return sorted(k for k in keys if k.startswith(prefix))

    def delete(self, key: str) -> None:
        path = self._path(key)
        path.unlink(missing_ok=True)
        path.with_name(path.name + META_SUFFIX).unlink(missing_ok=True)


def open_shelf(url: str) -> Shelf:
    """A shelf by URL: a directory (`file:///path` or a plain path); buckets
    (`gs://`) once there's a backend for them."""
    if url.startswith("gs://"):
        raise ShelfError(f"{url}: no bucket backend yet")
    return DirShelf(Path(url.removeprefix("file://")))
