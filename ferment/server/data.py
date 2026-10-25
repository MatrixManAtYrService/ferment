"""Data for the workload: fragments, datasets, data requests, and loading.

A **fragment** is a piece of a database as SQL text (gzip'd), stored once by
the sha256 of its bytes. Its key says what it is:

    {"source": "eda:na", "database": "billing_event", "table": "fee_summary",
     "selector": "all", "version": "<the source's checksum>"}

`selector` is `all` (the table: schema and rows, as mysqldump writes it) or
`schema` (CREATE TABLE only). Later selectors pick rows (a date range, the
rows some queries touched), so a database can be put together from
fragments of different sources and ages rather than one snapshot. A
**dataset** names a set of fragments to load together.

Fragments live under a cache when the provider mounted one (CACHE_MOUNT/data:
on the VM host, kept across fermenters), else in the server's state.

**Data requests** are how fragments arrive when the VM can't reach the
source (a prod database behind a VPN): like source requests, an attached
client that provides the capability (`data:<source>`) claims one, gets the
data with its own access, and uploads it (ferment.data). The spec means
something only to that source's handler; the result is whatever it reports
(e.g. the fragment it stored).

**Loading** a dataset replaces each of its databases: create it empty, then
run every fragment through the database's own client, schemas first.
mysqldump output is filtered on the way: GTID and binlog settings are
dropped (the fermenter's server isn't a replica of anything) and DEFINER
clauses removed (prod's users don't exist here; triggers and views would
fail as them).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
import subprocess
import tempfile
import threading
from collections.abc import Callable, Iterator
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from ferment.server.clock import real_now
from ferment.vm.base import CACHE_MOUNT

_SHA = re.compile(r"^[0-9a-f]{64}$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def key_id(key: dict) -> str:
    return hashlib.sha256(json.dumps(key, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:32]


def data_root(state_dir: Path) -> Path:
    cache = Path(CACHE_MOUNT) / "data"
    return cache if cache.is_dir() else state_dir / "data"


class DataStore:
    def __init__(self, root: Path):
        self.root = root
        for d in ("blobs", "fragments", "datasets", "tmp"):
            (root / d).mkdir(parents=True, exist_ok=True)

    def blob(self, sha256: str) -> Path:
        return self.root / "blobs" / f"{sha256}.sql.gz"

    def add_fragment(self, key: dict, sha256: str) -> dict:
        if not self.blob(sha256).exists():
            raise LookupError(f"no blob {sha256}")
        rec = {
            "id": key_id(key),
            "key": key,
            "sha256": sha256,
            "size": self.blob(sha256).stat().st_size,
            "stored_at": real_now().isoformat(),
        }
        _write_json(self.root / "fragments" / f"{rec['id']}.json", rec)
        return rec

    def fragment(self, fid: str) -> dict | None:
        p = self.root / "fragments" / f"{fid}.json"
        return json.loads(p.read_text()) if p.exists() else None

    def save_dataset(self, name: str, fragments: list[str], who: str, meta: dict) -> dict:
        missing = [f for f in fragments if self.fragment(f) is None]
        if missing:
            raise LookupError(f"no such fragments: {', '.join(missing[:5])}")
        rec = {
            "name": name,
            "fragments": fragments,
            "meta": meta,
            "created_by": who,
            "created_at": real_now().isoformat(),
        }
        _write_json(self.root / "datasets" / f"{name}.json", rec)
        return rec

    def dataset(self, name: str) -> dict | None:
        p = self.root / "datasets" / f"{name}.json"
        return json.loads(p.read_text()) if p.exists() else None

    def datasets(self) -> list[dict]:
        out = []
        for p in sorted((self.root / "datasets").glob("*.json")):
            d = json.loads(p.read_text())
            frags = [self.fragment(f) or {} for f in d["fragments"]]
            out.append({**d, "fragments": len(frags), "size": sum(f.get("size", 0) for f in frags)})
        return out


def _write_json(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=2))
    tmp.replace(path)


# -- loading --------------------------------------------------------------------

_DEFINER = re.compile(rb"DEFINER=`[^`]*`@`[^`]*`\s?")
# Session settings each load runs under: no binlog, no checks a dump's own
# order would trip.
LOAD_PRELUDE = b"SET SESSION sql_log_bin=0;\nSET FOREIGN_KEY_CHECKS=0;\nSET UNIQUE_CHECKS=0;\n"


def filter_dump(lines: Iterator[bytes]) -> Iterator[bytes]:
    """mysqldump output, made loadable on a fresh standalone server."""
    skipping = False
    for line in lines:
        if skipping:
            skipping = not line.rstrip().endswith(b";")
            continue
        if line.startswith(b"SET @@GLOBAL.GTID_PURGED"):
            skipping = not line.rstrip().endswith(b";")  # the GTID set can span lines
            continue
        if line.startswith((b"SET @@SESSION.SQL_LOG_BIN", b"SET @MYSQLDUMP_TEMP_LOG_BIN")):
            continue
        if b"DEFINER" in line:
            line = _DEFINER.sub(b"", line)
        yield line


def load_order(fragments: list[dict]) -> list[dict]:
    """Schemas first, then tables, each by name."""
    return sorted(fragments, key=lambda f: (f["key"]["selector"] != "schema", f["key"]["table"]))


class Loader:
    """Loads a dataset into the database, one job at a time, in the background."""

    def __init__(
        self,
        datastore: DataStore,
        client_args: Callable[[], list[str]],
        steps: Callable[[str, list[str]], Callable[[str], None]],
        broadcast: Callable[..., None],
        run: Callable[[list[str], Iterator[bytes]], str] | None = None,
    ):
        """`client_args()`: the database's CLI client (ferment.databases);
        `steps(group, keys)`: board milestones, called with a key as it's done;
        `run(argv, stdin_chunks)`: run a client on input, '' or its error."""
        self.data = datastore
        self.client_args = client_args
        self.steps = steps
        self.broadcast = broadcast
        self.run = run or _run_client
        self.state: dict = {"state": "idle"}
        self._lock = threading.Lock()

    def start(self, name: str, who: str) -> dict:
        d = self.data.dataset(name)
        if d is None:
            raise LookupError(f"no dataset {name!r}")
        frags = [self.data.fragment(f) for f in d["fragments"]]
        by_db: dict[str, list[dict]] = {}
        for f in frags:
            by_db.setdefault(f["key"]["database"], []).append(f)
        with self._lock:
            if self.state["state"] == "running":
                raise RuntimeError(f"already loading {self.state['dataset']}")
            self.state = {
                "state": "running",
                "dataset": name,
                "databases": sorted(by_db),
                "done": 0,
                "total": len(frags),
                "current": None,
                "error": None,
                "started_at": real_now().isoformat(),
                "who": who,
            }
        threading.Thread(target=self._load, args=(by_db, who), daemon=True).start()
        return dict(self.state)

    def _load(self, by_db: dict[str, list[dict]], who: str) -> None:
        name = self.state["dataset"]
        step = self.steps(f"load {name}", sorted(by_db))
        self.broadcast("data_loading", who, f"{who} is loading {name} ({', '.join(sorted(by_db))})")
        began = real_now()
        try:
            for db in sorted(by_db):
                q = f"`{db}`"
                err = self.run(
                    self.client_args(),
                    iter([f"DROP DATABASE IF EXISTS {q}; CREATE DATABASE {q} CHARACTER SET utf8mb4;\n".encode()]),
                )
                if err:
                    raise RuntimeError(f"creating {db}: {err}")
                for f in load_order(by_db[db]):
                    table = f"{db}.{f['key']['table']}"
                    self.state["current"] = table
                    err = self.run([*self.client_args(), db], self._chunks(f))
                    if err:
                        raise RuntimeError(f"{table} ({f['key']['selector']}): {err}")
                    self.state["done"] += 1
                step(db)
        except Exception as e:  # report it, and leave the board saying so
            self.state.update(state="failed", error=str(e))
            fail = getattr(step, "fail", None)
            if fail is not None:
                fail(str(e))
            self.broadcast("data_load_failed", who, f"loading {name} failed: {e}")
            return
        took = (real_now() - began).total_seconds() / 60
        self.state.update(state="done", current=None, finished_at=real_now().isoformat())
        self.broadcast("data_loaded", who, f"loaded {name}: {self.state['total']} fragments in {took:.0f}m")

    def _chunks(self, fragment: dict) -> Iterator[bytes]:
        yield LOAD_PRELUDE
        with gzip.open(self.data.blob(fragment["sha256"]), "rb") as f:
            yield from filter_dump(f)


def _run_client(argv: list[str], chunks: Iterator[bytes]) -> str:
    with tempfile.TemporaryFile() as err:
        p = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err)
        assert p.stdin
        try:
            for chunk in chunks:
                p.stdin.write(chunk)
        except BrokenPipeError:
            pass  # the client quit early: its stderr says why
        finally:
            p.stdin.close()
        code = p.wait()
        err.seek(0)
        text = err.read().decode(errors="replace").strip()
    return "" if code == 0 else (text.splitlines()[-1] if text else f"exit {code}")


# -- routes ---------------------------------------------------------------------


class NewDataRequest(BaseModel):
    capability: str  # data:<source>
    spec: dict


class FinishDataRequest(BaseModel):
    result: dict | None = None
    error: str | None = None


class NewFragment(BaseModel):
    key: dict
    sha256: str


class FindFragment(BaseModel):
    key: dict


class NewDataset(BaseModel):
    fragments: list[str]
    meta: dict = {}


def data_router(
    store, datastore: DataStore, loader: Loader | None, who: Callable[[Request], str], broadcast
) -> APIRouter:
    r = APIRouter()

    @r.post("/data-requests")
    def request_data(req: NewDataRequest, request: Request):
        # Not broadcast: a pull asks for every table at once. The board's
        # `waiting` fact says when nobody attached can serve them.
        return store.request_data(req.capability, req.spec, who(request))

    @r.get("/data-requests")
    def data_requests(capability: str | None = None):
        return store.claimable_data_requests(capability)

    @r.get("/data-requests/{request_id}")
    def data_request(request_id: int):
        row = store.data_request(request_id)
        if row is None:
            raise HTTPException(404, f"no data request {request_id}")
        return row

    @r.post("/data-requests/{request_id}/claim")
    def claim(request_id: int, request: Request):
        if not store.claim_data_request(request_id, who(request)):
            raise HTTPException(409, "already claimed")
        return store.data_request(request_id)

    @r.post("/data-requests/{request_id}/finish")
    def finish(request_id: int, req: FinishDataRequest):
        if (req.result is None) == (req.error is None):
            raise HTTPException(422, "give exactly one of result or error")
        row = store.data_request(request_id)
        if row is None:
            raise HTTPException(404, f"no data request {request_id}")
        store.finish_data_request(request_id, result=req.result, error=req.error)
        if req.error is not None:
            broadcast("data_failed", row["claimed_by"] or "?", f"couldn't get {_describe(row['spec'])}: {req.error}")
        return store.data_request(request_id)

    @r.post("/data/blobs")
    async def upload_blob(request: Request):
        """Body: gzip'd SQL, streamed. Returns its sha256."""
        tmp = datastore.root / "tmp"
        h, size = hashlib.sha256(), 0
        with tempfile.NamedTemporaryFile(dir=tmp, delete=False) as f:
            async for chunk in request.stream():
                h.update(chunk)
                size += len(chunk)
                f.write(chunk)
        sha = h.hexdigest()
        Path(f.name).replace(datastore.blob(sha))
        return {"sha256": sha, "size": size}

    @r.post("/data/fragments")
    def add_fragment(req: NewFragment):
        if not _SHA.match(req.sha256):
            raise HTTPException(422, "sha256: 64 hex digits")
        try:
            return datastore.add_fragment(req.key, req.sha256)
        except LookupError as e:
            raise HTTPException(422, str(e))

    @r.post("/data/fragments/find")
    def find_fragment(req: FindFragment):
        rec = datastore.fragment(key_id(req.key))
        if rec is None:
            raise HTTPException(404, "no such fragment")
        return rec

    @r.get("/data/datasets")
    def datasets():
        return datastore.datasets()

    @r.get("/data/datasets/{name}")
    def dataset(name: str):
        d = datastore.dataset(name)
        if d is None:
            raise HTTPException(404, f"no dataset {name!r}")
        return {**d, "fragments": [datastore.fragment(f) for f in d["fragments"]]}

    @r.put("/data/datasets/{name}")
    def save_dataset(name: str, req: NewDataset, request: Request):
        if not _NAME.match(name):
            raise HTTPException(422, "dataset names: letters, digits, . _ -")
        try:
            d = datastore.save_dataset(name, req.fragments, who(request), req.meta)
        except LookupError as e:
            raise HTTPException(422, str(e))
        broadcast("dataset_saved", who(request), f"dataset {name}: {len(req.fragments)} fragments")
        return d

    @r.post("/data/load/{name}", status_code=202)
    def load(name: str, request: Request):
        if loader is None:
            raise HTTPException(501, "this fermenter has no database to load into")
        try:
            return loader.start(name, who(request))
        except LookupError as e:
            raise HTTPException(404, str(e))
        except RuntimeError as e:
            raise HTTPException(409, str(e))

    @r.get("/data/load")
    def load_state():
        return loader.state if loader is not None else {"state": "idle"}

    return r


def _describe(spec: dict) -> str:
    parts = [str(spec[k]) for k in ("database", "table") if k in spec]
    what = ".".join(parts) or json.dumps(spec, sort_keys=True)
    return f"{what} ({spec['selector']})" if "selector" in spec else what
