"""A workload's outbound HTTP, served only from a cache.

Services call out to things a fermenter must never reach during a run (a
production API behind a VPN). Principle: a run serves only cached data, so
it's repeatable and nothing it does touches the real thing. This server
stands where those services expect the outbound proxy and:

- answers GET/HEAD from the cache, by the request's key; a miss fails
  closed (511, "precache it") and is remembered, so a run's misses are the
  list of what to fetch before the next one;
- fakes writes (200, no body), counting them, so a run shows what it tried
  to change; except POSTs under a configured prefix (`read_posts`), which
  are reads in POST's clothing (a query whose arguments are the body: COS's
  app event searches take a merchant list) and are cached like GETs;
- takes entries through the same toolbox API (`/cache/put`, `/cache/get`)
  the proxy that fetches them (eda's egress, in record mode) writes to.

Entries are kept in named recordings (the `cache` of each call); a run is
served from the active one (`PUT /recording`), and precaching writes into
it. Each entry carries the hash of its response, and every hit and miss is
numbered as it happens (`seq`): `/hits?after=` and `/misses?after=` are one
attempt's, from the `seq` read before it (not a time: every attempt
restarts the fake clock), and `manifest()` over a run's hits identifies the
answers it was given.

Keys are the outbound proxy's own: `METHOD upstream_path?sorted-query`, for
requests to `/proxy/<env>/<upstream_path>`, plus ` #<sha256(body)[:16]>`
when a read carries a body (some are GETs whose arguments are JSON),
computed from the incoming request so a recording and a replay agree. A
miss keeps its body and the headers that shape the answer (content type,
the Clover env), so precaching can send the same request.

The cache lives in SQLite outside the workload's snapshot volume: restoring
a checkpoint keeps what was precached.

    python -m ferment.egress --port 44445 --db state/egress/egress.sqlite --cache fermenter \
        --read-post cos/v3/internal/app_
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import Response
from pydantic import BaseModel

READS = {"GET", "HEAD"}
# Headers a miss keeps (lowercase): what the answer depends on beyond the key.
KEPT_HEADERS = ("content-type", "x-clover-appenv")

SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
    cache TEXT NOT NULL, key TEXT NOT NULL, status_code INTEGER NOT NULL, body TEXT NOT NULL,
    content_type TEXT, meta TEXT, stored_at TEXT NOT NULL, PRIMARY KEY (cache, key));
CREATE TABLE IF NOT EXISTS misses (
    cache TEXT NOT NULL, key TEXT NOT NULL, env TEXT NOT NULL, count INTEGER NOT NULL,
    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, body TEXT NOT NULL DEFAULT '',
    headers TEXT NOT NULL DEFAULT '{}', PRIMARY KEY (cache, key));
CREATE TABLE IF NOT EXISTS writes (
    cache TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, count INTEGER NOT NULL,
    last_seen TEXT NOT NULL, PRIMARY KEY (cache, method, path));
CREATE TABLE IF NOT EXISTS served (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, cache TEXT NOT NULL, key TEXT NOT NULL,
    response_hash TEXT, at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def cache_key(method: str, upstream_path: str, query: list[tuple[str, str]], body: bytes = b"") -> str:
    """As eda's egress proxy computes it (its _cache_key): the one form a recording
    and a replay both produce."""
    q = urlencode(sorted(query))
    key = f"{method.upper()} {upstream_path}" + (f"?{q}" if q else "")
    return key + (f" #{hashlib.sha256(body).hexdigest()[:16]}" if body else "")


def response_hash(status_code: int, content_type: str | None, body: str) -> str:
    """What an entry answers, as a hash: equal hashes, equal answers."""
    return hashlib.sha256(json.dumps([status_code, content_type, body]).encode()).hexdigest()


def manifest(served: Iterable[tuple[str, str]]) -> str:
    """A recording's manifest over the entries a run was served, as (key,
    response hash) pairs: the same answers to the same requests, the same
    manifest, however often or in whatever order they were asked for."""
    lines = "".join(f"{k}\t{h}\n" for k, h in sorted(set(served)))
    return hashlib.sha256(lines.encode()).hexdigest()


def _now() -> str:
    # Real time where it runs: the workload VM's clock is the fake date, which is
    # fine here (it only orders misses), and says when in the run they happened.
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


class EgressCache:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.executescript(SCHEMA)
        have = {r[1] for r in self.db.execute("PRAGMA table_info(misses)")}
        if "body" not in have:  # made before bodies counted
            self.db.execute("ALTER TABLE misses ADD COLUMN body TEXT NOT NULL DEFAULT ''")
        if "headers" not in have:  # made before headers were kept
            self.db.execute("ALTER TABLE misses ADD COLUMN headers TEXT NOT NULL DEFAULT '{}'")
        if "response_hash" not in {r[1] for r in self.db.execute("PRAGMA table_info(entries)")}:  # made before hashes
            self.db.execute("ALTER TABLE entries ADD COLUMN response_hash TEXT")
            rows = self.db.execute("SELECT cache, key, status_code, content_type, body FROM entries").fetchall()
            self.db.executemany(
                "UPDATE entries SET response_hash = ? WHERE cache = ? AND key = ?",
                [(response_hash(st, ct, b), c, k) for c, k, st, ct, b in rows],
            )
        self.lock = threading.Lock()

    def recording(self, default: str) -> str:
        """The recording runs are served from (`default` until one is chosen)."""
        row = self.db.execute("SELECT value FROM settings WHERE name = 'recording'").fetchone()
        return default if row is None else row[0]

    def use(self, name: str) -> None:
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO settings VALUES ('recording', ?)", (name,))

    def get(self, cache: str, key: str) -> dict | None:
        row = self.db.execute(
            "SELECT status_code, body, content_type, response_hash FROM entries WHERE cache = ? AND key = ?",
            (cache, key),
        ).fetchone()
        names = ("status_code", "body", "content_type", "response_hash")
        return None if row is None else dict(zip(names, row, strict=True))

    def put(self, cache: str, key: str, status_code: int, body: str, content_type: str | None, meta: dict) -> None:
        with self.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO entries "
                "(cache, key, status_code, body, content_type, meta, stored_at, response_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cache,
                    key,
                    status_code,
                    body,
                    content_type,
                    json.dumps(meta),
                    _now(),
                    response_hash(status_code, content_type, body),
                ),  # fmt: skip
            )
            self.db.execute("DELETE FROM misses WHERE cache = ? AND key = ?", (cache, key))

    def drop(self, cache: str, keys: list[str]) -> int:
        """Forget entries (e.g. one recorded from a wrong route): the next run misses them again."""
        with self.lock:
            cur = self.db.executemany("DELETE FROM entries WHERE cache = ? AND key = ?", [(cache, k) for k in keys])
            return cur.rowcount

    def entries(self, cache: str) -> list[dict]:
        rows = self.db.execute(
            "SELECT key, status_code, LENGTH(body), stored_at, response_hash FROM entries WHERE cache = ? ORDER BY key",
            (cache,),
        ).fetchall()
        names = ("key", "status_code", "bytes", "stored_at", "response_hash")
        return [dict(zip(names, r, strict=True)) for r in rows]

    def hit(self, cache: str, key: str, response_hash: str) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO served (cache, key, response_hash, at) VALUES (?, ?, ?, ?)",
                (cache, key, response_hash, _now()),
            )

    def miss(self, cache: str, key: str, env: str, body: str = "", headers: dict | None = None) -> None:
        now = _now()
        with self.lock:
            self.db.execute(
                "INSERT INTO served (cache, key, response_hash, at) VALUES (?, ?, NULL, ?)", (cache, key, now)
            )
            self.db.execute(
                "INSERT INTO misses (cache, key, env, count, first_seen, last_seen, body, headers) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?) "
                "ON CONFLICT (cache, key) DO UPDATE SET count = count + 1, last_seen = excluded.last_seen",
                (cache, key, env, now, now, body, json.dumps(headers or {})),
            )

    def wrote(self, cache: str, method: str, path: str) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO writes VALUES (?, ?, ?, 1, ?) ON CONFLICT (cache, method, path) "
                "DO UPDATE SET count = count + 1, last_seen = excluded.last_seen",
                (cache, method, path, _now()),
            )

    def seq(self) -> int:
        """The number of the last hit or miss (0 before any)."""
        return self.db.execute("SELECT COALESCE(MAX(seq), 0) FROM served").fetchone()[0]

    def hits(self, cache: str, after: int = 0) -> list[dict]:
        rows = self.db.execute(
            "SELECT seq, key, response_hash FROM served "
            "WHERE cache = ? AND seq > ? AND response_hash IS NOT NULL ORDER BY seq",
            (cache, after),
        ).fetchall()
        return [dict(zip(("seq", "key", "response_hash"), r, strict=True)) for r in rows]

    def misses(self, cache: str, after: int | None = None) -> list[dict]:
        """What's still missing; with `after`, only what was missed since that `seq`."""
        since = (
            ""
            if after is None
            else " AND key IN (SELECT key FROM served WHERE cache = ? AND seq > ? AND response_hash IS NULL)"
        )
        rows = self.db.execute(
            "SELECT key, env, count, first_seen, last_seen, body, headers FROM misses WHERE cache = ?"
            + since
            + " ORDER BY first_seen",
            (cache,) if after is None else (cache, cache, after),
        ).fetchall()
        names = ("key", "env", "count", "first_seen", "last_seen", "body", "headers")
        return [{**dict(zip(names, r, strict=True)), "headers": json.loads(r[6])} for r in rows]

    def stats(self, cache: str) -> dict:
        one = lambda q: self.db.execute(q, (cache,)).fetchone()[0]  # noqa: E731
        return {
            "cache": cache,
            "entries": one("SELECT COUNT(*) FROM entries WHERE cache = ?"),
            "misses": one("SELECT COUNT(*) FROM misses WHERE cache = ?"),
            "writes_faked": one("SELECT COALESCE(SUM(count), 0) FROM writes WHERE cache = ?"),
            "seq": self.seq(),
        }


class CacheGet(BaseModel):
    cache_name: str | None = None
    key: str


class CacheDrop(BaseModel):
    cache_name: str | None = None
    keys: list[str]


class Recording(BaseModel):
    name: str


class CachePut(BaseModel):
    cache_name: str | None = None
    key: str
    status_code: int
    body: str = ""
    content_type: str | None = None
    meta: dict = {}


def create_egress_app(cache: EgressCache, default_cache: str, read_posts: tuple[str, ...] = ()) -> FastAPI:
    """`default_cache`: the recording served until another is chosen. `read_posts`:
    upstream path prefixes whose POSTs are reads (cached, not faked)."""
    app = FastAPI(title="egress cache")

    def active() -> str:
        return cache.recording(default_cache)

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.api_route("/proxy/{env}/{upstream_path:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(env: str, upstream_path: str, request: Request) -> Response:
        method, name = request.method.upper(), active()
        if method not in READS and not (method == "POST" and upstream_path.startswith(read_posts)):
            cache.wrote(name, method, upstream_path)
            return Response(status_code=200)  # faked, as the recording proxy fakes them
        body = await request.body()
        key = cache_key(method, upstream_path, list(request.query_params.multi_items()), body)
        hit = cache.get(name, key)
        if hit is None:
            kept = {h: request.headers[h] for h in KEPT_HEADERS if h in request.headers}
            cache.miss(name, key, env, body.decode(errors="replace"), kept)
            return Response(
                content=f"egress cache: miss for {key} (precache it; a run never reaches the real thing)".encode(),
                status_code=511,
                media_type="text/plain",
            )
        cache.hit(name, key, hit["response_hash"])
        body = b"" if method == "HEAD" else hit["body"].encode()
        return Response(content=body, status_code=hit["status_code"], media_type=hit["content_type"])

    @app.get("/recording")
    def recording():
        return {"name": active()}

    @app.put("/recording")
    def use(req: Recording):
        cache.use(req.name)
        return {"name": req.name}

    # The toolbox API the recording proxy talks to.

    @app.post("/cache/put")
    def put(req: CachePut):
        cache.put(req.cache_name or active(), req.key, req.status_code, req.body, req.content_type, req.meta)
        return {"ok": True}

    @app.post("/cache/get")
    def get(req: CacheGet):
        hit = cache.get(req.cache_name or active(), req.key)
        return {"hit": False, "detail": f"cache miss for {req.key}"} if hit is None else {"hit": True, **hit}

    @app.post("/cache/drop")
    def drop(req: CacheDrop):
        return {"dropped": cache.drop(req.cache_name or active(), req.keys)}

    @app.get("/entries")
    def entries(cache_name: str | None = None):
        return cache.entries(cache_name or active())

    @app.get("/misses")
    def misses(cache_name: str | None = None, after: int | None = None):
        return cache.misses(cache_name or active(), after)

    @app.get("/hits")
    def hits(cache_name: str | None = None, after: int = 0):
        return cache.hits(cache_name or active(), after)

    @app.get("/manifest")
    def manifest_(cache_name: str | None = None, after: int = 0):
        """The manifest over what was served since `after`, and how many entries it covers."""
        pairs = {(h["key"], h["response_hash"]) for h in cache.hits(cache_name or active(), after)}
        return {"manifest": manifest(pairs), "entries": len(pairs)}

    @app.get("/stats")
    def stats(cache_name: str | None = None):
        return cache.stats(cache_name or active())

    return app


def main(argv: list[str] | None = None) -> None:
    import uvicorn

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--cache", default="fermenter", help="recording served until `PUT /recording` chooses another")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument(
        "--read-post", action="append", default=[], metavar="PREFIX",
        help="upstream path prefix whose POSTs are reads (cached like GETs); repeatable",
    )  # fmt: skip
    a = p.parse_args(argv)
    uvicorn.run(create_egress_app(EgressCache(a.db), a.cache, tuple(a.read_post)), host=a.host, port=a.port)


if __name__ == "__main__":
    main()
