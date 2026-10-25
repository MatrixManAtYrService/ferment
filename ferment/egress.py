"""A workload's outbound HTTP, served only from a cache.

Services call out to things a fermenter must never reach during a run (a
production API behind a VPN). Principle: a run serves only cached data, so
it's repeatable and nothing it does touches the real thing. This server
stands where those services expect the outbound proxy and:

- answers GET/HEAD from the cache, by the request's key; a miss fails
  closed (511, "precache it") and is remembered, so a run's misses are the
  list of what to fetch before the next one;
- fakes writes (200, no body), counting them, so a run shows what it tried
  to change;
- takes entries through the same toolbox API (`/cache/put`, `/cache/get`)
  the proxy that fetches them (eda's egress, in record mode) writes to.

Keys are the outbound proxy's own: `METHOD upstream_path?sorted-query`, for
requests to `/proxy/<env>/<upstream_path>`, plus ` #<sha256(body)[:16]>`
when a read carries a body (some are GETs whose arguments are JSON),
computed from the incoming request so a recording and a replay agree. A
miss keeps its body, so precaching can send the same request.

The cache lives in SQLite outside the workload's snapshot volume: restoring
a checkpoint keeps what was precached.

    python -m ferment.egress --port 44445 --db state/egress/egress.sqlite --cache fermenter
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import sqlite3
import threading
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import Response
from pydantic import BaseModel

READS = {"GET", "HEAD"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS entries (
    cache TEXT NOT NULL, key TEXT NOT NULL, status_code INTEGER NOT NULL, body TEXT NOT NULL,
    content_type TEXT, meta TEXT, stored_at TEXT NOT NULL, PRIMARY KEY (cache, key));
CREATE TABLE IF NOT EXISTS misses (
    cache TEXT NOT NULL, key TEXT NOT NULL, env TEXT NOT NULL, count INTEGER NOT NULL,
    first_seen TEXT NOT NULL, last_seen TEXT NOT NULL, body TEXT NOT NULL DEFAULT '', PRIMARY KEY (cache, key));
CREATE TABLE IF NOT EXISTS writes (
    cache TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL, count INTEGER NOT NULL,
    last_seen TEXT NOT NULL, PRIMARY KEY (cache, method, path));
"""


def cache_key(method: str, upstream_path: str, query: list[tuple[str, str]], body: bytes = b"") -> str:
    """As eda's egress proxy computes it (its _cache_key): the one form a recording
    and a replay both produce."""
    q = urlencode(sorted(query))
    key = f"{method.upper()} {upstream_path}" + (f"?{q}" if q else "")
    return key + (f" #{hashlib.sha256(body).hexdigest()[:16]}" if body else "")


def _now() -> str:
    # Real time where it runs: the workload VM's clock is the fake date, which is
    # fine here (it only orders misses), and says when in the run they happened.
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


class EgressCache:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.executescript(SCHEMA)
        if "body" not in {r[1] for r in self.db.execute("PRAGMA table_info(misses)")}:  # made before bodies counted
            self.db.execute("ALTER TABLE misses ADD COLUMN body TEXT NOT NULL DEFAULT ''")
        self.lock = threading.Lock()

    def get(self, cache: str, key: str) -> dict | None:
        row = self.db.execute(
            "SELECT status_code, body, content_type FROM entries WHERE cache = ? AND key = ?", (cache, key)
        ).fetchone()
        return None if row is None else {"status_code": row[0], "body": row[1], "content_type": row[2]}

    def put(self, cache: str, key: str, status_code: int, body: str, content_type: str | None, meta: dict) -> None:
        with self.lock:
            self.db.execute(
                "INSERT OR REPLACE INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cache, key, status_code, body, content_type, json.dumps(meta), _now()),
            )
            self.db.execute("DELETE FROM misses WHERE cache = ? AND key = ?", (cache, key))

    def drop(self, cache: str, keys: list[str]) -> int:
        """Forget entries (e.g. one recorded from a wrong route): the next run misses them again."""
        with self.lock:
            cur = self.db.executemany("DELETE FROM entries WHERE cache = ? AND key = ?", [(cache, k) for k in keys])
            return cur.rowcount

    def entries(self, cache: str) -> list[dict]:
        rows = self.db.execute(
            "SELECT key, status_code, LENGTH(body), stored_at FROM entries WHERE cache = ? ORDER BY key", (cache,)
        ).fetchall()
        return [dict(zip(("key", "status_code", "bytes", "stored_at"), r, strict=True)) for r in rows]

    def miss(self, cache: str, key: str, env: str, body: str = "") -> None:
        now = _now()
        with self.lock:
            self.db.execute(
                "INSERT INTO misses (cache, key, env, count, first_seen, last_seen, body) VALUES (?, ?, ?, 1, ?, ?, ?) "
                "ON CONFLICT (cache, key) DO UPDATE SET count = count + 1, last_seen = excluded.last_seen",
                (cache, key, env, now, now, body),
            )

    def wrote(self, cache: str, method: str, path: str) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO writes VALUES (?, ?, ?, 1, ?) ON CONFLICT (cache, method, path) "
                "DO UPDATE SET count = count + 1, last_seen = excluded.last_seen",
                (cache, method, path, _now()),
            )

    def misses(self, cache: str) -> list[dict]:
        rows = self.db.execute(
            "SELECT key, env, count, first_seen, last_seen, body FROM misses WHERE cache = ? ORDER BY first_seen",
            (cache,),
        ).fetchall()
        return [dict(zip(("key", "env", "count", "first_seen", "last_seen", "body"), r, strict=True)) for r in rows]

    def stats(self, cache: str) -> dict:
        one = lambda q: self.db.execute(q, (cache,)).fetchone()[0]  # noqa: E731
        return {
            "cache": cache,
            "entries": one("SELECT COUNT(*) FROM entries WHERE cache = ?"),
            "misses": one("SELECT COUNT(*) FROM misses WHERE cache = ?"),
            "writes_faked": one("SELECT COALESCE(SUM(count), 0) FROM writes WHERE cache = ?"),
        }


class CacheGet(BaseModel):
    cache_name: str | None = None
    key: str


class CacheDrop(BaseModel):
    cache_name: str | None = None
    keys: list[str]


class CachePut(BaseModel):
    cache_name: str | None = None
    key: str
    status_code: int
    body: str = ""
    content_type: str | None = None
    meta: dict = {}


def create_egress_app(cache: EgressCache, default_cache: str) -> FastAPI:
    app = FastAPI(title="egress cache")

    @app.get("/health")
    def health():
        return {"healthy": True}

    @app.api_route("/proxy/{env}/{upstream_path:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"])
    async def proxy(env: str, upstream_path: str, request: Request) -> Response:
        method = request.method.upper()
        if method not in READS:
            cache.wrote(default_cache, method, upstream_path)
            return Response(status_code=200)  # faked, as the recording proxy fakes them
        body = await request.body()
        key = cache_key(method, upstream_path, list(request.query_params.multi_items()), body)
        hit = cache.get(default_cache, key)
        if hit is None:
            cache.miss(default_cache, key, env, body.decode(errors="replace"))
            return Response(
                content=f"egress cache: miss for {key} (precache it; a run never reaches the real thing)".encode(),
                status_code=511,
                media_type="text/plain",
            )
        body = b"" if method == "HEAD" else hit["body"].encode()
        return Response(content=body, status_code=hit["status_code"], media_type=hit["content_type"])

    # The toolbox API the recording proxy talks to.

    @app.post("/cache/put")
    def put(req: CachePut):
        cache.put(req.cache_name or default_cache, req.key, req.status_code, req.body, req.content_type, req.meta)
        return {"ok": True}

    @app.post("/cache/get")
    def get(req: CacheGet):
        hit = cache.get(req.cache_name or default_cache, req.key)
        return {"hit": False, "detail": f"cache miss for {req.key}"} if hit is None else {"hit": True, **hit}

    @app.post("/cache/drop")
    def drop(req: CacheDrop):
        return {"dropped": cache.drop(req.cache_name or default_cache, req.keys)}

    @app.get("/entries")
    def entries(cache_name: str | None = None):
        return cache.entries(cache_name or default_cache)

    @app.get("/misses")
    def misses(cache_name: str | None = None):
        return cache.misses(cache_name or default_cache)

    @app.get("/stats")
    def stats(cache_name: str | None = None):
        return cache.stats(cache_name or default_cache)

    return app


def main(argv: list[str] | None = None) -> None:
    import uvicorn

    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--db", type=Path, required=True)
    p.add_argument("--cache", default="fermenter", help="cache name requests are served from")
    p.add_argument("--host", default="127.0.0.1")
    a = p.parse_args(argv)
    uvicorn.run(create_egress_app(EgressCache(a.db), a.cache), host=a.host, port=a.port)


if __name__ == "__main__":
    main()
