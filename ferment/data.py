"""An attached client getting data on the VM's behalf (ferment.server.data).

The VM can't reach some data sources (a prod database behind a VPN), but a
client attached to it can. Anyone may queue a data request (POST
/data-requests: a capability `data:<source>` and a spec); a client running a
DataFulfiller with a handler for that capability claims it, runs the handler
with its own access, and finishes it with what the handler returns.

A handler gets the spec and a `Fragments`, through which it stores what it
got (gzip'd on the way up, stored by hash) and finds what's already stored,
so it can skip data the fermenter already has.
"""

from __future__ import annotations

import sys
import threading
import zlib
from collections.abc import Callable, Iterable, Iterator

import httpx

from ferment.client import WHO_HEADER, whoami

DATA_PREFIX = "data:"


def capability(source: str) -> str:
    return f"{DATA_PREFIX}{source}"


def gzipped(chunks: Iterable[bytes], level: int = 6) -> Iterator[bytes]:
    """A gzip stream of `chunks`, produced as they come."""
    z = zlib.compressobj(level, zlib.DEFLATED, 31)  # 31: gzip header and trailer
    for chunk in chunks:
        if out := z.compress(chunk):
            yield out
    yield z.flush()


class Fragments:
    """The fermenter's fragment store, from a client."""

    def __init__(self, server_url: str, headers: dict | None = None):
        self.url = server_url
        self.headers = headers or {WHO_HEADER: whoami()}

    def find(self, key: dict) -> dict | None:
        r = httpx.post(f"{self.url}/data/fragments/find", json={"key": key}, headers=self.headers, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def store(self, key: dict, sql_chunks: Iterable[bytes]) -> dict:
        """Upload SQL text (gzip'd on the way) as the fragment `key`."""
        blob = httpx.post(
            f"{self.url}/data/blobs",
            content=gzipped(sql_chunks),
            headers=self.headers,
            timeout=httpx.Timeout(30, read=None, write=None),
        )
        blob.raise_for_status()
        r = httpx.post(
            f"{self.url}/data/fragments",
            json={"key": key, "sha256": blob.json()["sha256"]},
            headers=self.headers,
            timeout=30,
        )
        r.raise_for_status()
        return r.json()


Handler = Callable[[dict, Fragments], dict]


class DataFulfiller(threading.Thread):
    """Claims data requests this client has a handler for, one at a time."""

    POLL = 3.0

    def __init__(self, server_url: str, handlers: dict[str, Handler]):
        super().__init__(daemon=True)
        self.url = server_url
        self.handlers = handlers
        self.headers = {WHO_HEADER: whoami()}
        self.fragments = Fragments(server_url, self.headers)
        self.stop = threading.Event()

    def _say(self, msg: str) -> None:
        print(f"getting data for the fermenter: {msg}", file=sys.stderr)

    def run(self) -> None:
        while not self.stop.wait(self.POLL):
            for cap in self.handlers:
                try:
                    r = httpx.get(f"{self.url}/data-requests", params={"capability": cap}, timeout=10)
                    pending = r.json() if r.status_code == 200 else []
                except (httpx.HTTPError, ValueError):
                    continue  # tunnel not up yet, a blip, or a server without data requests
                for req in pending:
                    if self.stop.is_set():
                        return
                    if self._claim(req["id"]):
                        self.fulfill(req)

    def _claim(self, request_id: int) -> bool:
        try:
            r = httpx.post(f"{self.url}/data-requests/{request_id}/claim", headers=self.headers, timeout=10)
        except httpx.HTTPError:
            return False
        return r.status_code == 200  # 409: someone else got it

    def fulfill(self, req: dict) -> None:
        handler = self.handlers[req["capability"]]
        try:
            body = {"result": handler(req["spec"], self.fragments)}
        except Exception as e:  # report it to the requester rather than lose it here
            self._say(f"request {req['id']} failed: {e}")
            body = {"error": str(e) or type(e).__name__}
        try:
            httpx.post(
                f"{self.url}/data-requests/{req['id']}/finish", json=body, headers=self.headers, timeout=30
            ).raise_for_status()
        except httpx.HTTPError as e:
            self._say(f"couldn't report request {req['id']}: {e}")
