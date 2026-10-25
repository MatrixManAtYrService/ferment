"""An attached client getting data on the VM's behalf (ferment.server.data).

The VM can't reach some data sources (a prod database behind a VPN), but a
client attached to it can. Anyone may queue a data request (POST
/data-requests: a capability `data:<source>` and a spec); a client running a
DataFulfiller with a handler for that capability claims it, runs the handler
with its own access, and finishes it with what the handler returns.

A handler gets the spec and a `Fragments`, through which it stores what it
got (gzip'd on the way up, stored by hash) and finds what's already stored,
so it can skip data the fermenter already has.

When a handler can't go on until the person at this machine does something
(sign in to the source), it raises ActionNeeded. The request goes back in
the queue rather than failing, the fermenter is told what's needed (an
action: the board shows it, and this machine's attach offers to do it), and
this capability pauses until the person acts (`retry`) or a minute passes.
The next request that goes through resolves the action.
"""

from __future__ import annotations

import sys
import threading
import time
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


class ActionNeeded(Exception):
    """Raised by a handler: the person at this machine has to do something
    first. `key` names it in this client's own terms (its attach maps keys
    to the commands it runs; the fermenter never sends one)."""

    def __init__(self, key: str, title: str, detail: str = ""):
        super().__init__(title)
        self.key, self.title, self.detail = key, title, detail


class DataFulfiller(threading.Thread):
    """Claims data requests this client has a handler for, one at a time."""

    POLL = 3.0
    RETRY = 60.0  # after an ActionNeeded, try again this often until it works

    def __init__(self, server_url: str, handlers: dict[str, Handler]):
        super().__init__(daemon=True)
        self.url = server_url
        self.handlers = handlers
        self.headers = {WHO_HEADER: whoami()}
        self.fragments = Fragments(server_url, self.headers)
        self.stop = threading.Event()
        self.retry = threading.Event()  # set when the person may have acted: try paused capabilities now
        self.paused: dict[str, dict] = {}  # capability -> {"action": id, "until": monotonic time}

    def _say(self, msg: str) -> None:
        print(f"getting data for the fermenter: {msg}", file=sys.stderr)

    def run(self) -> None:
        while not self.stop.wait(self.POLL):
            if self.retry.is_set():
                self.retry.clear()
                for p in self.paused.values():
                    p["until"] = 0.0
            for cap in self.handlers:
                if cap in self.paused and time.monotonic() < self.paused[cap]["until"]:
                    continue
                try:
                    r = httpx.get(f"{self.url}/data-requests", params={"capability": cap}, timeout=10)
                    pending = r.json() if r.status_code == 200 else []
                except (httpx.HTTPError, ValueError):
                    continue  # tunnel not up yet, a blip, or a server without data requests
                for req in pending:
                    if self.stop.is_set():
                        return
                    if self._claim(req["id"]) and not self.fulfill(req):
                        break  # paused: the rest wait too

    def _claim(self, request_id: int) -> bool:
        try:
            r = httpx.post(f"{self.url}/data-requests/{request_id}/claim", headers=self.headers, timeout=10)
        except httpx.HTTPError:
            return False
        return r.status_code == 200  # 409: someone else got it

    def fulfill(self, req: dict) -> bool:
        """Serve one claimed request; False if it's waiting on the person here."""
        cap = req["capability"]
        handler = self.handlers[cap]
        try:
            body = {"result": handler(req["spec"], self.fragments)}
        except ActionNeeded as a:
            self._pause(cap, req, a)
            return False
        except Exception as e:  # report it to the requester rather than lose it here
            self._say(f"request {req['id']} failed: {e}")
            body = {"error": str(e) or type(e).__name__}
        try:
            httpx.post(
                f"{self.url}/data-requests/{req['id']}/finish", json=body, headers=self.headers, timeout=30
            ).raise_for_status()
        except httpx.HTTPError as e:
            self._say(f"couldn't report request {req['id']}: {e}")
        if "result" in body and cap in self.paused:  # it works again
            p = self.paused.pop(cap)
            if p["action"] is not None:
                self._post("DELETE", f"/actions/{p['action']}")
            self._say("going on")
        return True

    def _post(self, method: str, path: str, **kw) -> httpx.Response | None:
        try:
            return httpx.request(method, f"{self.url}{path}", headers=self.headers, timeout=10, **kw)
        except httpx.HTTPError:
            return None

    def _pause(self, cap: str, req: dict, a: ActionNeeded) -> None:
        self._post("POST", f"/data-requests/{req['id']}/release")
        r = self._post("POST", "/actions", json={"key": a.key, "title": a.title, "detail": a.detail})
        action = r.json()["id"] if r is not None and r.status_code == 200 else None
        if cap not in self.paused:
            self._say(f"{a.title}: waiting for you (see attach)")
        self.paused[cap] = {"action": action, "until": time.monotonic() + self.RETRY}
