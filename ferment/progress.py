"""A client's long task, shown on the fermenter's board with its progress.

    with Task(url, "pull na-2026-10", total=188) as t:
        for i, table in enumerate(tables, 1):
            ...
            t.update(done=i, detail=table)

Advisory, like ferment.needs.Waiting: the board shows it, nothing depends on
it, so a server without /tasks (404) or a blip is ignored. A task the server
lost (it restarted) is opened again on the next update. Leaving the block
finishes it: done, or failed by an exception.
"""

from __future__ import annotations

import time

import httpx

from ferment.client import WHO_HEADER, whoami


class Task:
    # Report at most this often: an update per table of a pull is plenty.
    MIN_INTERVAL = 2.0

    def __init__(self, server_url: str, label: str, total: int | None = None):
        self.url = server_url
        self.label = label
        self.total = total
        self.id: str | None = None
        self.done: int | None = None
        self.detail: str | None = None
        self._sent = 0.0
        self.headers = {WHO_HEADER: whoami()}

    def _call(self, method: str, path: str, **kw) -> httpx.Response | None:
        try:
            return httpx.request(method, f"{self.url}{path}", headers=self.headers, timeout=10, **kw)
        except httpx.HTTPError:
            return None

    def _open(self) -> None:
        r = self._call("POST", "/tasks", json={"label": self.label, "total": self.total})
        self.id = r.json()["id"] if r is not None and r.status_code == 200 else None

    def __enter__(self) -> Task:
        self._open()
        return self

    def update(
        self, done: int | None = None, detail: str | None = None, total: int | None = None, force: bool = False
    ) -> None:
        """Also call it now and then with nothing new: it keeps the task from lapsing."""
        self.done = done if done is not None else self.done
        self.detail = detail if detail is not None else self.detail
        self.total = total if total is not None else self.total
        if not force and time.monotonic() - self._sent < self.MIN_INTERVAL:
            return
        if self.id is None:
            self._open()
        if self.id is None:
            return
        body = {"done": self.done, "total": self.total, "detail": self.detail}
        r = self._call("PATCH", f"/tasks/{self.id}", json=body)
        if r is not None and r.status_code == 404:  # the server forgot it (a restart): open it again
            self._open()
            if self.id is not None:
                self._call("PATCH", f"/tasks/{self.id}", json=body)
        self._sent = time.monotonic()

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.id is None:
            return
        self.update(force=True)
        ok = exc_type is None
        detail = self.detail if ok else ("stopped" if exc_type is KeyboardInterrupt else f"failed: {exc}")
        self._call("POST", f"/tasks/{self.id}/finish", json={"ok": ok, "detail": detail})
