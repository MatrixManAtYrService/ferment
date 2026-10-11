"""The fermenter server's own records: one SQLite file on the VM.

What lives here is what the server knows that no workload database does:

- sessions     who is attached right now (multiparty from the start)
- events       every state-changing request: who, what, when (real and fake)
- checkpoints  named points in the workload's history, each spanning every
               versioned store (one external checkpoint = N internal commits)
- checkpoint_parts   store -> internal ref, per checkpoint

The server runs on real time, so rows carry real time; anything that happened
"in the workload" also carries the fake time it happened at.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path

# Append-only: schema version N is MIGRATIONS[:N] applied in order.
MIGRATIONS = [
    """
    CREATE TABLE sessions (
        id          INTEGER PRIMARY KEY,
        who         TEXT NOT NULL,
        client      TEXT NOT NULL,          -- the attaching machine
        relays      TEXT NOT NULL,          -- JSON list of host:port it serves
        opened_at   TEXT NOT NULL,
        last_seen   TEXT NOT NULL,
        closed_at   TEXT
    );
    CREATE TABLE events (
        id          INTEGER PRIMARY KEY,
        real_time   TEXT NOT NULL,
        fake_time   TEXT,
        who         TEXT NOT NULL,
        action      TEXT NOT NULL,          -- e.g. "POST /time"
        status      INTEGER,
        detail      TEXT NOT NULL           -- JSON
    );
    CREATE TABLE checkpoints (
        id          INTEGER PRIMARY KEY,
        name        TEXT UNIQUE,
        message     TEXT NOT NULL,
        kind        TEXT NOT NULL,          -- "manual" | "clock-change"
        who         TEXT NOT NULL,
        real_time   TEXT NOT NULL,
        fake_time   TEXT NOT NULL,          -- the workload's clock at the checkpoint
        offset_seconds INTEGER NOT NULL
    );
    CREATE TABLE checkpoint_parts (
        checkpoint_id INTEGER NOT NULL REFERENCES checkpoints(id),
        store       TEXT NOT NULL,          -- e.g. "dolt:billing_bookkeeper"
        ref         TEXT NOT NULL,          -- that store's own commit/snapshot id
        PRIMARY KEY (checkpoint_id, store)
    );
    """,
]

# A session not heard from in this long is treated as gone (attach heartbeats
# every 15s; a laptop that sleeps or loses the network just stops).
SESSION_TIMEOUT = dt.timedelta(seconds=60)


def _now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class Store:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.lock = threading.Lock()
        self._migrate()

    def _migrate(self) -> None:
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        for i, sql in enumerate(MIGRATIONS[version:], start=version + 1):
            with self.lock:
                self.db.executescript(f"BEGIN; {sql}; PRAGMA user_version = {i}; COMMIT;")

    def _rows(self, sql: str, *args) -> list[dict]:
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]

    # -- events -----------------------------------------------------------------

    def add_event(
        self,
        who: str,
        action: str,
        *,
        status: int | None = None,
        fake_time: str | None = None,
        detail: dict | None = None,
    ) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO events (real_time, fake_time, who, action, status, detail) VALUES (?, ?, ?, ?, ?, ?)",
                (_now(), fake_time, who, action, status, json.dumps(detail or {})),
            )

    def events(self, limit: int = 50) -> list[dict]:
        rows = self._rows("SELECT * FROM events ORDER BY id DESC LIMIT ?", limit)
        return [{**r, "detail": json.loads(r["detail"])} for r in rows]

    # -- sessions ---------------------------------------------------------------

    def open_session(self, who: str, client: str, relays: list[str]) -> int:
        now = _now()
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO sessions (who, client, relays, opened_at, last_seen) VALUES (?, ?, ?, ?, ?)",
                (who, client, json.dumps(relays), now, now),
            )
            return int(cur.lastrowid)

    def touch_session(self, session_id: int) -> bool:
        with self.lock:
            cur = self.db.execute(
                "UPDATE sessions SET last_seen = ? WHERE id = ? AND closed_at IS NULL", (_now(), session_id)
            )
            return cur.rowcount == 1

    def close_session(self, session_id: int) -> None:
        with self.lock:
            self.db.execute(
                "UPDATE sessions SET closed_at = ? WHERE id = ? AND closed_at IS NULL", (_now(), session_id)
            )

    def sessions(self, include_gone: bool = False) -> list[dict]:
        cutoff = (dt.datetime.now(dt.UTC) - SESSION_TIMEOUT).isoformat()
        out = []
        for r in self._rows("SELECT * FROM sessions ORDER BY id"):
            r["relays"] = json.loads(r["relays"])
            r["active"] = r["closed_at"] is None and r["last_seen"] >= cutoff
            if include_gone or r["active"]:
                out.append(r)
        return out

    # -- checkpoints ------------------------------------------------------------

    def add_checkpoint(
        self,
        *,
        name: str | None,
        message: str,
        kind: str,
        who: str,
        fake_time: str,
        offset_seconds: int,
        parts: dict[str, str],
    ) -> dict:
        with self.lock:
            self.db.execute("BEGIN")
            try:
                cur = self.db.execute(
                    "INSERT INTO checkpoints (name, message, kind, who, real_time, fake_time, offset_seconds) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (name, message, kind, who, _now(), fake_time, offset_seconds),
                )
                cid = int(cur.lastrowid)
                self.db.executemany(
                    "INSERT INTO checkpoint_parts (checkpoint_id, store, ref) VALUES (?, ?, ?)",
                    [(cid, store, ref) for store, ref in parts.items()],
                )
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
        return self.checkpoint(str(cid))  # type: ignore[return-value]

    def next_checkpoint_id(self) -> int:
        """The id the next checkpoint will get (single writer: the controller's lock)."""
        return int(self._rows("SELECT COALESCE(MAX(id), 0) + 1 AS n FROM checkpoints")[0]["n"])

    def name_taken(self, name: str) -> bool:
        return bool(self._rows("SELECT 1 FROM checkpoints WHERE name = ?", name))

    def checkpoint(self, ref: str) -> dict | None:
        """By name, or by id."""
        rows = self._rows("SELECT * FROM checkpoints WHERE name = ?", ref)
        if not rows and ref.isdigit():
            rows = self._rows("SELECT * FROM checkpoints WHERE id = ?", int(ref))
        if not rows:
            return None
        cp = rows[0]
        cp["parts"] = {
            r["store"]: r["ref"] for r in self._rows("SELECT * FROM checkpoint_parts WHERE checkpoint_id = ?", cp["id"])
        }
        return cp

    def checkpoints(self, limit: int = 50) -> list[dict]:
        ids = [r["id"] for r in self._rows("SELECT id FROM checkpoints ORDER BY id DESC LIMIT ?", limit)]
        return [self.checkpoint(str(i)) for i in ids]  # type: ignore[misc]
