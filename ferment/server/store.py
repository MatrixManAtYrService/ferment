"""The fermenter server's own records: one SQLite file on the VM.

What lives here is what the server knows that no workload database does:

- sessions     who is attached right now (multiparty from the start)
- events       every state-changing request: who, what, when (real and fake)
- checkpoints  named points in the workload's history, each spanning every
               versioned store (one external checkpoint = N internal commits)
- checkpoint_parts   store -> internal ref, per checkpoint
- builds       what was built from which commit, how, and how it went
- source_requests   code the VM needs that only an attached client can fetch
- deployments  which build of each service the workload runs

The server runs on real time, so rows carry real time; anything that happened
"in the workload" also carries the fake time it happened at.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import threading
from pathlib import Path

from ferment.server.clock import real_now

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
    """
    CREATE TABLE builds (
        id          INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,          -- the source (service) built
        commit_sha  TEXT NOT NULL,
        command     TEXT NOT NULL,
        artifact_glob TEXT,
        status      TEXT NOT NULL,          -- queued | running | succeeded | failed
        artifact    TEXT,                   -- path of what the glob matched
        log_path    TEXT NOT NULL,
        who         TEXT NOT NULL,
        requested_at TEXT NOT NULL,
        finished_at TEXT,
        error       TEXT
    );
    CREATE INDEX builds_by_input ON builds (name, commit_sha, command);
    """,
    """
    ALTER TABLE sessions ADD COLUMN fetches TEXT NOT NULL DEFAULT '[]';  -- JSON list of git hosts it can fetch from
    CREATE TABLE source_requests (
        id          INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,          -- source (service) name: where the bundle goes
        repo        TEXT NOT NULL,
        ref         TEXT NOT NULL,          -- branch, tag, or commit
        status      TEXT NOT NULL,          -- pending | claimed | done | failed
        commit_sha  TEXT,                   -- what ref resolved to (when done)
        requested_by TEXT NOT NULL,
        claimed_by  TEXT,
        requested_at TEXT NOT NULL,
        claimed_at  TEXT,
        finished_at TEXT,
        error       TEXT
    );
    """,
    """
    CREATE TABLE deployments (
        name        TEXT PRIMARY KEY,       -- service name; one running build per service
        build_id    INTEGER NOT NULL REFERENCES builds(id),
        who         TEXT NOT NULL,
        deployed_at TEXT NOT NULL
    );
    """,
]

# A claimed source request not finished in this long is up for grabs again
# (the client that claimed it went away mid-fetch).
CLAIM_TIMEOUT = dt.timedelta(minutes=10)

# A session not heard from in this long is treated as gone (attach heartbeats
# every 15s; a laptop that sleeps or loses the network just stops).
SESSION_TIMEOUT = dt.timedelta(seconds=60)


def _now() -> str:
    """Real time (the fermenter's own clock reads the workload's date)."""
    return real_now().isoformat()


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

    def open_session(self, who: str, client: str, relays: list[str], fetches: list[str] = ()) -> int:  # type: ignore[assignment]
        now = _now()
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO sessions (who, client, relays, fetches, opened_at, last_seen) VALUES (?, ?, ?, ?, ?, ?)",
                (who, client, json.dumps(relays), json.dumps(list(fetches)), now, now),
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
        cutoff = (real_now() - SESSION_TIMEOUT).isoformat()
        out = []
        for r in self._rows("SELECT * FROM sessions ORDER BY id"):
            r["relays"] = json.loads(r["relays"])
            r["fetches"] = json.loads(r["fetches"])
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

    # -- builds -----------------------------------------------------------------

    def add_build(
        self, *, name: str, commit: str, command: str, artifact_glob: str | None, who: str, log_path: str
    ) -> int:
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO builds (name, commit_sha, command, artifact_glob, status, log_path, who, requested_at) "
                "VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)",
                (name, commit, command, artifact_glob, log_path, who, _now()),
            )
            return int(cur.lastrowid)

    def update_build(self, build_id: int, **fields) -> None:
        if fields.get("status") in ("succeeded", "failed"):
            fields["finished_at"] = _now()
        cols = ", ".join(f"{k} = ?" for k in fields)
        with self.lock:
            self.db.execute(f"UPDATE builds SET {cols} WHERE id = ?", (*fields.values(), build_id))

    def build(self, build_id: int) -> dict | None:
        rows = self._rows("SELECT * FROM builds WHERE id = ?", build_id)
        return rows[0] if rows else None

    def find_build(self, name: str, commit: str, command: str) -> dict | None:
        """The latest build of exactly these inputs that succeeded or is still going."""
        rows = self._rows(
            "SELECT * FROM builds WHERE name = ? AND commit_sha = ? AND command = ? "
            "AND status IN ('queued', 'running', 'succeeded') ORDER BY id DESC LIMIT 1",
            name,
            commit,
            command,
        )
        return rows[0] if rows else None

    def builds(self, limit: int = 50) -> list[dict]:
        return self._rows("SELECT * FROM builds ORDER BY id DESC LIMIT ?", limit)

    def running_builds(self) -> list[dict]:
        return self._rows("SELECT * FROM builds WHERE status = 'running' ORDER BY id")

    def fail_unlaunched_builds(self) -> None:
        """At server start: builds still 'queued' never got a process. Running
        ones did, and outlive the server; the Builder adopts them."""
        with self.lock:
            self.db.execute(
                "UPDATE builds SET status = 'failed', finished_at = ?, error = 'server restarted before it started' "
                "WHERE status = 'queued'",
                (_now(),),
            )

    # -- source requests ----------------------------------------------------------

    def request_source(self, name: str, repo: str, ref: str, who: str) -> dict:
        """Queue a fetch for an attached client; an identical open request is reused."""
        open_ = self._rows(
            "SELECT id FROM source_requests WHERE name = ? AND repo = ? AND ref = ? "
            "AND status IN ('pending', 'claimed') ORDER BY id DESC LIMIT 1",
            name,
            repo,
            ref,
        )
        if open_:
            return self.source_request(open_[0]["id"])  # type: ignore[return-value]
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO source_requests (name, repo, ref, status, requested_by, requested_at) "
                "VALUES (?, ?, ?, 'pending', ?, ?)",
                (name, repo, ref, who, _now()),
            )
            rid = int(cur.lastrowid)
        return self.source_request(rid)  # type: ignore[return-value]

    def source_request(self, request_id: int) -> dict | None:
        rows = self._rows("SELECT * FROM source_requests WHERE id = ?", request_id)
        return rows[0] if rows else None

    def claimable_source_requests(self) -> list[dict]:
        cutoff = (real_now() - CLAIM_TIMEOUT).isoformat()
        return self._rows(
            "SELECT * FROM source_requests WHERE status = 'pending' OR (status = 'claimed' AND claimed_at < ?) "
            "ORDER BY id",
            cutoff,
        )

    def claim_source_request(self, request_id: int, who: str) -> bool:
        """Atomic: exactly one client wins a request."""
        cutoff = (real_now() - CLAIM_TIMEOUT).isoformat()
        with self.lock:
            cur = self.db.execute(
                "UPDATE source_requests SET status = 'claimed', claimed_by = ?, claimed_at = ? "
                "WHERE id = ? AND (status = 'pending' OR (status = 'claimed' AND claimed_at < ?))",
                (who, _now(), request_id, cutoff),
            )
            return cur.rowcount == 1

    def finish_source_request(self, request_id: int, *, commit: str | None = None, error: str | None = None) -> None:
        status = "done" if error is None else "failed"
        with self.lock:
            self.db.execute(
                "UPDATE source_requests SET status = ?, commit_sha = ?, error = ?, finished_at = ? WHERE id = ?",
                (status, commit, error, _now(), request_id),
            )

    # -- deployments --------------------------------------------------------------

    def deploy(self, name: str, build_id: int, who: str) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO deployments (name, build_id, who, deployed_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET build_id = excluded.build_id, who = excluded.who, "
                "deployed_at = excluded.deployed_at",
                (name, build_id, who, _now()),
            )

    def undeploy(self, name: str) -> bool:
        with self.lock:
            return self.db.execute("DELETE FROM deployments WHERE name = ?", (name,)).rowcount == 1

    def deployments(self) -> list[dict]:
        """Each deployed service with its build (commit, artifact)."""
        return self._rows(
            "SELECT d.name, d.build_id, d.who, d.deployed_at, b.commit_sha, b.artifact "
            "FROM deployments d JOIN builds b ON b.id = d.build_id ORDER BY d.name"
        )
