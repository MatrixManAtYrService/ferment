"""SQL databases a fermenter's workload can run on.

Both speak the MySQL protocol on 127.0.0.1:<port>, so services don't care
which one they got. Both run inside the faked tree:

- MariaDB reads the clock through libc: libfaketime covers it.
- Dolt is Go, which libfaketime can't reach; it must be built with ferment's
  patched Go toolchain (nix/fakeable-go.nix: `fermentFakeable pkgs.dolt`),
  which honors the same FAKETIME offset.

Dolt adds version control: checkpoints (a commit across every database),
history, diffs between checkpoints, and hard restores to one. Commits made
under a fake clock carry fake dates, so history reads in the workload's time.
"""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from pathlib import Path

import pymysql

from ferment.procompose import Process

# Schemas every server has that aren't workload data.
SYSTEM_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}


@dataclass
class Database:
    kind: str
    state_dir: Path
    port: int = 3306

    @property
    def client(self) -> str:
        """Shell prefix for the mariadb CLI against this server (no TLS).

        No TLS anywhere: a server's self-signed cert is minted by the FAKE
        clock, so real-time clients would reject it as not yet valid.
        """
        return f"mariadb --skip-ssl -h 127.0.0.1 -P {self.port} -u root"

    def processes(self) -> list[Process]:
        raise NotImplementedError

    @property
    def versioned(self) -> bool:
        return False


class MariaDB(Database):
    def __init__(self, state_dir: Path, port: int = 3306):
        super().__init__("mariadb", state_dir, port)

    @property
    def socket(self) -> Path:
        return self.state_dir / "mysql.sock"

    @property
    def client(self) -> str:
        # Over the socket, authenticated as the OS user (mariadb-install-db
        # run as a non-root user grants that user unix_socket access).
        return f"mariadb --skip-ssl --socket={shlex.quote(str(self.socket))}"

    def processes(self) -> list[Process]:
        data = shlex.quote(str(self.state_dir / "mysql"))
        sock = shlex.quote(str(self.socket))
        return [
            Process(
                "mariadb",
                f"[ -d {data}/mysql ] || mariadb-install-db --datadir={data} --skip-test-db >/dev/null; "
                f"exec mariadbd --datadir={data} --socket={sock} --port={self.port} "
                "--bind-address=127.0.0.1 --skip-ssl",
                readiness_exec=f"{self.client} -e 'SELECT 1'",
            )
        ]


class Dolt(Database):
    def __init__(self, state_dir: Path, port: int = 3306):
        super().__init__("dolt", state_dir, port)

    @property
    def data_dir(self) -> Path:
        return self.state_dir / "dolt"

    @property
    def versioned(self) -> bool:
        return True

    def connect(self, database: str | None = None) -> pymysql.Connection:
        return pymysql.connect(host="127.0.0.1", port=self.port, user="root", database=database, autocommit=True)

    def databases(self) -> list[str]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return [row[0] for row in cur.fetchall() if row[0].lower() not in SYSTEM_SCHEMAS]

    def processes(self) -> list[Process]:
        data = shlex.quote(str(self.data_dir))
        return [
            Process(
                "dolt",
                f"mkdir -p {data} && cd {data} && "
                "dolt config --global --add user.name fermenter >/dev/null 2>&1; "
                "dolt config --global --add user.email fermenter@localhost >/dev/null 2>&1; "
                "dolt config --global --add versioncheck.disabled true >/dev/null 2>&1; "
                f"exec dolt sql-server --host 127.0.0.1 --port {self.port} --data-dir {data}",
                readiness_exec=f"{self.client} -e 'SELECT 1'",
            )
        ]

    # -- checkpoints ----------------------------------------------------------

    def checkpoint(self, message: str, tag: str | None = None) -> dict[str, str | None]:
        """Commit every database's working set; returns {database: commit hash}.

        A database with nothing to commit keeps its current head (and still
        gets the tag, so a tag always names a consistent cross-database state).
        """
        out: dict[str, str | None] = {}
        for db in self.databases():
            with self.connect(db) as conn, conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM dolt_status")
                if cur.fetchone()[0]:
                    cur.execute("CALL DOLT_COMMIT('-Am', %s)", (message,))
                cur.execute("SELECT HASHOF('HEAD')")
                head = cur.fetchone()[0]
                if tag:
                    cur.execute("CALL DOLT_TAG(%s, 'HEAD')", (tag,))
                out[db] = head
        return out

    def log(self, limit: int = 20) -> dict[str, list[dict]]:
        out = {}
        for db in self.databases():
            with self.connect(db) as conn, conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute("SELECT commit_hash, date, message FROM dolt_log LIMIT %s", (limit,))
                commits = cur.fetchall()
                cur.execute("SELECT tag_name, tag_hash FROM dolt_tags")
                tags: dict[str, list[str]] = {}
                for t in cur.fetchall():
                    tags.setdefault(t["tag_hash"], []).append(t["tag_name"])
                out[db] = [{**c, "date": str(c["date"]), "tags": tags.get(c["commit_hash"], [])} for c in commits]
        return out

    def diff(self, from_ref: str, to_ref: str = "WORKING") -> dict[str, list[dict]]:
        """Per database: which tables changed between two refs, and how much."""
        out = {}
        for db in self.databases():
            with self.connect(db) as conn, conn.cursor(pymysql.cursors.DictCursor) as cur:
                try:
                    cur.execute("SELECT * FROM DOLT_DIFF_STAT(%s, %s)", (from_ref, to_ref))
                except pymysql.err.MySQLError as e:  # ref unknown in this database
                    out[db] = [{"error": str(e)}]
                    continue
                out[db] = [{k: (str(v) if v is not None else None) for k, v in row.items()} for row in cur.fetchall()]
        return out

    def restore(self, ref: str) -> dict[str, str]:
        """Hard-reset every database to `ref` (a hash or tag), discarding later
        changes. Services that cache state should be restarted afterwards."""
        out = {}
        for db in self.databases():
            with self.connect(db) as conn, conn.cursor() as cur:
                cur.execute("CALL DOLT_RESET('--hard', %s)", (ref,))
                cur.execute("SELECT HASHOF('HEAD')")
                out[db] = cur.fetchone()[0]
        return out


def make_database(kind: str, state_dir: Path, port: int = 3306) -> Database:
    if kind == "mariadb":
        return MariaDB(state_dir, port)
    if kind == "dolt":
        return Dolt(state_dir, port)
    raise ValueError(f"unknown database {kind!r} (mariadb, dolt)")
