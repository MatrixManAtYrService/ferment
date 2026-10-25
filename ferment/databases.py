"""SQL databases a fermenter's workload runs on.

Both speak the MySQL protocol on 127.0.0.1:<port>. On a split fermenter
(docs/split-vms.md) MySQL also listens on the workload VM's private network
(`bind`), and the control VM reaches it there (`reached_at`): checkpoints, SQL.

- MySQL 8.4 (the default): the line production runs, so services, their
  migrations, and their queries behave as they do in production. Root over
  TCP with no password (a loopback-only sandbox). Its history is kept by
  checkpoints are snapshots of its data dir's volume (ferment.volume).
- MariaDB: the older fallback; no archive.

No TLS where a client might check validity dates: a server's self-signed cert
is minted at the fermenter's (fake) date, which a client elsewhere would see
as not yet valid.
"""

from __future__ import annotations

import contextlib
import copy
import hashlib
import os
import shlex
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pymysql

from ferment.procompose import Process

# Schemas every server has that aren't workload data.
SYSTEM_SCHEMAS = {"information_schema", "mysql", "performance_schema", "sys"}


class DatabaseError(RuntimeError):
    pass


@dataclass
class Database:
    kind: str
    state_dir: Path
    port: int = 3306

    @property
    def client(self) -> str:
        """Shell prefix for a CLI client against this server."""
        raise NotImplementedError

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


class MySQL(Database):
    """MySQL 8.4 from FERMENT_MYSQL_BIN (by path: MariaDB's packages also
    install `mysql`/`mysqldump` names).

    `host`: where clients connect. `bind`: where the server listens; off
    loopback, root may also log in from elsewhere (no password: the sandbox is
    a private network between a fermenter's VMs)."""

    def __init__(
        self,
        state_dir: Path,
        port: int = 3306,
        bin_dir: str | None = None,
        host: str = "127.0.0.1",
        bind: str = "127.0.0.1",
    ):
        super().__init__("mysql", state_dir, port)
        self.bin = bin_dir if bin_dir is not None else os.environ.get("FERMENT_MYSQL_BIN", "")
        self.host = host
        self.bind = bind

    def reached_at(self, host: str) -> MySQL:
        """The same server, as a client on another machine reaches it."""
        other = copy.copy(self)
        other.host = host
        return other

    @property
    def data_dir(self) -> Path:
        return self.state_dir / "mysql84"

    def tool(self, name: str) -> str:
        return str(Path(self.bin) / name) if self.bin else name

    def client_args(self, port: int | None = None) -> list[str]:
        return [self.tool("mysql"), "--no-defaults", "-h", self.host, "-P", str(port or self.port), "-uroot"]

    @property
    def client(self) -> str:
        return " ".join(shlex.quote(a) for a in self.client_args())

    @property
    def _init_file(self) -> Path:
        return self.state_dir / "mysql84-init.sql"

    def _init_script(self) -> str:
        """Initialize the data dir once; root reachable over TCP (init file)."""
        d = shlex.quote(str(self.data_dir))
        init = shlex.quote(str(self._init_file))
        mysqld = shlex.quote(self.tool("mysqld"))
        hosts = ["127.0.0.1"] + (["%"] if self.bind != "127.0.0.1" else [])
        sql = " ".join(
            f"CREATE USER IF NOT EXISTS 'root'@'{h}'; GRANT ALL ON *.* TO 'root'@'{h}' WITH GRANT OPTION;"
            for h in hosts
        )
        return (
            f"mkdir -p {shlex.quote(str(self.state_dir))}; echo {shlex.quote(sql)} > {init}; "
            f"[ -d {d}/mysql ] || {mysqld} --no-defaults --initialize-insecure --datadir={d} >/dev/null 2>&1"
        )

    def _socket(self, name: str) -> Path:
        """Unix socket paths max out around 104 bytes; fall back to /tmp."""
        path = self.state_dir / f"{name}.sock"
        if len(str(path)) < 100:
            return path
        digest = hashlib.sha256(str(path).encode()).hexdigest()[:12]
        return Path(tempfile.gettempdir()) / f"ferment-{digest}.sock"

    def _server_args(self, port: int, socket: Path) -> list[str]:
        return [
            self.tool("mysqld"), "--no-defaults", f"--datadir={self.data_dir}", f"--port={port}",
            f"--socket={socket}", f"--bind-address={self.bind}", "--mysqlx=OFF",
            f"--pid-file={self.state_dir / f'mysql84.{port}.pid'}", f"--init-file={self._init_file}",
            # The fake clock jumps (a month, or back to a checkpoint): don't let
            # it expire binlogs, which are how checkpoints get diffed.
            "--binlog-expire-logs-auto-purge=OFF",
        ]  # fmt: skip

    def processes(self) -> list[Process]:
        server = " ".join(shlex.quote(a) for a in self._server_args(self.port, self._socket("mysql84")))
        return [
            Process(
                "mysql",
                f"{self._init_script()} && exec {server}",
                readiness_exec=f"{self.client} -e 'SELECT 1'",
                readiness_initial_delay=2,
            )
        ]

    # -- for the archive -----------------------------------------------------------

    def connect(self, port: int | None = None, database: str | None = None) -> pymysql.Connection:
        return pymysql.connect(host=self.host, port=port or self.port, user="root", database=database, autocommit=True)

    def databases(self, port: int | None = None) -> list[str]:
        with self.connect(port) as conn, conn.cursor() as cur:
            cur.execute("SHOW DATABASES")
            return [row[0] for row in cur.fetchall() if row[0].lower() not in SYSTEM_SCHEMAS]

    @contextlib.contextmanager
    def offline_server(self, port: int = 3399) -> Iterator[int]:
        """Run mysqld on the live data dir while the workload is stopped (to
        dump or restore it); yields its port."""
        subprocess.run(["bash", "-c", self._init_script()], check=True)
        log_path = self.state_dir / "mysql84-offline.log"
        with log_path.open("a") as log:
            proc = subprocess.Popen(self._server_args(port, self._socket(f"mysql84-{port}")), stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 120
            while subprocess.run([*self.client_args(port), "-e", "SELECT 1"], capture_output=True).returncode:
                if proc.poll() is not None or time.monotonic() > deadline:
                    raise DatabaseError(f"mysqld wouldn't start; see {log_path}")
                time.sleep(0.5)
            yield port
        finally:
            proc.terminate()
            proc.wait(timeout=120)

    def _query(self, sql: str) -> str:
        p = subprocess.run([*self.client_args(), "-N", "-B", "-e", sql], capture_output=True, text=True)
        if p.returncode != 0:
            raise DatabaseError(f"{sql}: {p.stderr.strip()}")
        return p.stdout

    def before_snapshot(self, timeout: float = 60.0) -> str | None:
        """Get a running server ready for a snapshot of its data dir: dump the
        buffer pool's page list (so a restore starts warm), and say where the
        binlog is (`file:position`), which is where this checkpoint's changes
        end in it."""
        self._query("SET GLOBAL innodb_buffer_pool_dump_now = ON")
        deadline = time.monotonic() + timeout
        while "completed" not in self._query("SHOW STATUS LIKE 'Innodb_buffer_pool_dump_status'"):
            if time.monotonic() > deadline:
                break  # a cold restore, not a wrong one
            time.sleep(0.2)
        row = self._query("SHOW BINARY LOG STATUS").split("\t")
        return f"{row[0]}:{row[1]}" if len(row) > 1 else None

    def binlog_position(self) -> str | None:
        """The same, for a stopped server: the newest binlog and its length."""
        index = self.data_dir / "binlog.index"
        if not index.exists():
            return None
        names = [line.strip() for line in index.read_text().splitlines() if line.strip()]
        if not names:
            return None
        newest = self.data_dir / Path(names[-1]).name
        return f"{newest.name}:{newest.stat().st_size}" if newest.exists() else None

    def load(self, database: str, port: int, sql: str, *, replace: bool) -> None:
        head = f"DROP DATABASE IF EXISTS `{database}`;\nCREATE DATABASE `{database}`;\n" if replace else ""
        sql = f"{head}USE `{database}`;\n{sql}"
        result = subprocess.run(self.client_args(port), input=sql, text=True, capture_output=True)
        if result.returncode != 0:
            raise DatabaseError(f"loading {database}: {result.stderr.strip()[-2000:]}")


def make_database(kind: str, state_dir: Path, port: int = 3306, bind: str = "127.0.0.1") -> Database:
    """`bind`: MySQL's listen address (MariaDB stays on loopback: a split
    fermenter needs MySQL)."""
    if kind == "mysql":
        return MySQL(state_dir, port, bind=bind)
    if kind == "mariadb":
        return MariaDB(state_dir, port)
    raise ValueError(f"unknown database {kind!r} (mysql, mariadb)")
