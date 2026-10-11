"""DoltArchive against real MySQL and Dolt (skipped unless both are around:
FERMENT_MYSQL_BIN=<mysql 8.4 bin dir> and `dolt` on PATH)."""

import os
import shutil

import pytest

from ferment.archive import DoltArchive
from ferment.databases import MySQL

pytestmark = pytest.mark.skipif(
    not (os.environ.get("FERMENT_MYSQL_BIN") and shutil.which("dolt")),
    reason="needs FERMENT_MYSQL_BIN and dolt",
)

SCHEMA = """
CREATE TABLE jobs (id INT PRIMARY KEY, state VARCHAR(20), reference VARCHAR(10));
ALTER TABLE jobs ADD COLUMN note CHAR(26) AFTER reference;
CREATE TABLE audit (id INT AUTO_INCREMENT PRIMARY KEY, job INT);
CREATE TRIGGER jobs_audit AFTER INSERT ON jobs FOR EACH ROW INSERT INTO audit (job) VALUES (NEW.id);
CREATE VIEW job_stats AS
  SELECT coalesce(max((SELECT r.c WHERE state IS NULL)), 0) AS total
  FROM (SELECT state, count(*) AS c FROM jobs GROUP BY state WITH ROLLUP) r;
DELIMITER $$
CREATE PROCEDURE count_jobs(OUT n INT) BEGIN SELECT COUNT(*) INTO @n FROM jobs; SET n = @n; END $$
DELIMITER ;
INSERT INTO jobs (id, state) VALUES (1, 'ENQUEUED'), (2, 'FAILED');
"""


def q(mysql, port, sql, db="billing"):
    with mysql.connect(port, db) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_mysql_runs_dolt_remembers(tmp_path):
    mysql = MySQL(tmp_path / "state", port=3398)
    archive = DoltArchive(tmp_path / "archive", mysql)

    with mysql.offline_server(port=3398) as port:  # stands in for the live workload
        mysql.load("billing", port, SCHEMA, replace=True)
        mysql.load("scratch", port, "CREATE TABLE t (x INT);", replace=True)
        first = archive.snapshot("loaded", ["loaded"], running=True)
        mysql.load(
            "billing",
            port,
            "INSERT INTO jobs (id, state) VALUES (3, 'ENQUEUED'); DELETE FROM jobs WHERE id=2;",
            replace=False,
        )
        mysql.load("later", port, "CREATE TABLE t (x INT);", replace=True)
    second = archive.snapshot("billed", ["billed"], running=False)  # stopped: archive starts mysqld itself

    assert set(first) == {"archive:billing", "archive:scratch"}
    stats = archive.diff("loaded", "billed")["billing"]
    jobs = next(r for r in stats if r["table_name"] == "jobs")
    assert (jobs["rows_added"], jobs["rows_deleted"]) == ("1", "1")
    assert {r["diff_type"] for r in archive.row_diff("billing", "jobs", "loaded", "billed")} == {"added", "removed"}
    assert second["archive:billing"] != first["archive:billing"]

    archive.restore(first)
    with mysql.offline_server(port=3398) as port:
        assert sorted(r[0] for r in q(mysql, port, "SELECT id FROM jobs")) == [1, 2]
        assert q(mysql, port, "SELECT total FROM job_stats")[0][0] == 2  # the ROLLUP view came back
        with mysql.connect(port, "billing") as conn, conn.cursor() as cur:  # one session: @x lives in it
            cur.execute("CALL count_jobs(@x)")
            cur.execute("SELECT @x")
            assert cur.fetchone()[0] == 2  # so did the procedure
        mysql.load("billing", port, "INSERT INTO jobs (id, state) VALUES (9, 'X');", replace=False)
        assert q(mysql, port, "SELECT COUNT(*) FROM audit WHERE job = 9")[0][0] == 1  # and the trigger
        assert "later" not in mysql.databases(port)  # created after the checkpoint: gone
        assert "ferment_extra" not in [r[0] for r in q(mysql, port, "SHOW TABLES")]
