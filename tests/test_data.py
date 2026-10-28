import datetime as dt
import gzip
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from ferment import data
from ferment.server.app import create_app
from ferment.server.clock import GuestClock
from ferment.server.data import LOAD_PRELUDE, filter_dump, key_id, load_order
from ferment.server.time_control import TimeController

DUMP = b"""\
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN= 0;
SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ 'a1:1-5,
b2:1-9';
DROP TABLE IF EXISTS `fee`;
CREATE TABLE `fee` (`id` int);
INSERT INTO `fee` VALUES (1),(2);
/*!50003 CREATE*/ /*!50017 DEFINER=`app`@`%`*/ /*!50003 TRIGGER t BEFORE INSERT ON fee FOR EACH ROW SET @x=1 */;;
"""


def test_dumps_lose_gtids_binlog_settings_and_definers():
    out = b"".join(filter_dump(iter(DUMP.splitlines(keepends=True))))
    assert b"GTID" not in out and b"b2:1-9" not in out and b"SQL_LOG_BIN" not in out
    assert b"DEFINER" not in out and b"TRIGGER t BEFORE INSERT" in out
    assert b"INSERT INTO `fee` VALUES (1),(2);" in out


def test_schemas_load_before_tables():
    frags = [
        {"key": {"table": "b", "selector": "all"}},
        {"key": {"table": "z", "selector": "schema"}},
        {"key": {"table": "a", "selector": "all"}},
    ]
    assert [f["key"]["table"] for f in load_order(frags)] == ["z", "a", "b"]


def test_gzipped_streams_round_trip():
    chunks = [b"INSERT 1;\n" * 1000, b"INSERT 2;\n"]
    assert gzip.decompress(b"".join(data.gzipped(chunks))) == b"".join(chunks)


class Stack:
    def processes(self):
        return []


class Supervisor:
    running = False

    def stop(self):
        self.running = False

    def launch(self, processes):
        self.running = True

    def wait_ready(self, timeout=300.0):
        return True

    def processes(self):
        return []

    def is_ready(self, p):
        return True

    def all_ready(self, procs):
        return True


class FakeMySQL:
    """Records what a load would pipe into the client."""

    def __init__(self):
        self.ran: list[tuple[list[str], bytes]] = []

    def client_args(self):
        return ["mysql", "-uroot"]


@pytest.fixture
def app(tmp_path, monkeypatch):
    system = {"t": dt.datetime.now(dt.UTC)}
    clock = GuestClock(
        tmp_path / "time.json", set_system_time=lambda when: system.update(t=when), system_now=lambda: system["t"]
    )
    tc = TimeController(Stack(), Supervisor(), tmp_path, versioned=[], clock=clock)
    db = FakeMySQL()
    with TestClient(create_app(tc, database=db)) as client:

        def route(method, url, **kw):
            return client.request(method, url.removeprefix("http://f"), **kw)

        monkeypatch.setattr(httpx, "request", route)
        monkeypatch.setattr(httpx, "get", lambda url, **kw: route("GET", url, **kw))
        monkeypatch.setattr(httpx, "post", lambda url, **kw: route("POST", url, **kw))
        loader = client.app.state.loader
        loader.run = lambda argv, chunks: db.ran.append((argv, b"".join(chunks))) or ""
        yield client, db


def _waiting(client):
    client.app.state.publisher.refresh()
    return {f["key"]: f["value"] for f in client.get("/board").json()["facts"]}.get("waiting")


def test_a_client_that_provides_data_fills_requests_and_a_dataset_loads(app):
    client, db = app
    me = {"X-Ferment-User": "me@twin"}
    for table in ("fee", "captured"):
        client.post(
            "/data-requests",
            json={"capability": "data:test", "spec": {"database": "billing", "table": table}},
            headers=me,
        )
    assert _waiting(client) == "2 data requests need someone attached who gets data from test (0m)"

    vpn = {"X-Ferment-User": "me@vpn"}
    client.post("/sessions", json={"client": "vpn", "provides": ["data:test"]}, headers=vpn)
    assert _waiting(client) is None

    def handler(spec, fragments):
        selector = "schema" if spec["table"] == "captured" else "all"
        key = {"source": "test", "database": spec["database"], "table": spec["table"], "selector": selector}
        if (found := fragments.find(key)) is not None:
            return {"fragment": found["id"], "reused": True}
        sql = DUMP if selector == "all" else b"CREATE TABLE `captured` (`id` int);\n"
        return {"fragment": fragments.store(key, [sql])["id"], "reused": False}

    f = data.DataFulfiller("http://f", {"data:test": handler})
    pending = client.get("/data-requests", params={"capability": "data:test"}).json()
    assert len(pending) == 2
    for req in pending:
        assert f._claim(req["id"])
        f.fulfill(req)
    done = [client.get(f"/data-requests/{r['id']}").json() for r in pending]
    assert {d["status"] for d in done} == {"done"}
    frags = [d["result"]["fragment"] for d in done]

    # Asked again: the fragment is found, not fetched anew.
    again = client.post(
        "/data-requests", json={"capability": "data:test", "spec": {"database": "billing", "table": "fee"}}
    ).json()
    f.fulfill(again)
    assert client.get(f"/data-requests/{again['id']}").json()["result"]["reused"] is True

    assert client.put("/data/datasets/base", json={"fragments": frags}, headers=me).status_code == 200
    (ds,) = client.get("/data/datasets").json()
    assert ds["name"] == "base" and ds["fragments"] == 2
    assert client.put("/data/datasets/bad", json={"fragments": ["nope"]}).status_code == 422

    assert client.post("/data/load/base", headers=me).status_code == 202
    state = _loaded(client)
    assert state["state"] == "done", state
    create, schema, table = db.ran
    assert b"CREATE DATABASE `billing`" in create[1]
    assert schema[0][-1] == "billing" and b"CREATE TABLE `captured`" in schema[1]  # schemas first
    assert table[1].startswith(LOAD_PRELUDE) and b"GTID" not in table[1] and b"INSERT INTO `fee`" in table[1]
    milestones = client.get("/board").json()["milestones"]
    assert any(m["group"] == "load base" and m["state"] == "done" for m in milestones)


def _loaded(client) -> dict:
    for _ in range(250):
        state = client.get("/data/load").json()
        if state["state"] != "running":
            return state
        time.sleep(0.02)
    return state


def test_a_failing_fragment_fails_the_load_and_says_which(app):
    client, db = app
    key = {"source": "test", "database": "billing", "table": "fee", "selector": "all"}
    rec = data.Fragments("http://f").store(key, [DUMP])
    client.put("/data/datasets/base", json={"fragments": [rec["id"]]})
    client.app.state.loader.run = lambda argv, chunks: "ERROR 1064: syntax" if argv[-1] == "billing" else ""
    client.post("/data/load/base")
    state = _loaded(client)
    assert state["state"] == "failed" and "billing.fee (all): ERROR 1064" in state["error"]
    assert rec["id"] == key_id(key)


def test_the_client_runner_reports_the_clients_last_error_line(tmp_path):
    from ferment.server.data import _run_client

    out = tmp_path / "got"
    assert _run_client(["sh", "-c", f"cat > {out}"], iter([b"a\n", b"b\n"])) == ""
    assert out.read_bytes() == b"a\nb\n"
    failing = ["sh", "-c", "cat >/dev/null; echo warming up >&2; echo 'ERROR 1146: no table' >&2; exit 1"]
    assert _run_client(failing, iter([b"x\n"] * 10000)) == "ERROR 1146: no table"


def test_a_sign_in_pauses_requests_until_the_person_acts(app):
    client, _ = app
    vpn = {"X-Ferment-User": "me@vpn"}
    client.post("/sessions", json={"client": "vpn", "provides": ["data:test"]}, headers=vpn)
    monkey_who = data.whoami
    try:
        data.whoami = lambda: "me@vpn"  # the fulfiller is the attached client
        signed_in = {"yes": False}

        def handler(spec, fragments):
            if not signed_in["yes"]:
                raise data.ActionNeeded("test.login", "test: sign in", "the source's session expired")
            return {"ok": True}

        f = data.DataFulfiller("http://f", {"data:test": handler})
    finally:
        data.whoami = monkey_who
    req = client.post("/data-requests", json={"capability": "data:test", "spec": {"t": 1}}, headers=vpn).json()
    assert f._claim(req["id"])
    assert f.fulfill(req) is False
    assert client.get(f"/data-requests/{req['id']}").json()["status"] == "pending"  # given back, not failed
    (action,) = client.get("/actions").json()
    assert (action["key"], action["who"]) == ("test.login", "me@vpn")
    client.app.state.publisher.refresh()
    (w,) = client.get("/board").json()["waiting"]
    assert w["kind"] == "action" and w["title"] == "test: sign in" and w["who"] == "me@vpn"

    assert f._claim(req["id"])
    assert f.fulfill(req) is False  # still not: one action, not two
    assert len(client.get("/actions").json()) == 1

    signed_in["yes"] = True
    assert f._claim(req["id"])
    assert f.fulfill(req) is True
    assert client.get(f"/data-requests/{req['id']}").json()["status"] == "done"
    assert client.get("/actions").json() == [] and "data:test" not in f.paused


def test_a_request_waits_while_its_source_is_down_here(app, monkeypatch):
    client, _ = app
    served = []
    up = {"data:test": False}
    monkeypatch.setattr(data.httpx, "get", lambda url, **kw: client.get(url.removeprefix("http://f"), **kw))
    monkeypatch.setattr(data.httpx, "post", lambda url, **kw: client.post(url.removeprefix("http://f"), **kw))
    f = data.DataFulfiller("http://f", {"data:test": lambda spec, fr: served.append(spec) or {}}, available=up.get)
    req = client.post("/data-requests", json={"capability": "data:test", "spec": {"t": 1}}).json()
    f.poll()
    assert served == [] and client.get(f"/data-requests/{req['id']}").json()["status"] == "pending"  # not claimed
    up["data:test"] = True
    f.poll()
    assert served == [{"t": 1}]


def test_tasks_report_progress_and_lapse_when_their_client_goes(app):
    from ferment.progress import Task

    client, _ = app
    with Task("http://f", "pull base", total=3) as t:
        t.update(done=1, detail="fee", force=True)
        (m,) = [m for m in client.get("/board").json()["milestones"] if m["label"] == "pull base"]
        assert (m["state"], m["done"], m["total"], m["detail"]) == ("running", 1, 3, "fee")
    (m,) = [m for m in client.get("/board").json()["milestones"] if m["label"] == "pull base"]
    assert m["state"] == "done"

    tasks = client.app.state.publisher.tasks
    clock = {"t": 0.0}
    tasks.now = lambda: clock["t"]
    tid = client.post("/tasks", json={"label": "orphan"}).json()["id"]
    clock["t"] = 1000.0
    tasks.lapse()
    (m,) = [m for m in client.get("/board").json()["milestones"] if m["label"] == "orphan"]
    assert m["state"] == "failed" and m["detail"] == "its client stopped reporting"
    assert client.patch(f"/tasks/{tid}", json={"done": 1}).status_code == 404
