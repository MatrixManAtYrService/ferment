import datetime as dt

import pytest
from fastapi.testclient import TestClient

from ferment.server.app import create_app
from ferment.server.store import Store
from ferment.server.time_control import CheckpointError, TimeController


class Stack:
    def processes(self):
        return []


class Supervisor:
    running = False

    def stop(self):
        self.running = False

    def start(self, config):
        self.running = True

    def wait_ready(self, timeout=300.0):
        return True

    def processes(self):
        return []


class FakeDolt:
    """Versioned store whose 'data' is a counter; snapshots remember it."""

    def __init__(self):
        self.data, self.commits, self.calls = 0, {}, []

    def snapshot(self, message, tags, *, running, env):
        ref = f"c{len(self.commits) + 1}"
        self.commits[ref] = self.data
        self.calls.append(("snapshot", running, tags, env.get("FAKETIME")))
        return {"dolt:billing": ref}

    def restore(self, parts, *, env):
        self.data = self.commits[parts["dolt:billing"]]
        self.calls.append(("restore", parts))


@pytest.fixture
def setup(tmp_path):
    dolt = FakeDolt()
    tc = TimeController(Stack(), Supervisor(), "/lib/libfaketime.so.1", tmp_path, versioned=[dolt])
    return tc, dolt


def test_store_migrates_once_and_survives_reopen(tmp_path):
    s = Store(tmp_path / "f.db")
    s.add_event("me", "POST /time", status=200)
    s2 = Store(tmp_path / "f.db")  # reopen: no re-migration, data kept
    assert s2.events()[0]["who"] == "me"


def test_clock_change_checkpoints_the_stopped_workload_at_the_old_time(setup):
    tc, dolt = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    assert tc.store.checkpoints() == []  # first clock: nothing to checkpoint yet
    dolt.data = 1
    out = tc.set_time(dt.datetime(2026, 10, 4, 12, tzinfo=dt.UTC), who="alice")
    cp = out["checkpoint"]
    assert cp["kind"] == "clock-change" and cp["who"] == "alice"
    assert cp["fake_time"].startswith("2026-10-03")  # where the clock was, not where it's going
    assert cp["parts"] == {"dolt:billing": "c1"}
    _, running, tags, faketime = dolt.calls[-1]
    assert running is False  # cut while stopped: consistent across stores
    assert tags == [f"ckpt-{cp['id']}"]
    assert faketime.startswith("-")  # committed under the (old, past) fake clock


def test_restore_returns_data_and_clock_and_can_be_undone(setup):
    tc, dolt = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    dolt.data = 1
    tc.checkpoint("loaded", name="loaded", who="bob")
    dolt.data = 2
    tc.set_time(dt.datetime(2026, 10, 9, 12, tzinfo=dt.UTC))
    out = tc.restore("loaded", who="bob")
    assert dolt.data == 1
    assert out["fake_now"].startswith("2026-10-03")  # clock went back too
    undo = out["previous"]["id"]
    tc.restore(str(undo))
    assert dolt.data == 2 and tc.status()["fake_now"].startswith("2026-10-09")


def test_checkpoint_names_are_validated_and_unique(setup):
    tc, _ = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    tc.checkpoint("x", name="day1")
    for bad in ("day1", "12", "has space", "-x"):
        with pytest.raises(CheckpointError):
            tc.checkpoint("x", name=bad)
    with pytest.raises(CheckpointError, match="no checkpoint"):
        tc.restore("nope")


def test_failed_clock_change_checkpoint_leaves_workload_running(setup):
    tc, dolt = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)

    def boom(*a, **k):
        raise RuntimeError("dolt broke")

    dolt.snapshot = boom
    with pytest.raises(RuntimeError):
        tc.set_time(dt.datetime(2026, 10, 4, 12, tzinfo=dt.UTC))
    assert tc.supervisor.running
    assert tc.status()["fake_now"].startswith("2026-10-03")


def test_api_records_who_did_what_and_who_is_here(setup):
    tc, _ = setup
    with TestClient(create_app(tc)) as client:  # lifespan resumes: clock starts on real time
        hdr = {"X-Ferment-User": "carol@laptop"}
        sid = client.post("/sessions", json={"client": "laptop", "relays": ["nexus:443"]}, headers=hdr).json()["id"]
        assert client.post(f"/sessions/{sid}/heartbeat").status_code == 200
        (s,) = client.get("/sessions").json()
        assert (s["who"], s["relays"], s["active"]) == ("carol@laptop", ["nexus:443"], True)

        assert client.post("/checkpoints", json={"message": "m", "name": "1bad"}, headers=hdr).status_code == 400
        client.post("/checkpoints", json={"message": "first", "name": "first"}, headers=hdr)
        assert client.get("/checkpoints").json()[0]["name"] == "first"

        client.delete(f"/sessions/{sid}", headers=hdr)
        assert client.get("/sessions").json() == []
        actions = [(e["who"], e["action"]) for e in client.get("/events").json()]
        assert ("carol@laptop", "POST /checkpoints") in actions
        assert not any(a.endswith("/heartbeat") for _, a in actions)  # heartbeats aren't events
