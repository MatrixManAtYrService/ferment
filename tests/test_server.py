import datetime as dt

import pytest
from fastapi.testclient import TestClient

from ferment.server.app import create_app
from ferment.server.clock import GuestClock
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

    def snapshot(self, message, tags, *, running):
        ref = f"c{len(self.commits) + 1}"
        self.commits[ref] = self.data
        self.calls.append(("snapshot", running, tags))
        return {"dolt:billing": ref}

    def restore(self, parts):
        self.data = self.commits[parts["dolt:billing"]]
        self.calls.append(("restore", parts))


@pytest.fixture
def setup(tmp_path):
    dolt = FakeDolt()
    system = {"t": dt.datetime.now(dt.UTC)}
    clock = GuestClock(
        tmp_path / "time.json", set_system_time=lambda when: system.update(t=when), system_now=lambda: system["t"]
    )
    tc = TimeController(Stack(), Supervisor(), tmp_path, versioned=[dolt], clock=clock)
    return tc, dolt


def test_store_migrates_once_and_survives_reopen(tmp_path):
    s = Store(tmp_path / "f.db")
    s.add_event("me", "POST /time", status=200)
    s2 = Store(tmp_path / "f.db")  # reopen: no re-migration, data kept
    assert s2.events()[0]["who"] == "me"


def test_clock_change_checkpoints_the_stopped_workload_at_the_old_time(setup):
    tc, dolt = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    dolt.data = 1
    out = tc.set_time(dt.datetime(2026, 10, 4, 12, tzinfo=dt.UTC), who="alice")
    cp = out["checkpoint"]
    assert cp["kind"] == "clock-change" and cp["who"] == "alice"
    assert cp["fake_time"].startswith("2026-10-03")  # where the clock was, not where it's going
    assert cp["parts"] == {"dolt:billing": "c2"}
    _, running, tags = dolt.calls[-1]
    assert running is False  # cut while stopped: consistent across stores
    assert tags == [f"ckpt-{cp['id']}"]
    assert out["fake_now"].startswith("2026-10-04")


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
        assert client.post(f"/sessions/{sid}/heartbeat").status_code == 200  # older clients send no body
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


def test_reach_reports_instead_of_raising(setup):
    tc, _ = setup
    with TestClient(create_app(tc)) as client:
        assert client.get("/reach", params={"url": "ftp://x"}).status_code == 422
        r = client.get("/reach", params={"url": "https://127.0.0.1:9/"}).json()
        assert r["reachable"] is False and "Connect" in r["error"]


def test_broadcasts_let_attached_people_see_each_other(setup, monkeypatch):
    import httpx

    from ferment.broadcasts import BroadcastWatcher

    tc, _ = setup
    with TestClient(create_app(tc)) as client:
        monkeypatch.setattr(
            httpx, "get", lambda url, **kw: client.get(url.removeprefix("http://f"), params=kw.get("params"))
        )
        bob = BroadcastWatcher("http://f", me="bob@laptop")
        assert bob.poll() == []  # starts at now: no replay of history

        alice = {"X-Ferment-User": "alice@vpn"}
        client.post("/sessions", json={"client": "vpn", "relays": ["nexus:443"]}, headers=alice)
        client.post("/time", json={"at": "2026-10-03T12:00:00Z"}, headers=alice)
        client.post("/checkpoints", json={"message": "loaded", "name": "loaded"}, headers=alice)
        client.post("/checkpoints", json={"message": "mine"}, headers={"X-Ferment-User": "bob@laptop"})

        heard = [e["message"] for e in bob.poll()]
        assert heard[0] == "alice@vpn attached from vpn (serving: nexus:443)"
        assert any("moving the clock to 2026-10-03" in m for m in heard)
        assert any("checkpointed" in m and "[loaded]" in m for m in heard)
        assert not any("mine" in m for m in heard)  # your own actions aren't echoed back
        assert bob.poll() == []  # nothing new


def test_deploy_runs_in_the_background_and_a_failing_hook_stops_it(setup, tmp_path):
    import time as _time

    tc, _ = setup
    calls = []

    def hook(name, build, say):
        calls.append(name)
        say(f"migrating {name}")
        if name == "broken":
            raise RuntimeError("migration 1.41 failed")

    store = tc.store
    for name in ("svc", "broken"):
        bid = store.add_build(name=name, commit="c" * 40, command="x", artifact_glob=None, who="me", log_path="")
        store.update_build(bid, status="succeeded", artifact="/a.jar")

    def wait(client, name):
        deadline = _time.time() + 5
        while _time.time() < deadline:
            p = client.get(f"/deployments/{name}/progress").json()
            if p["state"] in ("started", "failed"):
                return p
            _time.sleep(0.05)
        raise AssertionError(p)

    with TestClient(create_app(tc, before_deploy=hook)) as client:
        r = client.put("/deployments/svc", json={"build_id": 1})
        assert r.status_code == 202 and r.json()["state"] == "queued"
        done = wait(client, "svc")
        assert done["state"] == "started" and done["steps"] == ["migrating svc"]
        assert [d["name"] for d in client.get("/deployments").json()] == ["svc"]

        client.put("/deployments/broken", json={"build_id": 2})
        failed = wait(client, "broken")
        assert failed["state"] == "failed" and "1.41" in failed["error"]
        assert [d["name"] for d in client.get("/deployments").json()] == ["svc"]  # not deployed

        log = tc.state_dir / "logs" / "svc.log"
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text("starting\nstarted\n")
        first = client.get("/processes/svc/log").json()
        assert first["text"] == "starting\nstarted\n"
        assert client.get("/processes/svc/log", params={"offset": first["offset"]}).json()["text"] == ""


def test_attached_clients_time_corrects_the_servers_real_time(setup):
    tc, _ = setup
    with TestClient(create_app(tc)) as client:
        assert client.get("/status").json()["real_time_checked"] is None
        late = tc.clock.real_now() + dt.timedelta(minutes=10)  # the VM's clock fell 10 minutes behind
        sid = client.post("/sessions", json={"client": "laptop", "client_time": late.isoformat()}).json()["id"]
        checked = client.get("/status").json()["real_time_checked"]
        assert round(checked["error_seconds"]) == -600
        assert abs((tc.clock.real_now() - late).total_seconds()) < 1
        assert client.post(f"/sessions/{sid}/heartbeat", json={"client_time": late.isoformat()}).status_code == 200
        assert client.get("/status").json()["real_time_checked"]["samples"] == 2
