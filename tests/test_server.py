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

    def launch(self, processes):
        self.running = True

    def __init__(self):
        self.procs = []  # what process-compose would report while running

    def wait_ready(self, timeout=300.0):
        return True

    def processes(self):
        return self.procs if self.running else []

    logs: dict[str, str] = {}

    def log(self, name, offset=0):
        text = self.logs.get(name, "")[offset:]
        return text, offset + len(text)

    def is_ready(self, p):
        return p["status"] == "Running" and p["is_ready"] in ("Ready", "-")

    def all_ready(self, procs):
        return bool(procs) and all(self.is_ready(p) for p in procs)


class FakeStore:
    """Versioned store whose 'data' is a counter; snapshots remember it."""

    def __init__(self):
        self.data, self.commits, self.calls = 0, {}, []

    def snapshot(self, message, tags, *, running):
        ref = f"c{len(self.commits) + 1}"
        self.commits[ref] = self.data
        self.calls.append(("snapshot", running, tags))
        return {"fake:billing": ref}

    def restore(self, parts):
        self.data = self.commits[parts["fake:billing"]]
        self.calls.append(("restore", parts))


@pytest.fixture
def setup(tmp_path):
    store = FakeStore()
    system = {"t": dt.datetime.now(dt.UTC)}
    clock = GuestClock(
        tmp_path / "time.json", set_system_time=lambda when: system.update(t=when), system_now=lambda: system["t"]
    )
    tc = TimeController(Stack(), Supervisor(), tmp_path, versioned=[store], clock=clock)
    return tc, store


def test_store_migrates_once_and_survives_reopen(tmp_path):
    s = Store(tmp_path / "f.db")
    s.add_event("me", "POST /time", status=200)
    s2 = Store(tmp_path / "f.db")  # reopen: no re-migration, data kept
    assert s2.events()[0]["who"] == "me"


def test_clock_change_checkpoints_the_stopped_workload_at_the_old_time(setup):
    tc, store = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    store.data = 1
    out = tc.set_time(dt.datetime(2026, 10, 4, 12, tzinfo=dt.UTC), who="alice")
    cp = out["checkpoint"]
    assert cp["kind"] == "clock-change" and cp["who"] == "alice"
    assert cp["fake_time"].startswith("2026-10-03")  # where the clock was, not where it's going
    assert cp["parts"] == {"fake:billing": "c2"}
    _, running, tags = store.calls[-1]
    assert running is False  # cut while stopped: consistent across stores
    assert tags == [f"ckpt-{cp['id']}"]
    assert out["fake_now"].startswith("2026-10-04")


def test_restore_returns_data_and_clock_and_can_be_undone(setup):
    tc, store = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)
    store.data = 1
    tc.checkpoint("loaded", name="loaded", who="bob")
    store.data = 2
    tc.set_time(dt.datetime(2026, 10, 9, 12, tzinfo=dt.UTC))
    out = tc.restore("loaded", who="bob")
    assert store.data == 1
    assert out["fake_now"].startswith("2026-10-03")  # clock went back too
    undo = out["previous"]["id"]
    tc.restore(str(undo))
    assert store.data == 2 and tc.status()["fake_now"].startswith("2026-10-09")


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
    tc, store = setup
    tc.set_time(dt.datetime(2026, 10, 3, 12, tzinfo=dt.UTC), wait_ready=False)

    def boom(*a, **k):
        raise RuntimeError("the store broke")

    store.snapshot = boom
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


def test_a_stage_is_found_by_its_inputs_and_keeps_what_it_returned(setup):
    tc, store = setup
    with TestClient(create_app(tc)) as client:
        assert (
            client.post("/checkpoints", json={"message": "m", "stage": "backout"}).status_code == 422
        )  # no fingerprint
        store.data = 1
        made = {"message": "stage backout", "stage": "backout", "fingerprint": "f1", "value": {"run_started": "t0"}}
        first = client.post("/checkpoints", json=made).json()
        assert (first["kind"], first["value"]) == ("stage", {"run_started": "t0"})
        store.data = 2
        client.post("/checkpoints", json={**made, "fingerprint": "f2", "value": None})
        assert [c["fingerprint"] for c in client.get("/stages/backout").json()] == ["f2", "f1"]
        (found,) = client.get("/stages/backout", params={"fingerprint": "f1"}).json()
        assert found["id"] == first["id"] and found["value"] == {"run_started": "t0"}
        assert client.get("/stages/backout", params={"fingerprint": "f3"}).json() == []
        tc.restore(str(found["id"]))
        assert store.data == 1

        hand = client.post("/checkpoints", json={"message": "by hand", "name": "day-ready"}).json()
        assert client.post("/checkpoints/nope/stage", json={"stage": "s", "fingerprint": "f"}).status_code == 404
        client.post("/checkpoints/day-ready/stage", json={"stage": "ready", "fingerprint": "r1", "value": [1]})
        (adopted,) = client.get("/stages/ready", params={"fingerprint": "r1"}).json()
        assert (adopted["id"], adopted["name"], adopted["value"]) == (hand["id"], "day-ready", [1])


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

        tc.supervisor.logs = {"svc": "starting\nstarted\n"}  # where the workload runs (ferment.agent)
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


def test_the_board_follows_what_happens(setup):
    import time as _time

    from ferment.board import Service

    tc, _ = setup
    sup = tc.supervisor
    sup.procs = [{"name": "db", "status": "Running", "is_ready": "Ready"}]

    def hook(name, build, say):
        say(f"migrating {name}")
        if name == "broken":
            raise RuntimeError("migration 1.41 failed")

    for name in ("svc", "broken"):
        bid = tc.store.add_build(name=name, commit="c" * 40, command="x", artifact_glob=None, who="me", log_path="")
        tc.store.update_build(bid, status="succeeded", artifact="/a.jar")

    def services():
        return [Service(name="fermenter", label="fermenter", port=8400, state="up"), Service(name="svc", label="svc")]

    def board(client):
        client.app.state.publisher.refresh()
        b = client.get("/board").json()
        return b, {m["label"]: m for m in b["milestones"]}, {s["name"]: s["state"] for s in b["services"]}

    def settle(client, check):
        deadline = _time.time() + 5
        while True:
            b, ms, svcs = board(client)
            if check(b, ms, svcs) or _time.time() > deadline:
                return b, ms, svcs
            _time.sleep(0.05)

    with TestClient(create_app(tc, before_deploy=hook, services=services)) as client:
        b, ms, svcs = board(client)
        assert ms["workload ready"]["state"] == "done" and ms["workload ready"]["group"] == "server started"
        assert svcs == {"fermenter": "up", "svc": "down"}
        assert {f["key"]: f["value"] for f in b["facts"]}["workload"] == "1/1 ready"

        client.post("/sessions", json={"client": "laptop"}, headers={"X-Ferment-User": "carol"})
        client.post("/time", json={"at": "2026-10-03T00:00:00Z"}, headers={"X-Ferment-User": "carol"})
        b, ms, svcs = board(client)
        clock = [m for m in b["milestones"] if m["group"].startswith("clock to 2026-10-03 00:00:00")]
        assert [m["label"] for m in clock] == ["workload stopped", "checkpointed", "clock set", "workload started",
                                               "workload ready"]  # fmt: skip
        assert {m["state"] for m in clock} == {"done"}
        assert {f["key"]: f["value"] for f in b["facts"]}["attached"] == "carol"
        assert round(b["fake_offset"] / 86400) == round((tc.clock.now() - tc.clock.real_now()).total_seconds() / 86400)
        feed = [e["message"] for e in b["feed"]]
        assert "carol is moving the clock to 2026-10-03T00:00:00+00:00 (workload restarting)" in feed
        assert all(e["fake_time"].startswith("2026-10-0") for e in b["feed"] if e["event"] == "clock_changed")

        # A deploy: migrations, a restart, then waiting for the service itself.
        sup.procs.append({"name": "svc", "status": "Running", "is_ready": "Not Ready"})
        client.put("/deployments/svc", json={"build_id": 1})
        b, ms, svcs = settle(client, lambda b, ms, svcs: ms.get("svc ready", {}).get("state") == "running")
        assert ms["prepared"]["state"] == "done" and ms["prepared"]["detail"] == "migrating svc"
        assert svcs["svc"] == "starting"
        sup.procs[-1]["is_ready"] = "Ready"
        b, ms, svcs = board(client)
        assert ms["svc ready"]["state"] == "done" and svcs["svc"] == "up"

        client.put("/deployments/broken", json={"build_id": 2})
        b, ms, svcs = settle(client, lambda b, ms, svcs: any(m["state"] == "failed" for m in b["milestones"]))
        (failed,) = [m for m in b["milestones"] if m["state"] == "failed"]
        assert failed["group"].startswith("deploy broken") and "1.41" in failed["detail"]


def test_deploying_and_undeploying_several_services_restarts_the_workload_once(setup):
    import time as _time

    tc, _ = setup
    store = tc.store
    for name in ("a", "b"):
        bid = store.add_build(name=name, commit="c" * 40, command="x", artifact_glob=None, who="me", log_path="")
        store.update_build(bid, status="succeeded", artifact="/a.jar")
    relaunches = []
    original = tc.relaunch

    def counting(*args, **kwargs):
        relaunches.append(1)
        return original(*args, **kwargs)

    tc.relaunch = counting
    with TestClient(create_app(tc)) as client:
        relaunches.clear()  # the server's own start
        r = client.put("/deployments", json={"builds": {"a": 1, "b": 2}})
        assert r.status_code == 202 and set(r.json()) == {"a", "b"}
        deadline = _time.time() + 5
        while client.get("/deployments/b/progress").json()["state"] not in ("started", "failed"):
            assert _time.time() < deadline
            _time.sleep(0.05)
        assert client.get("/deployments/a/progress").json()["state"] == "started"
        assert [d["name"] for d in client.get("/deployments").json()] == ["a", "b"]
        assert len(relaunches) == 1

        assert client.put("/deployments", json={"builds": {"a": 2}}).status_code == 422  # b's build, not a's
        assert client.delete("/deployments", params={"names": ["nope"]}).status_code == 404
        client.delete("/deployments")
        assert client.get("/deployments").json() == []
        assert len(relaunches) == 2


def test_waiting_needs_show_on_the_board_until_someone_serves_them(setup):
    tc, _ = setup

    def waiting(client):
        client.app.state.publisher.refresh()
        return {f["key"]: f["value"] for f in client.get("/board").json()["facts"]}.get("waiting")

    me = {"X-Ferment-User": "me@twin"}
    with TestClient(create_app(tc)) as client:
        assert waiting(client) is None
        need = client.post("/needs", json={"capability": "relay:nexus:443", "purpose": "fmnt apply"}, headers=me).json()
        assert waiting(client) == "fmnt apply needs someone attached who relays nexus:443 (0m)"
        client.post("/source-requests", json={"name": "svc", "repo": "git@git.corp:org/svc.git", "ref": "main"})
        assert "svc @ main needs someone attached who fetches from git.corp (0m)" in waiting(client)

        vpn = {"X-Ferment-User": "me@vpn"}
        client.post("/sessions", json={"client": "vpn", "relays": ["nexus:443"], "fetches": ["git.corp"]}, headers=vpn)
        assert waiting(client) == (
            "fmnt apply needs someone attached who relays nexus:443 (0m); me@vpn serves it, not working yet"
        )
        (n,) = client.get("/needs").json()
        assert n["served_by"] == ["me@vpn"]
        assert client.post(f"/needs/{need['id']}/heartbeat").status_code == 200
        client.delete(f"/needs/{need['id']}", headers=me)
        assert waiting(client) is None
        assert client.post(f"/needs/{need['id']}/heartbeat").status_code == 404


def test_a_claim_lapses_when_its_claimers_session_goes(setup):
    tc, _ = setup
    vpn, other = {"X-Ferment-User": "me@vpn"}, {"X-Ferment-User": "you@vpn"}
    with TestClient(create_app(tc)) as client:
        sid = client.post("/sessions", json={"client": "vpn", "fetches": ["git.corp"]}, headers=vpn).json()["id"]
        rid = client.post("/source-requests", json={"name": "svc", "repo": "git@git.corp:o/svc.git", "ref": "main"})
        rid = rid.json()["id"]
        assert client.post(f"/source-requests/{rid}/claim", headers=vpn).status_code == 200
        assert client.get("/source-requests").json() == []  # claimed, and its claimer is here
        assert client.post(f"/source-requests/{rid}/claim", headers=other).status_code == 409

        client.delete(f"/sessions/{sid}", headers=vpn)  # the laptop closed its lid
        assert [r["id"] for r in client.get("/source-requests").json()] == [rid]
        assert client.post(f"/source-requests/{rid}/claim", headers=other).status_code == 200


def test_waiting_registers_beats_and_closes_its_need(setup, monkeypatch):
    import httpx

    from ferment import needs

    tc, _ = setup
    with TestClient(create_app(tc)) as client:

        def route(method, url, **kw):
            return client.request(method, url.removeprefix("http://f"), **kw)

        monkeypatch.setattr(httpx, "request", route)
        with needs.Waiting("http://f", needs.relay("nexus:443"), "fmnt apply") as w:
            assert [n["capability"] for n in client.get("/needs").json()] == ["relay:nexus:443"]
            tc.store.close_need(w.id, met=False)  # lapsed meanwhile (say this machine slept)
            w.beat()  # registers anew
            assert len(client.get("/needs").json()) == 1
        assert client.get("/needs").json() == []

        monkeypatch.setattr(httpx, "request", lambda method, url, **kw: httpx.Response(404))
        with needs.Waiting("http://f", needs.fetch("git.corp"), "fmnt apply") as w:  # a server without /needs
            w.beat()
        assert w.id is None


def test_checkpoints_form_a_tree_and_a_restore_starts_a_branch(setup):
    tc, _ = setup
    a = tc.checkpoint("loaded", name="loaded")
    b = tc.checkpoint("day 1")
    assert (a["parent"], b["parent"]) == (None, a["id"])
    out = tc.restore("loaded", wait_ready=False)
    assert out["previous"]["parent"] == b["id"]  # where we were, kept
    c = tc.checkpoint("day 1, variant B")
    assert c["parent"] == a["id"]  # a branch off loaded, not after b
    with TestClient(create_app(tc)) as client:
        rows = {r["id"]: r for r in client.get("/checkpoints").json()}
        assert rows[c["id"]]["head"] and not rows[b["id"]]["head"]
