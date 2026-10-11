import asyncio
import json
import threading

from fastapi.testclient import TestClient

from ferment import fakeboard
from ferment.board import Board, Milestone
from ferment.server.board import events


def planned(*milestones: Milestone, estimates=None) -> Board:
    b = Board()
    b.plan(list(milestones), estimates)
    return b


def test_progress_is_lines_over_expected_and_never_done_early():
    b = planned(Milestone(id="api", label="api ready", source="api", expected=100))
    b.start("api")
    b.count("api", 40)
    assert b.milestones["api"].progress == 0.4
    b.count("api", 500)  # wrote more than usual: still not done
    assert b.milestones["api"].progress == 0.99
    b.finish("api")
    assert b.milestones["api"].progress == 1.0


def test_progress_unknown_without_an_estimate():
    b = planned(Milestone(id="vm", label="VM created"))
    b.start("vm")
    assert b.milestones["vm"].progress is None


def test_lines_count_only_toward_running_milestones_of_that_source():
    b = planned(
        Milestone(id="a", label="a", source="api"),
        Milestone(id="b", label="b", source="api"),
        Milestone(id="w", label="w", source="worker"),
    )
    b.start("a")
    b.start("w")
    b.count("api", 3)
    assert [b.milestones[m].lines for m in "abw"] == [3, 0, 0]


def test_actuals_become_the_next_runs_estimates():
    b = planned(Milestone(id="api", label="api ready", source="api"))
    b.start("api")
    b.count("api", 321)
    b.finish("api")
    nxt = planned(Milestone(id="api", label="api ready", source="api"), estimates=b.actuals())
    assert nxt.milestones["api"].expected == 321
    # an explicit expectation wins over an estimate
    pinned = planned(Milestone(id="api", label="api", source="api", expected=5), estimates=b.actuals())
    assert pinned.milestones["api"].expected == 5


def test_failed_milestones_are_not_estimates():
    b = planned(Milestone(id="api", label="api ready", source="api"))
    b.start("api")
    b.count("api", 7)
    b.finish("api", ok=False)
    assert b.actuals() == {}


def test_feed_after_and_limit():
    b = Board(feed_limit=3)
    for i in range(5):
        b.post("e", f"m{i}")
    snap = b.snapshot()
    assert [e.message for e in snap.feed] == ["m2", "m3", "m4"]
    assert snap.feed_seq == 5
    assert [e.message for e in b.snapshot(after=4).feed] == ["m4"]
    assert b.snapshot(after=5).feed == []


def test_set_service_merges_fields():
    b = Board()
    b.set_service("db", label="database", port=3306, scheme="mysql")
    b.set_service("db", state="up")
    s = b.services["db"]
    assert (s.label, s.port, s.scheme, s.state) == ("database", 3306, "mysql", "up")


def test_reset_keeps_the_feed():
    b = Board()
    b.set_fact("clock", "2026-10-02")
    b.post("e", "hello")
    b.reset()
    snap = b.snapshot()
    assert snap.facts == [] and [e.message for e in snap.feed] == ["hello"]


def test_wait_returns_when_the_board_changes():
    b = Board()
    v = b.version
    assert b.wait(v, timeout=0.01) == v  # nothing happened
    t = threading.Timer(0.05, b.set_fact, ("k", "v"))
    t.start()
    assert b.wait(v, timeout=5) > v


def _collect(board: Board, done, after=None, keepalive=0.05, min_interval=0.0) -> list[str]:
    """Chunks the stream sends until `done(chunks)`."""

    async def go():
        out = []

        async def disconnected():
            return done(out)

        async for chunk in events(board, after, disconnected, keepalive, min_interval):
            out.append(chunk)
        return out

    return asyncio.run(go())


def _data(chunk: str) -> dict:
    return json.loads(next(line[6:] for line in chunk.splitlines() if line.startswith("data: ")))


def test_stream_sends_the_board_then_only_new_feed():
    b = Board()
    b.post("e", "before")
    b.set_fact("clock", "2026-10-02", "fake time")
    threading.Timer(0.2, b.post, ("e", "after")).start()
    first, *rest = _collect(b, lambda out: sum(c.startswith("id:") for c in out) == 2)
    assert [e["message"] for e in _data(first)["feed"]] == ["before"]
    assert _data(first)["facts"][0]["value"] == "2026-10-02"
    assert ": keepalive\n\n" in rest  # quiet until the post
    later = [_data(c) for c in rest if c.startswith("id:")]
    assert [e["message"] for e in later[0]["feed"]] == ["after"]


def test_stream_sends_quick_changes_together():
    b = Board()

    def burst():
        for i in range(20):
            b.post("e", f"m{i}")

    threading.Timer(0.05, burst).start()
    chunks = _collect(
        b, lambda out: sum(len(_data(c)["feed"]) for c in out if c.startswith("id:")) == 20, min_interval=0.3
    )
    updates = [c for c in chunks if c.startswith("id:")]
    assert len(updates) <= 3  # the first board, then the burst in one or two


def test_stream_resumes_after_a_seq():
    b = Board()
    for m in ("a", "b", "c"):
        b.post("e", m)
    [first] = _collect(b, lambda out: len(out) == 1, after=2)
    assert [e["message"] for e in _data(first)["feed"]] == ["c"]


def test_the_fake_run_finishes_everything_but_its_planned_failure():
    b = Board()
    fakeboard.Run(b, pause=lambda _s: None)()
    states = {m.id: m.state for m in b.milestones.values()}
    assert states.pop("worker2") == "failed"
    assert set(states.values()) == {"done"}
    assert {s.state for s in b.services.values()} == {"up"}
    assert b.fake_offset is not None
    assert any(e.event == "clock_changed" for e in b.feed)


def test_fake_server_serves_the_board():
    with TestClient(fakeboard.create_app(speed=1000)) as client:
        snap = client.get("/board").json()
        assert {"version", "facts", "services", "milestones", "feed", "feed_seq", "fake_offset"} <= snap.keys()
