import io
import socket
import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI
from rich.console import Console

from ferment import fakeboard
from ferment.board import Board, Milestone
from ferment.boardview import BoardView, Follower
from ferment.broadcasts import BroadcastWatcher


def terminal_view(**kw) -> tuple[BoardView, io.StringIO]:
    out = io.StringIO()
    console = Console(file=out, width=120, force_terminal=True, color_system=None)
    return BoardView(console, "demo", "me@here", **kw), out


def plain_view() -> tuple[BoardView, io.StringIO]:
    out = io.StringIO()
    return BoardView(Console(file=out, width=120), "demo", "me@here"), out


def drawn(view: BoardView) -> str:
    out = io.StringIO()
    Console(file=out, width=120, color_system=None).print(view)
    return out.getvalue()


def test_done_groups_roll_up_and_the_open_one_shows_its_steps():
    b = Board()
    b.plan([Milestone(id="a", label="VM created", group="create"), Milestone(id="b", label="db up", group="create")])
    b.plan(
        [
            Milestone(id="c", label="fetched", group="load"),
            Milestone(id="d", label="loaded", group="load", source="loader", expected=200),
            Milestone(id="e", label="indexed", group="load"),
        ]
    )
    for m in "abc":
        b.start(m)
        b.finish(m)
    b.start("d")
    b.count("loader", 50)
    view, _ = terminal_view()
    view.update(b.snapshot())
    text = drawn(view)
    assert "✔ create (2)" in text
    assert "load  ✔ fetched" in text
    assert "25%  50/~200 lines" in text
    assert "next: indexed" in text
    assert "VM created" not in text  # rolled up


def test_a_group_with_a_failure_rolls_up_once_nothing_is_left_to_run():
    b = Board()
    b.plan([Milestone(id="x", label="worker ready", group="deploy"), Milestone(id="y", label="retry", group="deploy")])
    b.start("x")
    b.finish("x", ok=False)
    b.start("y")
    b.finish("y")
    view, _ = terminal_view()
    view.update(b.snapshot())
    assert "✔ deploy (1) ✘1" in drawn(view)


def test_services_show_local_addresses_and_uncovered_forwards():
    b = Board()
    b.set_service("grafana", port=3000, state="up")
    b.set_service("db", label="database", port=3306, scheme="mysql", state="starting")
    b.set_service("airflow", port=8080, state="down")
    view, _ = terminal_view(forwards={"grafana": (43000, 3000), "db": (43005, 3306), "fermenter": (43004, 8400)})
    view.update(b.snapshot())
    text = drawn(view)
    assert "grafana   http://localhost:43000" in text
    assert "database  mysql://127.0.0.1:43005" in text
    assert "not forwarded (VM :8080)" in text
    assert "→ fermenter http://localhost:43004" in text


def test_forwards_are_pinned_before_the_board_arrives():
    view, _ = terminal_view(forwards={"fermenter": (43004, 8400)})
    text = drawn(view)
    assert "connecting" in text and "http://localhost:43004" in text


def test_finished_steps_print_before_what_was_said_after_them_and_not_on_attach():
    b = Board()
    b.plan([Milestone(id="a", label="api ready"), Milestone(id="b", label="worker ready")])
    b.start("a")
    b.finish("a")
    view, out = plain_view()
    view.update(b.snapshot())
    assert out.getvalue() == ""  # attaching late: no replay of what already finished
    seq = b.feed_seq
    b.start("b")
    b.finish("b")
    b.post("ready", "stack is ready")
    view.update(b.snapshot(after=seq))
    lines = out.getvalue().splitlines()
    assert [line[11:] for line in lines] == ["✔ worker ready", "stack is ready"]


def test_failures_print_with_their_detail():
    b = Board()
    b.plan([Milestone(id="a", label="api ready")])
    view, out = plain_view()
    view.update(b.snapshot())
    b.start("a")
    b.finish("a", ok=False, detail="exit 1")
    view.update(b.snapshot())
    assert out.getvalue().endswith("✘ api ready: exit 1\n")


def test_local_lines_are_pinned_and_announced_only_when_they_change():
    view, out = plain_view()
    view.set_local("relay", "relay nexus: up", "green")
    view.set_local("relay", "relay nexus: up", "green")
    view.set_local("relay", "relay nexus: lost; retrying", "red")
    assert [line[11:] for line in out.getvalue().splitlines()] == ["relay nexus: up", "relay nexus: lost; retrying"]
    assert "relay nexus: lost; retrying" in drawn(view)


# -- following a real server --------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def serve():
    servers = []

    def start(app: FastAPI) -> str:
        port = _free_port()
        server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
        threading.Thread(target=server.run, daemon=True).start()
        while not server.started:
            time.sleep(0.01)
        servers.append(server)
        return f"http://127.0.0.1:{port}"

    yield start
    for s in servers:
        s.should_exit = True


def _until(predicate, timeout=20.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.05)


def test_follows_a_whole_fake_run(serve):
    url = serve(fakeboard.create_app(speed=40))
    view, out = plain_view()
    follower = Follower(url, view)
    follower.start()
    try:
        _until(lambda: "bob@vpnbox detached" in out.getvalue())
    finally:
        follower.stop.set()
    text = out.getvalue()
    assert "✘ worker ready: exited with status 1" in text
    assert text.index("✔ snapshot loaded") < text.index("is moving the clock")
    assert view.conn == "up"


def test_a_server_without_a_board_still_gets_its_broadcasts_shown(serve, monkeypatch):
    monkeypatch.setattr(BroadcastWatcher, "POLL", 0.05)
    entries = []
    app = FastAPI()

    @app.get("/broadcasts")
    def broadcasts(after: int = -1):
        if after < 0:
            return {"entries": [], "offset": 0}
        return {"entries": entries[after:], "offset": len(entries)}

    url = serve(app)
    view, out = plain_view()
    follower = Follower(url, view)
    follower.start()
    try:
        _until(lambda: view.conn == "legacy")
        entries.append({"event": "clock_change", "who": "bob@vpnbox", "message": "bob moved the clock"})
        entries.append({"event": "checkpoint", "who": "me@here", "message": "mine: not shown"})
        _until(lambda: "bob moved the clock" in out.getvalue())
    finally:
        follower.stop.set()
    assert "mine" not in out.getvalue()


def _waiting_snapshot(*items):
    from ferment.board import WaitItem

    b = Board()
    b.set_waiting([WaitItem(**i) for i in items])
    return b.snapshot()


def test_the_queue_shows_whats_waiting_and_what_enter_would_run(monkeypatch):
    ran = []
    resolve = {"eda.gcloud-login": ["eda", "auth", "gcloud-login", "--server-auth"]}.get
    view, _ = terminal_view(resolve=resolve, on_acted=lambda key, code: ran.append(("acted", key, code)))
    view.update(
        _waiting_snapshot(
            {"id": "data-x", "kind": "requests", "title": "3 data requests need someone", "detail": "asked by you"},
            {
                "id": "action-1",
                "kind": "action",
                "title": "eda: sign in to gcloud",
                "detail": "2 requests waiting",
                "who": "me@here",
                "key": "eda.gcloud-login",
            },
            {"id": "action-2", "kind": "action", "title": "eda: db login", "who": "them@there", "key": "eda.x"},
        )  # fmt: skip
    )
    view.interactive = True
    text = drawn(view)
    assert "▶ 3 data requests need someone" in text and "eda: sign in to gcloud" in text
    view.key("down")
    text = drawn(view)
    assert "▶ eda: sign in to gcloud" in text and "enter: eda auth gcloud-login --server-auth" in text

    monkeypatch.setattr(
        "ferment.boardview.subprocess.run", lambda argv: ran.append(argv) or type("R", (), {"returncode": 0})()
    )
    view.key("enter")
    assert ran == [["eda", "auth", "gcloud-login", "--server-auth"], ("acted", "eda.gcloud-login", 0)]

    view.key("down")  # someone else's action: shown, not runnable here
    assert "them@there's to do, on their machine" in drawn(view)
    view.key("enter")
    assert len(ran) == 2
    view.key("down")  # wraps around
    assert "▶ 3 data requests" in drawn(view)


def test_counted_progress_shows_done_of_total():
    b = Board()
    b.plan([Milestone(id="t", label="pull na", group="pull na", total=188, done=0)])
    b.start("t")
    b.set_progress("t", 30, detail="billing_bookkeeper.fee_summary")
    view, _ = terminal_view()
    view.update(b.snapshot())
    assert "30/188  billing_bookkeeper.fee_summary" in drawn(view)
