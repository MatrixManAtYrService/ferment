import datetime as dt

import pytest
from fastapi.testclient import TestClient

from ferment.server.app import create_app
from ferment.server.clock import GuestClock
from ferment.server.time_control import CheckpointError, TimeController
from ferment.shelf import DirShelf, ShelfError, open_shelf
from tests.test_server import FakeStore, Stack, Supervisor


def test_a_put_lands_whole_with_its_metadata_or_not_at_all(tmp_path):
    shelf = DirShelf(tmp_path)
    meta = shelf.put("datasets/na/frag-1.sql.gz", [b"ab", b"cd"], {"provenance": "prod"})
    assert meta == {"provenance": "prod", "key": "datasets/na/frag-1.sql.gz", "bytes": 4}
    assert b"".join(shelf.get("datasets/na/frag-1.sql.gz")) == b"abcd"
    assert shelf.meta("datasets/na/frag-1.sql.gz") == meta

    def broken():
        yield b"half"
        raise ConnectionError("the sender went away")

    with pytest.raises(ConnectionError):
        shelf.put("snapshots/x/1/volume.btrfs.zst", broken(), {})
    assert shelf.list() == ["datasets/na/frag-1.sql.gz"]  # nothing half-written, no partial file listed
    assert shelf.meta("snapshots/x/1/volume.btrfs.zst") is None
    shelf.delete("datasets/na/frag-1.sql.gz")
    assert shelf.list() == []


def test_keys_stay_inside_the_shelf(tmp_path):
    shelf = DirShelf(tmp_path)
    for bad in ["../etc/passwd", "/abs", "a//b", "a/../b", "x.meta.json", ""]:
        with pytest.raises(ShelfError):
            shelf.put(bad, [b""], {})


def test_a_synthetic_only_shelf_refuses_anything_else(tmp_path):
    shelf = DirShelf(tmp_path, synthetic_only=True)
    shelf.put("datasets/gen/1", [b"x"], {"provenance": "synthetic"})
    for meta in [{"provenance": "prod"}, {}]:
        with pytest.raises(ShelfError, match="synthetic data only"):
            shelf.put("datasets/na/1", [b"x"], meta)
    assert open_shelf(f"file://{tmp_path}").list() == ["datasets/gen/1"]
    with pytest.raises(ShelfError, match="no bucket backend"):
        open_shelf("gs://somewhere")


class ShelvingStore(FakeStore):
    """FakeStore that sends a checkpoint's data as bytes naming its base, and makes it again from them."""

    archive_name = "fake.bin"

    def send(self, parts, base):
        ref = parts["fake:billing"]
        yield f"{self.commits[ref]}|{base['fake:billing'] if base else '-'}".encode()

    def parts_for(self, checkpoint_id):
        return {"fake:billing": f"imported-{checkpoint_id}"}

    def receive(self, parts, chunks):
        data, base = b"".join(chunks).decode().split("|")
        assert base == "-" or base in self.commits, f"received {parts} without its base {base}"
        self.commits[parts["fake:billing"]] = int(data)


@pytest.fixture
def shelved(tmp_path):
    store = ShelvingStore()
    system = {"t": dt.datetime.now(dt.UTC)}
    clock = GuestClock(
        tmp_path / "time.json", set_system_time=lambda w: system.update(t=w), system_now=lambda: system["t"]
    )
    shelf = DirShelf(tmp_path / "shelf")
    tc = TimeController(
        Stack(), Supervisor(), tmp_path, versioned=[store], clock=clock, shelf=shelf, shelf_prefix="snapshots/toaster",
        provenance="prod",
    )  # fmt: skip
    return tc, store, shelf


def test_dropped_checkpoints_are_archived_first_and_restore_from_the_shelf(shelved):
    tc, store, shelf = shelved
    tc.checkpoint("loaded", name="loaded")
    store.data = 1
    a = tc.checkpoint("a")
    store.data = 2
    b = tc.checkpoint("b")  # descends from a
    tc.restore("loaded", wait_ready=False)  # leaves a before-restore checkpoint; head is loaded
    assert tc.drop(str(b["id"]))["archive"]["base"] == a["id"]  # relative to the nearest ancestor that's here
    assert tc.drop(str(a["id"]))["archive"]["base"] == 1  # a's base: loaded
    assert len(shelf.list("snapshots/toaster/")) == 2
    meta = shelf.meta(f"snapshots/toaster/{b['id']}/fake.bin")
    assert meta["provenance"] == "prod" and meta["checkpoint"]["message"] == "b" and meta["base"] == a["id"]

    out = tc.restore(str(b["id"]), wait_ready=False)  # a comes back first: b was sent relative to it
    assert store.data == 2 and out["restored"]["parts"]
    assert tc.store.checkpoint(str(a["id"]))["parts"]  # restorable again too
    assert tc.archive(str(b["id"]))["checkpoint_id"] == b["id"]  # once: nothing sent again
    assert len(shelf.list()) == 2


def test_a_shelf_that_refuses_the_data_keeps_the_checkpoint(shelved):
    tc, store, _ = shelved
    tc.shelf = DirShelf(tc.state_dir / "gcs-like", synthetic_only=True)
    tc.checkpoint("loaded", name="loaded")
    a = tc.checkpoint("a")
    tc.restore("loaded", wait_ready=False)
    with pytest.raises(Exception, match="synthetic data only"):
        tc.drop(str(a["id"]))
    assert tc.store.checkpoint(str(a["id"]))["parts"]  # not dropped


def test_archive_over_http_and_prune_archives_what_it_drops(shelved):
    tc, store, shelf = shelved
    tc.checkpoint("loaded", name="loaded")
    for _ in range(3):
        tc.restore("loaded", wait_ready=False)
    with TestClient(create_app(tc)) as client:
        dropped = client.post("/checkpoints/prune", params={"keep": 1}).json()["dropped"]
        assert len(dropped) == 2 and len(shelf.list()) == 2
        assert client.post(f"/checkpoints/{dropped[0]}/unarchive").json()["parts"]
        assert client.post("/checkpoints/nope/archive").status_code == 409


def test_without_a_shelf_a_drop_is_for_good(shelved):
    tc, _, _ = shelved
    tc.shelf = None
    tc.checkpoint("loaded", name="loaded")
    a = tc.checkpoint("a")
    tc.restore("loaded", wait_ready=False)
    tc.drop(str(a["id"]))
    with pytest.raises(CheckpointError, match="no versioned data"):
        tc.restore(str(a["id"]), wait_ready=False)


def test_forever_drops_named_ones_on_request_and_takes_the_shelf_copy_too(shelved):
    tc, store, shelf = shelved
    tc.checkpoint("loaded", name="loaded")
    store.data = 1
    tc.checkpoint("a", name="a")
    store.data = 2
    b = tc.checkpoint("b")
    tc.restore("loaded", wait_ready=False)
    tc.drop(str(b["id"]))  # archived relative to a
    with pytest.raises(CheckpointError, match="named"):
        tc.drop("a")  # not without forever
    with pytest.raises(CheckpointError, match=rf"base of archived checkpoint\(s\) \[{b['id']}\]"):
        tc.drop("a", forever=True)
    tc.drop(str(b["id"]), forever=True)  # already dropped: just its shelf copy
    assert shelf.list() == [] and tc.store.checkpoint(str(b["id"]))["archive"] is None
    tc.drop("a", forever=True)
    assert not tc.store.checkpoint("a")["parts"] and sorted(store.commits) == ["c1", "c4"]  # loaded, the before-restore
    with pytest.raises(CheckpointError, match="no versioned data"):
        tc.restore("a", wait_ready=False)
    head = tc.store.head()
    with pytest.raises(CheckpointError, match="named or a stage|where the workload is"):
        tc.drop(str(head))
    tc.drop(str(head), forever=True)  # the live data stays; only the way back to it goes
    assert not tc.store.checkpoint(str(head))["parts"]


def test_another_fermenter_imports_a_chain_from_the_shelf_with_its_stages(shelved, tmp_path):
    tc, store, shelf = shelved  # "toaster"
    tc.checkpoint("loaded", name="loaded")
    store.data = 7
    rewound = tc.checkpoint("rewound")
    tc.store.adopt(rewound["id"], "rewound-11-03", "fp-1", {"run_started": "2026-11-03T00:31:00Z"})
    loaded = tc.store.checkpoint("loaded")
    tc.archive(str(loaded["id"]))
    tc.archive(str(rewound["id"]))  # relative to loaded

    other = ShelvingStore()  # "skillet": the same shelf, its own volume and store
    clock = GuestClock(
        tmp_path / "time2.json", set_system_time=lambda w: None, system_now=lambda: dt.datetime.now(dt.UTC)
    )
    (tmp_path / "skillet").mkdir()
    sk = TimeController(Stack(), Supervisor(), tmp_path / "skillet", versioned=[other], clock=clock, shelf=shelf)
    sk.checkpoint("its own", name="its-own")
    head = sk.store.head()
    got = sk.import_archive(f"snapshots/toaster/{rewound['id']}", "me")
    assert got["stage"] == "rewound-11-03" and got["fingerprint"] == "fp-1"
    assert got["value"] == {"run_started": "2026-11-03T00:31:00Z"}
    base = sk.store.checkpoint(str(got["parent"]))
    assert base is not None and base["name"] == "loaded"  # its base came first, by name
    assert sk.store.head() == head  # the workload didn't move
    assert sk.store.stage("rewound-11-03", "fp-1")[0]["id"] == got["id"]  # a script here finds it
    sk.restore(str(got["id"]), wait_ready=False)
    assert other.data == 7
    assert sk.import_archive(f"snapshots/toaster/{rewound['id']}", "me")["id"] == got["id"]  # once
    with pytest.raises(CheckpointError, match="nothing on the shelf"):
        sk.import_archive("snapshots/toaster/999", "me")


def test_archiving_one_archives_what_its_sent_relative_to(shelved):
    tc, store, shelf = shelved
    tc.checkpoint("loaded", name="loaded")
    store.data = 3
    tc.restore("loaded", wait_ready=False)  # a before-restore checkpoint in between, unarchived
    store.data = 4
    later = tc.checkpoint("later")
    out = tc.archive(str(later["id"]))
    base = tc.store.checkpoint(str(out["base"]))
    assert base is not None and base["archive"] is not None  # on the shelf too
    keys = shelf.list("snapshots/toaster/")
    assert all(any(k.startswith(f"snapshots/toaster/{i}/") for k in keys) for i in (later["id"], base["id"]))
