import subprocess
import time

import pytest

from ferment import sources
from ferment.server.builds import Builder
from ferment.server.sources import SourceStore, SourceStoreError
from ferment.server.store import Store


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def upstream(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "client-state"))
    repo = tmp_path / "upstream"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "development")
    git(repo, "config", "user.email", "t@t")
    git(repo, "config", "user.name", "t")
    (repo / "pom.txt").write_text("v1\n")
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "one")
    return repo


def commit(repo, content):
    (repo / "pom.txt").write_text(content)
    git(repo, "commit", "-qam", content)
    return git(repo, "rev-parse", "HEAD")


def wait(builder_store, build_id, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        b = builder_store.build(build_id)
        if b["status"] in ("succeeded", "failed"):
            return b
        time.sleep(0.05)
    raise AssertionError("build didn't finish")


def test_branch_to_vm_checkout_with_incremental_bundles(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    assert c1 == git(upstream, "rev-parse", "HEAD")
    with pytest.raises(sources.SourceError, match="no branch or tag"):
        sources.resolve(url, "nope")

    vm = SourceStore(tmp_path / "vm")
    assert vm.commits("svc") == []
    first = sources.bundle(sources.fetch(url, c1), c1, have=vm.commits("svc"))
    assert vm.receive("svc", first) == [c1]

    c2 = commit(upstream, "v2\n")
    second = sources.bundle(sources.fetch(url, c2), c2, have=vm.commits("svc"))
    assert len(second) < len(first) * 2  # only the new commit's objects, not all history again
    vm.receive("svc", second)
    assert set(vm.commits("svc")) == {c1, c2}

    # c1 arrived as history of c2's bundle too; an older commit needs nothing sent
    assert not sources.missing(sources.fetch(url, c1), c1, [c2])
    assert vm.has("svc", c1)

    tree = vm.worktree("svc", c2)
    assert (tree / "pom.txt").read_text() == "v2\n"
    assert (tree / ".git").is_dir()  # a real repo: JGit (git-commit-id plugin) can't follow a .git file
    assert git(tree, "rev-parse", "HEAD") == c2
    with pytest.raises(SourceStoreError, match="upload a bundle first"):
        vm.worktree("svc", "0" * 40)


def test_builds_succeed_reuse_and_fail_loudly(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    vm = SourceStore(tmp_path / "vm")
    vm.receive("svc", sources.bundle(sources.fetch(url, c1), c1))
    store = Store(tmp_path / "vm" / "f.db")
    builder = Builder(store, vm, tmp_path / "vm" / "logs")

    ok = builder.request("svc", c1, "mkdir -p target && cp pom.txt target/svc-1.jar", "target/svc-*.jar", "me")
    done = wait(store, ok["id"])
    assert done["status"] == "succeeded" and done["artifact"].endswith("target/svc-1.jar")
    assert (
        builder.request("svc", c1, "mkdir -p target && cp pom.txt target/svc-1.jar", "target/svc-*.jar", "me")["id"]
        == ok["id"]
    )  # cached by (source, commit, command)

    stamp = wait(store, builder.request("svc", c1, "echo epoch=$SOURCE_DATE_EPOCH", None, "me")["id"])
    assert f"epoch={git(upstream, 'log', '-1', '--format=%ct', c1)}" in builder.log(stamp["id"])[0]

    bad = wait(store, builder.request("svc", c1, "echo boom; exit 3", None, "me")["id"])
    assert bad["status"] == "failed" and bad["error"] == "exit 3"
    assert "boom" in builder.log(bad["id"])[0]

    glob = wait(store, builder.request("svc", c1, "true", "target/*.war", "me")["id"])
    assert glob["status"] == "failed" and "matched 0 files" in glob["error"]


def test_source_requests_are_claimed_once_and_expire(tmp_path, monkeypatch):
    import datetime as dt

    from ferment.server import store as store_mod

    s = Store(tmp_path / "f.db")
    r = s.request_source("svc", "git@corp:o/svc.git", "development", "me")
    assert s.request_source("svc", "git@corp:o/svc.git", "development", "you")["id"] == r["id"]  # deduped
    assert s.claim_source_request(r["id"], "vpn-box")
    assert not s.claim_source_request(r["id"], "other-box")  # exactly one winner
    monkeypatch.setattr(store_mod, "CLAIM_TIMEOUT", dt.timedelta(seconds=-1))  # claimant vanished
    assert s.claim_source_request(r["id"], "other-box")


def test_attached_client_fetches_for_the_vm(upstream, tmp_path, monkeypatch):
    """apply on a machine that can't reach the repo queues a request; a
    client that can claims it, fetches, uploads; the commit lands on the VM."""
    import httpx
    from fastapi.testclient import TestClient

    from ferment.fulfill import SourceFulfiller
    from ferment.server.app import create_app
    from ferment.server.time_control import TimeController

    class Stack:
        def processes(self):
            return []

    class Sup:
        running = False

        def stop(self):
            pass

        def start(self, c):
            pass

        def wait_ready(self, timeout=1):
            return True

        def processes(self):
            return []

    vm_state = tmp_path / "vm"
    with TestClient(create_app(TimeController(Stack(), Sup(), "/x", vm_state))) as client:
        base = "http://fermenter"
        for verb in ("get", "post"):
            monkeypatch.setattr(
                httpx,
                verb,
                lambda url, _v=verb, **kw: getattr(client, _v)(
                    url.removeprefix(base), **{k: v for k, v in kw.items() if k != "timeout"}
                ),
            )
        monkeypatch.setattr(sources, "repo_host", lambda repo: "corp.example")  # pretend it's remote

        req = client.post("/source-requests", json={"name": "svc", "repo": str(upstream), "ref": "development"}).json()
        assert req["status"] == "pending"
        assert client.post(f"/source-requests/{req['id']}/finish", json={"commit": "0" * 40}).status_code == 422

        fulfiller = SourceFulfiller(base, ["corp.example"])
        (pending,) = httpx.get(f"{base}/source-requests").json()
        assert fulfiller._claim(pending["id"])
        fulfiller.fulfill(pending)

        done = client.get(f"/source-requests/{req['id']}").json()
        assert done["status"] == "done"
        assert done["commit_sha"] == git(upstream, "rev-parse", "HEAD")
        assert done["commit_sha"] in client.get("/sources/svc").json()["commits"]

        bad = client.post("/source-requests", json={"name": "svc", "repo": str(upstream), "ref": "nope"}).json()
        assert fulfiller._claim(bad["id"])
        fulfiller.fulfill(bad)
        failed = client.get(f"/source-requests/{bad['id']}").json()
        assert failed["status"] == "failed" and "no branch or tag" in failed["error"]


def _built(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    vm = SourceStore(tmp_path / "vm")
    vm.receive("svc", sources.bundle(sources.fetch(url, c1), c1))
    return c1, vm, Store(tmp_path / "vm" / "f.db")


def test_builds_outlive_the_server_that_started_them(upstream, tmp_path):
    c1, vm, store = _built(upstream, tmp_path)
    first = Builder(store, vm, tmp_path / "vm" / "logs", watch=False)
    b = first.request("svc", c1, "sleep 1; mkdir -p target && cp pom.txt target/svc-1.jar", "target/*.jar", "me")
    assert b["status"] == "running"
    del first  # the server restarts mid-build

    second = Builder(store, vm, tmp_path / "vm" / "logs", watch=False)
    assert store.build(b["id"])["status"] == "running"  # adopted, not failed
    deadline = time.time() + 10
    while store.build(b["id"])["status"] == "running" and time.time() < deadline:
        second.reap()
        time.sleep(0.1)
    assert store.build(b["id"])["status"] == "succeeded"


def test_a_killed_build_fails_with_a_reason(upstream, tmp_path):
    import os
    import signal

    c1, vm, store = _built(upstream, tmp_path)
    builder = Builder(store, vm, tmp_path / "vm" / "logs", watch=False)
    b = builder.request("svc", c1, "sleep 30", None, "me")
    pid_file = tmp_path / "vm" / "logs" / f"build-{b['id']}.pid"
    deadline = time.time() + 5
    while not pid_file.exists() and time.time() < deadline:
        time.sleep(0.05)
    os.killpg(int(pid_file.read_text()), signal.SIGKILL)
    time.sleep(0.2)
    builder.reap()
    done = store.build(b["id"])
    assert done["status"] == "failed" and "without reporting" in done["error"]


def test_old_style_running_builds_are_failed_at_startup(upstream, tmp_path):
    c1, vm, store = _built(upstream, tmp_path)
    bid = store.add_build(name="svc", commit=c1, command="x", artifact_glob=None, who="me", log_path="")
    store.update_build(bid, status="running")  # a build from before builds had pid files
    Builder(store, vm, tmp_path / "vm" / "logs", watch=False)
    assert "old build" in store.build(bid)["error"]


def test_an_old_linked_worktree_is_replaced_by_a_real_checkout(upstream, tmp_path):
    url = str(upstream)
    c1 = sources.resolve(url, "development")
    vm = SourceStore(tmp_path / "vm")
    vm.receive("svc", sources.bundle(sources.fetch(url, c1), c1))
    old = tmp_path / "vm" / "worktrees" / f"svc-{c1[:12]}"
    git(tmp_path / "vm" / "sources" / "svc.git", "worktree", "add", "--quiet", "--detach", str(old), c1)
    assert (old / ".git").is_file()
    tree = vm.worktree("svc", c1)
    assert tree == old and (tree / ".git").is_dir()
