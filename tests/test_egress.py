from fastapi.testclient import TestClient

from ferment.egress import EgressCache, cache_key, create_egress_app


def test_keys_are_the_recording_proxys_own():
    # eda's _cache_key: method, upstream path, then the query sorted.
    assert cache_key("get", "cos/v3/merchants/M/apps", [("limit", "5"), ("expand", "x")]) == (
        "GET cos/v3/merchants/M/apps?expand=x&limit=5"
    )
    assert cache_key("GET", "cos/v1/a", []) == "GET cos/v1/a"


def test_a_run_reads_only_the_cache_misses_fail_closed_and_writes_are_faked(tmp_path):
    cache = EgressCache(tmp_path / "egress.sqlite")
    client = TestClient(create_egress_app(cache, "fermenter"))
    miss = client.get("/proxy/na/cos/v3/apps", params={"limit": "5", "expand": "x"})
    assert miss.status_code == 511 and "precache it" in miss.text
    assert [m["key"] for m in client.get("/misses").json()] == ["GET cos/v3/apps?expand=x&limit=5"]

    # What eda's record mode sends (its _tee_to_cache payload).
    client.post("/cache/put", json={"cache_name": "fermenter", "key": "GET cos/v3/apps?expand=x&limit=5",
                                    "status_code": 200, "body": '{"apps": []}', "content_type": "application/json",
                                    "meta": {"env": "na"}})  # fmt: skip
    hit = client.get("/proxy/na/cos/v3/apps", params={"expand": "x", "limit": "5"})  # any order
    assert hit.status_code == 200 and hit.json() == {"apps": []}
    assert client.get("/misses").json() == []  # precached: no longer a miss
    assert client.post("/cache/get", json={"key": "GET cos/v3/apps?expand=x&limit=5"}).json()["hit"] is True

    assert client.post("/proxy/na/cos/v3/apps", json={"name": "x"}).status_code == 200  # never forwarded
    assert client.get("/stats").json() == {"cache": "fermenter", "entries": 1, "misses": 0, "writes_faked": 1}


def test_the_cache_survives_a_restart(tmp_path):
    EgressCache(tmp_path / "e.sqlite").put("c", "GET a", 200, "x", None, {})
    assert EgressCache(tmp_path / "e.sqlite").get("c", "GET a")["body"] == "x"


def test_an_entry_from_a_wrong_route_can_be_dropped_and_is_missed_again(tmp_path):
    client = TestClient(create_egress_app(EgressCache(tmp_path / "e.sqlite"), "fermenter"))
    client.post("/cache/put", json={"key": "GET cos/v3/developer-apps", "status_code": 405, "body": ""})
    assert [(e["key"], e["status_code"]) for e in client.get("/entries").json()] == [("GET cos/v3/developer-apps", 405)]
    assert client.post("/cache/drop", json={"keys": ["GET cos/v3/developer-apps"]}).json() == {"dropped": 1}
    assert client.get("/proxy/na/cos/v3/developer-apps").status_code == 511


def test_a_get_with_a_body_is_keyed_by_it_and_its_miss_keeps_the_body(tmp_path):
    client = TestClient(create_egress_app(EgressCache(tmp_path / "e.sqlite"), "fermenter"))
    body = '["APP1","APP2"]'
    r = client.request(
        "GET", "/proxy/na/cos/v3/developer-apps", content=body, headers={"content-type": "application/json"}
    )
    assert r.status_code == 511
    (m,) = client.get("/misses").json()
    assert m["key"] == cache_key("GET", "cos/v3/developer-apps", [], body.encode()) and m["body"] == body
    assert m["key"] != cache_key("GET", "cos/v3/developer-apps", [], b'["APP3"]')


def test_a_post_under_a_read_prefix_is_a_read_keyed_by_its_body_and_others_stay_faked(tmp_path):
    client = TestClient(
        create_egress_app(EgressCache(tmp_path / "e.sqlite"), "fermenter", read_posts=("cos/v3/internal/app_",))
    )
    path = "cos/v3/internal/app_install_uninstall_events"
    headers = {"content-type": "text/plain", "x-clover-appenv": "usprod", "x-other": "dropped"}
    r = client.post(f"/proxy/na/{path}?start=a&stop=b", content="M1,M2", headers=headers)
    assert r.status_code == 511  # a miss, not a faked 200
    (m,) = client.get("/misses").json()
    key = cache_key("POST", path, [("start", "a"), ("stop", "b")], b"M1,M2")
    assert m["key"] == key and m["body"] == "M1,M2"
    assert m["headers"] == {"content-type": "text/plain", "x-clover-appenv": "usprod"}
    client.post("/cache/put", json={"key": key, "status_code": 200, "body": "[{}]", "content_type": "application/json"})
    assert client.post(f"/proxy/na/{path}?stop=b&start=a", content="M1,M2", headers=headers).json() == [{}]
    assert client.post("/proxy/na/cos/v3/merchants/M1/notes", content="x").status_code == 200  # still a faked write
    assert client.get("/stats").json()["writes_faked"] == 1
