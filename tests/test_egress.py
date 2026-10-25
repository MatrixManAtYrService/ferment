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
