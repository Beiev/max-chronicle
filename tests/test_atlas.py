"""The Atlas: the memory as a graph for the map, edges by meaning kept current, and a page that runs only its own code."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import random
import shutil
import struct
import subprocess
import sys
import time

import pytest
from starlette.testclient import TestClient

from max_chronicle import atlas, cli, service
from max_chronicle.embeddings import active_profile
from max_chronicle.mcp_server import build_server
from max_chronicle.notes import sync_notes
from max_chronicle.store import config_from_manifest, open_connection, store_event_embedding

BASE = "http://127.0.0.1:8093"
REPO = Path(__file__).resolve().parents[1]
DIM = 16


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


@pytest.fixture()
def client(chronicle_sandbox):
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app(), base_url=BASE) as test_client:
        yield test_client


def _record(manifest, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields},
                                append_compat=False)


def _vector(seed: int, *, near: list[float] | None = None, spread: float = 0.05) -> list[float]:
    rng = random.Random(seed)
    if near is None:
        return [rng.uniform(-1, 1) for _ in range(DIM)]
    return [value + rng.uniform(-spread, spread) for value in near]


def _embed(manifest, event_id: str, vector: list[float]) -> None:
    store_event_embedding(config_from_manifest(manifest), event_id, vector, active_profile().key, len(vector))


def _nodes(graph: dict) -> dict[str, dict]:
    return {node["id"]: node for node in graph["nodes"]}


# ---------------------------------------------------------------------------
# The graph
# ---------------------------------------------------------------------------


def test_the_map_holds_projects_tasks_events_and_fact_slots_with_their_edges(loaded_manifest) -> None:
    first = _record(loaded_manifest, "Decided to move the gateway to port 9000", project="alpha",
                    task_id="migrate", category="decision",
                    fact={"slot": "gateway.port", "value": "9000", "kind": "decision"})
    second = _record(loaded_manifest, "Moved the gateway", project="alpha", task_id="migrate",
                     fact={"slot": "gateway.port", "value": "9100", "kind": "decision",
                           "supersedes": first["fact_id"]})
    loose = _record(loaded_manifest, "A thought about the project", project="alpha")

    graph = atlas.graph_payload(loaded_manifest)
    nodes = _nodes(graph)
    index = {node["id"]: position for position, node in enumerate(graph["nodes"])}
    edges = {(s, t, graph["edge_kinds"][k]) for s, t, k in graph["edges"]}

    project, task = index["p:alpha"], index["t:alpha/migrate"]
    assert nodes["p:alpha"]["k"] == "project" and nodes["t:alpha/migrate"]["k"] == "task"
    assert (index[f"e:{first['id']}"], task, "task") in edges
    assert (index[f"e:{second['id']}"], task, "task") in edges
    assert (task, project, "project") in edges
    assert (index[f"e:{loose['id']}"], project, "project") in edges
    slot = next(position for node_id, position in index.items() if node_id.startswith("f:gateway.port|global|alpha|"))
    assert (slot, index[f"e:{first['id']}"], "fact") in edges and (slot, index[f"e:{second['id']}"], "fact") in edges
    chain = sorted((fact["value"], fact["to"] is None) for fact in graph["facts"] if fact["node"] == slot)
    assert chain == [("9000", False), ("9100", True)]  # the first value was replaced, the second holds
    assert graph["span"][0] <= nodes[f"e:{first['id']}"]["t"] <= graph["span"][1]
    assert graph["projects"][nodes[f"e:{first['id']}"]["p"]] == "alpha"


def test_a_quarantined_event_never_reaches_the_map(loaded_manifest) -> None:
    hidden = _record(loaded_manifest, "Quarantined zebra secret", project="alpha", task_id="zoo",
                     fact={"slot": "zebra.slot", "value": "stripes", "kind": "decision"},
                     memory_guard={"visibility": "raw_only"})
    _record(loaded_manifest, "Visible step", project="alpha")

    payload = json.dumps(atlas.graph_payload(loaded_manifest))

    assert hidden["id"] not in payload and "zebra" not in payload and "stripes" not in payload


def test_an_event_whose_every_observation_is_quarantined_is_hidden_everywhere(client, loaded_manifest) -> None:
    hidden = _record(loaded_manifest, "Observed in quarantine only", project="alpha",
                     fact={"slot": "zebra.slot", "value": "stripes", "kind": "decision"})
    _record(loaded_manifest, "Visible step", project="alpha")
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE event_observations SET visibility = 'raw_only' WHERE event_id = ?", (hidden["id"],))

    payload = json.dumps(atlas.graph_payload(loaded_manifest))

    assert hidden["id"] not in payload and "stripes" not in payload
    assert client.get(f"/ui/api/item?id=event:{hidden['id']}").status_code == 404


def test_one_slot_in_two_domains_is_two_fact_nodes(loaded_manifest) -> None:
    _record(loaded_manifest, "Port for the whole system", project="alpha",
            fact={"slot": "gateway.port", "value": "8080", "kind": "decision"})
    _record(loaded_manifest, "Port for the memory", project="alpha", domain="memory",
            fact={"slot": "gateway.port", "value": "9090", "kind": "decision"})

    graph = atlas.graph_payload(loaded_manifest)
    slots = [position for position, node in enumerate(graph["nodes"]) if node["k"] == "fact"]

    assert len(slots) == 2
    assert sorted(fact["value"] for fact in graph["facts"] if fact["to"] is None) == ["8080", "9090"]
    assert len({fact["node"] for fact in graph["facts"]}) == 2


def test_a_secret_in_an_old_row_reaches_neither_the_map_nor_the_panel(client, loaded_manifest) -> None:
    key = "sk-" + "proj-" + "Zq8" * 16
    stored = _record(loaded_manifest, "Rotated the key for the gateway", project="alpha", why="It leaked")
    # A row written before the secret filter, or imported past it.
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE events SET text = ?, why = ? WHERE id = ?",
                           (f"Rotated the key {key} for the gateway", f"The old key {key} leaked", stored["id"]))

    payload = json.dumps(atlas.graph_payload(loaded_manifest))
    item = client.get(f"/ui/api/item?id=event:{stored['id']}").text

    assert key not in payload and "Rotated the key" in payload
    assert key not in item and "[REDACTED:openai_key]" in item


def test_every_text_of_a_record_is_redacted_and_its_identifiers_are_kept(loaded_manifest, monkeypatch) -> None:
    key = "sk-" + "proj-" + "Zq8" * 16
    record = {"id": "note:abc", "kind": "note", "path": "/notes/ops.md", "title": f"Ops {key}",
              "text": "Plain", "checkpoint": {"goal": f"Rotate {key}", "next_steps": [f"Revoke {key}"]},
              "provenance": {"observations": [{"session_id": "s-1", "note": f"saw {key}"}]}}
    monkeypatch.setattr(atlas, "get_items", lambda manifest, ids: {"items": [record], "missing": []})

    item = atlas.item_payload(loaded_manifest, "note:abc")

    assert key not in json.dumps(item)
    assert item["id"] == "note:abc" and item["path"] == "/notes/ops.md"
    assert item["provenance"]["observations"][0]["session_id"] == "s-1"


def test_notes_link_each_other_and_a_deleted_note_leaves_the_map(loaded_manifest, tmp_path) -> None:
    folder = tmp_path / "notes"
    folder.mkdir()
    (folder / "gateway.md").write_text("# Gateway\n\nSee [[ports]] and the [runbook](./runbook.md).\n", encoding="utf-8")
    (folder / "ports.md").write_text("# Ports\n\nThe gateway listens on 9000.\n", encoding="utf-8")
    (folder / "runbook.md").write_text("# Runbook\n\nRestart the gateway.\n", encoding="utf-8")
    (folder / "2026-06-18-standup.md").write_text("# Standup\n\nNothing new.\n", encoding="utf-8")
    manifest = {**loaded_manifest, "notes": {"paths": [str(folder / "*.md")]}}
    sync_notes(manifest, embed=False)

    graph = atlas.graph_payload(manifest)
    by_title = {node["l"]: position for position, node in enumerate(graph["nodes"]) if node["k"] == "note"}
    links = {(s, t) for s, t, k in graph["edges"] if graph["edge_kinds"][k] == "link"}

    assert (by_title["Gateway"], by_title["Ports"]) in links and (by_title["Gateway"], by_title["Runbook"]) in links
    standup = graph["nodes"][by_title["Standup"]]
    assert time.strftime("%Y-%m-%d", time.gmtime(standup["t"])) == "2026-06-18"  # dated by its name

    (folder / "ports.md").unlink()
    sync_notes(manifest, embed=False)
    titles = {node["l"] for node in atlas.graph_payload(manifest)["nodes"] if node["k"] == "note"}
    assert "Ports" not in titles and "Gateway" in titles


# ---------------------------------------------------------------------------
# Edges by meaning
# ---------------------------------------------------------------------------


def _unit(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in values))
    return [value / norm for value in values]


def _clustered(rng: random.Random, count: int) -> dict[str, list[float]]:
    """Memories in a few tight clusters, so many pairs pass the similarity floor."""
    centres = [[rng.uniform(-1, 1) for _ in range(DIM)] for _ in range(5)]
    return {f"e:{i:03d}": _unit([value + rng.uniform(-0.15, 0.15) for value in rng.choice(centres)])
            for i in range(count)}


def _as_sets(neighbours: atlas.Neighbours) -> dict[str, set[tuple[float, str]]]:
    return {key: set(heap) for key, heap in neighbours.items()}


@pytest.mark.parametrize("seed", range(6))
def test_an_update_finds_the_same_neighbours_as_a_fresh_pass(seed: int) -> None:
    rng = random.Random(seed)
    vectors = _clustered(rng, 60)
    checksums = {key: 1 for key in vectors}
    first, stats = atlas.nearest_neighbours(vectors, checksums)
    assert stats["mode"] == "full" and any(first.values())
    previous = {"checksums": dict(checksums), "neighbours": first}

    # some memories go, some change, a few arrive
    keys = sorted(vectors)
    for key in rng.sample(keys, 3):
        del vectors[key], checksums[key]
    for key in rng.sample(sorted(vectors), 3):
        vectors[key], checksums[key] = _clustered(rng, 1)["e:000"], 2
    for i in range(4):
        vectors[f"e:new{i}"], checksums[f"e:new{i}"] = _clustered(rng, 1)["e:000"], 1

    updated, stats = atlas.nearest_neighbours(vectors, checksums, previous)
    fresh, _ = atlas.nearest_neighbours(vectors, checksums)

    if stats["mode"] == "incremental":  # unless the update would cost more than a fresh pass
        assert stats["changed"] == 7 and stats["rechecked"] > 0
        assert stats["compared"] < 61 * 60 // 2
    assert _as_sets(updated) == _as_sets(fresh)
    assert atlas.neighbour_edges(updated) == atlas.neighbour_edges(fresh)


def test_copies_keep_the_same_neighbours_whatever_the_order() -> None:
    vectors = {f"e:{name}": [1.0, 0.0] for name in "bcdef"}
    first, _ = atlas.nearest_neighbours(vectors, {key: 0 for key in vectors})
    vectors["e:a"] = [1.0, 0.0]
    checksums = {key: 0 for key in vectors}

    updated, stats = atlas.nearest_neighbours(vectors, checksums, {"checksums": {k: 0 for k in first},
                                                                    "neighbours": first})
    fresh, _ = atlas.nearest_neighbours(vectors, checksums)

    assert stats["mode"] == "incremental" and _as_sets(updated) == _as_sets(fresh)


def test_neighbours_are_the_nearest_k_above_the_floor() -> None:
    vectors = {"e:a": [1.0, 0.0], "e:b": _unit([0.95, 0.31]), "e:c": _unit([0.9, 0.44]), "e:d": [0.0, 1.0]}
    neighbours, _ = atlas.nearest_neighbours(vectors, {key: 0 for key in vectors}, k=1, minimum=0.8)

    assert {key: [other for _, other in heap] for key, heap in neighbours.items()} == {
        "e:a": ["e:b"], "e:b": ["e:c"], "e:c": ["e:b"], "e:d": []}
    edges = atlas.neighbour_edges(neighbours)
    assert [(a, b) for a, b, _, _ in edges] == [("e:a", "e:b"), ("e:b", "e:c")]


def test_a_near_copy_is_flagged_as_a_duplicate() -> None:
    vectors = {"e:a": [1.0, 0.0], "e:b": _unit([1.0, 0.01]), "e:c": _unit([0.85, 0.53])}
    edges = atlas.neighbour_edges(atlas.nearest_neighbours(vectors, {key: 0 for key in vectors})[0])

    assert {(a, b): dup for a, b, _, dup in edges} == {("e:a", "e:b"): 1, ("e:a", "e:c"): 0, ("e:b", "e:c"): 0}


def test_a_pass_writes_the_cache_and_the_next_compares_only_what_changed(loaded_manifest) -> None:
    centre = _vector(1)
    ids = []
    for i in range(6):
        stored = _record(loaded_manifest, f"Gateway note {i}", project="alpha")
        _embed(loaded_manifest, stored["id"], _vector(100 + i, near=centre))
        ids.append(stored["id"])
    hidden = _record(loaded_manifest, "Hidden twin", project="alpha", memory_guard={"visibility": "raw_only"})
    _embed(loaded_manifest, hidden["id"], centre)

    first = atlas.refresh_similar(loaded_manifest)
    assert first["status"] == "computed" and first["mode"] == "full" and first["memories"] == 6
    assert atlas.refresh_similar(loaded_manifest)["status"] == "fresh"

    later = _record(loaded_manifest, "One more gateway note", project="alpha")
    _embed(loaded_manifest, later["id"], _vector(200, near=centre))
    second = atlas.refresh_similar(loaded_manifest)

    assert second["mode"] == "incremental" and second["changed"] == 1
    cached = atlas.load_similar(atlas.similar_cache_path(config_from_manifest(loaded_manifest)))
    edges = atlas.neighbour_edges(cached["neighbours"])
    assert any(f"e:{later['id']}" in (a, b) for a, b, _, _ in edges)
    assert all(f"e:{hidden['id']}" not in (a, b) for a, b, _, _ in edges)  # quarantine reaches no edge
    atlas.refresh_similar(loaded_manifest, full=True)
    assert atlas.neighbour_edges(atlas.load_similar(
        atlas.similar_cache_path(config_from_manifest(loaded_manifest)))["neighbours"]) == edges


def test_the_signature_notices_what_timestamps_and_counts_miss(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    model = active_profile().key
    first = _record(loaded_manifest, "First", project="alpha")
    second = _record(loaded_manifest, "Second", project="alpha", memory_guard={"visibility": "raw_only"})
    for stored in (first, second):
        _embed(loaded_manifest, stored["id"], _vector(len(stored["id"])))

    def signature():
        with open_connection(config) as connection:
            return atlas.vector_digest(connection, model)

    before, shown = signature()
    assert shown == {f"e:{first['id']}"}
    with open_connection(config) as connection, connection:  # replaced in place, in the same second
        connection.execute("UPDATE event_vectors SET vector = ? WHERE event_id = ?",
                           (struct.pack(f"{DIM}f", *_vector(99)), first["id"]))
    replaced, _ = signature()
    with open_connection(config) as connection, connection:  # one leaves quarantine, the other enters it
        for table, column in (("events", "id"), ("event_observations", "event_id")):
            connection.execute(f"UPDATE {table} SET visibility = CASE {column} WHEN ? THEN 'raw_only' "
                               f"ELSE 'default' END WHERE {column} IN (?, ?)", (first["id"], first["id"], second["id"]))
    swapped, shown = signature()

    assert len({str(before), str(replaced), str(swapped)}) == 3
    assert before[1] == replaced[1] == swapped[1] == 1  # the same count each time
    assert shown == {f"e:{second['id']}"}


def test_edges_of_an_earlier_pass_leave_out_memories_hidden_since(loaded_manifest) -> None:
    centre = _vector(3)
    stored = [_record(loaded_manifest, f"Gateway {i}", project="alpha") for i in range(3)]
    for i, event in enumerate(stored):
        _embed(loaded_manifest, event["id"], _vector(30 + i, near=centre))
    atlas.refresh_similar(loaded_manifest)
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE events SET visibility = 'raw_only' WHERE id = ?", (stored[0]["id"],))

    class Waiting(atlas.SimilarRefresher):
        def ensure(self, log_path):
            return "running"

    body = atlas.similar_payload(loaded_manifest, Waiting(Path("unused.toml")))

    assert body["status"] == "stale" and body["edges"]
    assert all(f"e:{stored[0]['id']}" not in edge[:2] for edge in body["edges"])


def test_a_pass_runs_the_code_of_the_server_that_started_it() -> None:
    env = atlas._same_code_env()

    assert env["PYTHONPATH"].split(os.pathsep)[0] == str(REPO)  # a checkout: it comes first


def test_a_damaged_or_foreign_cache_is_no_cache(tmp_path) -> None:
    path = tmp_path / "similar.json"
    for content in ("not json", "[]", json.dumps({"version": 99}),
                    json.dumps({"version": atlas.SIMILAR_VERSION, "k": atlas.SIMILAR_K,
                                "minimum": atlas.SIMILAR_MIN, "neighbours": {"e:a": [["x", "e:b"]]}}),
                    json.dumps({"version": atlas.SIMILAR_VERSION, "k": atlas.SIMILAR_K, "model": "m",
                                "minimum": atlas.SIMILAR_MIN, "neighbours": {}, "checksums": {}})):
        path.write_text(content, encoding="utf-8")
        assert atlas.load_similar(path) is None, content


def test_the_cli_runs_a_pass(loaded_manifest, chronicle_sandbox) -> None:
    stored = _record(loaded_manifest, "Gateway", project="alpha")
    _embed(loaded_manifest, stored["id"], _vector(1))

    result = subprocess.run([sys.executable, "-P", "-m", "max_chronicle.cli", "--manifest",
                             str(chronicle_sandbox.manifest_path), "atlas", "similar"],
                            capture_output=True, text=True, check=True, env=atlas._same_code_env())

    assert json.loads(result.stdout)["status"] == "computed"
    assert cli.build_parser().parse_args(["atlas", "similar", "--full"]).full is True


def _similar(client, until: set[str], timeout_s: float = 60.0) -> dict:
    deadline = time.monotonic() + timeout_s
    while True:
        body = client.get("/ui/api/atlas/similar").json()
        if body["status"] in until or time.monotonic() > deadline:
            return body
        time.sleep(0.2)


def test_the_page_gets_edges_by_meaning_from_a_pass_of_their_own(client, loaded_manifest) -> None:
    _record(loaded_manifest, "Nothing embedded yet", project="alpha")
    assert client.get("/ui/api/atlas/similar").json() == {"status": "none", "edges": []}
    centre = _vector(7)
    ids = []
    for i in range(3):
        stored = _record(loaded_manifest, f"Gateway {i}", project="alpha")
        _embed(loaded_manifest, stored["id"], _vector(10 + i, near=centre))
        ids.append(f"e:{stored['id']}")

    first = client.get("/ui/api/atlas/similar").json()
    ready = _similar(client, {"ready", "failed"})

    assert first["status"] == "computing" and ready["status"] == "ready", ready
    assert {a for a, _, _, _ in ready["edges"]} | {b for _, b, _, _ in ready["edges"]} == set(ids)

    later = _record(loaded_manifest, "Gateway again", project="alpha")
    _embed(loaded_manifest, later["id"], _vector(20, near=centre))
    stale = client.get("/ui/api/atlas/similar").json()
    assert stale["status"] in ("stale", "ready") and stale["edges"]  # the earlier pass shows while one runs
    assert any(f"e:{later['id']}" in edge[:2] for edge in _similar(client, {"ready", "failed"})["edges"])


def test_a_failed_pass_is_not_started_again_at_once(tmp_path) -> None:
    refresher = atlas.SimilarRefresher(tmp_path / "no-manifest.toml", retry_s=60.0)
    log = tmp_path / "similar.log"

    assert refresher.ensure(log) == "running"
    deadline = time.monotonic() + 30
    while refresher.ensure(log) == "running" and time.monotonic() < deadline:
        time.sleep(0.1)

    assert refresher.ensure(log) == "failed"
    assert "atlas similar" in log.read_text(encoding="utf-8")


def test_the_pass_log_keeps_one_earlier_file(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(atlas, "SIMILAR_LOG_BYTES", 10)
    log = tmp_path / "similar.log"
    log.write_text("an earlier pass that wrote a long log\n", encoding="utf-8")

    atlas.SimilarRefresher(tmp_path / "no-manifest.toml").ensure(log)

    assert "an earlier pass" in (tmp_path / "similar.log.1").read_text(encoding="utf-8")
    assert "an earlier pass" not in log.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# The page and its routes
# ---------------------------------------------------------------------------


def test_the_page_and_its_files_carry_a_policy_that_runs_only_their_own_code(client) -> None:
    page = client.get("/ui/atlas")
    script = client.get("/ui/static/atlas/main.js")
    style = client.get("/ui/static/atlas/atlas.css")

    assert page.status_code == 200 and page.headers["content-type"].startswith("text/html")
    assert '<script type="module" src="/ui/static/atlas/main.js">' in page.text
    assert script.status_code == 200 and script.headers["content-type"].startswith("text/javascript")
    assert style.status_code == 200 and style.headers["content-type"].startswith("text/css")
    for response in (page, script, style):
        policy = response.headers["content-security-policy"]
        assert policy.startswith("default-src 'none'") and "script-src 'self'" in policy
        assert "unsafe-inline" not in policy and "unsafe-eval" not in policy
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["x-content-type-options"] == "nosniff"


def test_every_file_the_page_loads_is_served(client) -> None:
    names = {path.name for path in atlas.UI_DIR.iterdir() if path.suffix in (".js", ".css")}
    text = "".join((atlas.UI_DIR / name).read_text(encoding="utf-8") for name in names)
    text += atlas.page().decode()

    for name in names:
        assert client.get(f"/ui/static/atlas/{name}").status_code == 200, name
    for referenced in ("main.js", "atlas.css", "render.js", "timeline.js", "panel.js", "layout.worker.js"):
        assert referenced in text and referenced in names


def test_a_date_on_the_time_machine_never_passes_the_last_memory(tmp_path) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    # The map as served on 2026-10-29: the axis counts whole days from the first
    # memory, so it ran on to 30 October and the page said "Now · 30 Oct 2026".
    first = datetime(2026, 3, 15, 11, 48, 32, tzinfo=timezone.utc).timestamp()
    last = datetime(2026, 10, 29, 12, 46, 51, tzinfo=timezone.utc).timestamp()
    days = math.ceil((last - first) / 86400)
    module = tmp_path / "timeline.mjs"  # loads as a module on any node
    module.write_bytes((atlas.UI_DIR / "timeline.js").read_bytes())
    script = (f"import {{ momentAt }} from {json.dumps(module.as_uri())};"
              f"console.log(JSON.stringify([0, 10, {days} - 0.5, {days}].map((day) => momentAt([{first}, {last}], day))));")

    result = subprocess.run([node, "--input-type=module", "-e", script], capture_output=True, text=True, timeout=30)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [first, first + 10 * 86400, last, last]


@pytest.mark.parametrize("path", [
    "/ui/static/atlas/index.html",
    "/ui/static/atlas/..%2Fatlas.py",
    "/ui/static/atlas/%2E%2E%2Fatlas.py",
    "/ui/static/atlas/missing.js",
    "/ui/static/atlas/Main.js",
    "/ui/static/other/main.js",
    "/ui/api/nothing",
])
def test_nothing_else_is_served_from_the_atlas_paths(client, path) -> None:
    assert client.get(path).status_code == 404


@pytest.mark.parametrize("path", ["/ui/atlas", "/ui/static/atlas/main.js", "/ui/api/atlas",
                                  "/ui/api/atlas/similar", "/ui/api/item?id=event:x"])
@pytest.mark.parametrize("headers", [{"host": "evil.example"}, {"origin": "http://evil.example"},
                                     {"sec-fetch-site": "cross-site"}],
                         ids=["foreign-host", "foreign-origin", "cross-site"])
def test_the_atlas_answers_only_this_machine(client, path, headers) -> None:
    assert client.get(path, headers=headers).status_code == 403


def test_the_graph_api_serves_the_map(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Something to map", project="alpha")

    response = client.get("/ui/api/atlas")

    assert response.status_code == 200 and response.headers["content-type"] == "application/json"
    assert response.headers["content-security-policy"] == atlas.CSP
    assert f"e:{stored['id']}" in _nodes(response.json())


def test_the_item_api_gives_one_memory_in_full_and_nothing_hidden(client, loaded_manifest) -> None:
    shown = _record(loaded_manifest, "Decided to keep SQLite " + "with a long explanation " * 20, project="alpha",
                    task_id="storage", why="One file, one writer",
                    checkpoint={"goal": "Keep storage simple", "next_steps": ["Ship"]})
    hidden = _record(loaded_manifest, "Hidden", project="alpha", memory_guard={"visibility": "raw_only"})

    item = client.get(f"/ui/api/item?id=event:{shown['id']}").json()

    assert "long explanation" in item["text"] and len(item["text"]) > atlas.LABEL_CHARS
    assert item["why"] == "One file, one writer"
    assert item["checkpoint"]["goal"] == "Keep storage simple"
    assert client.get(f"/ui/api/item?id=event:{hidden['id']}").status_code == 404
    assert client.get("/ui/api/item?id=event:nothing").status_code == 404
    assert client.get("/ui/api/item?id=fact:x").status_code == 400
    assert client.get("/ui/api/item").status_code == 400


def test_without_a_database_the_api_says_so(client) -> None:
    response = client.get("/ui/api/atlas")

    assert response.status_code == 503 and response.json() == {"error": "Unavailable: FileNotFoundError"}


def test_the_old_pages_lead_to_the_atlas(client, loaded_manifest) -> None:
    _record(loaded_manifest, "Something", project="alpha")

    assert 'href="/ui/atlas"' in client.get("/ui").text
