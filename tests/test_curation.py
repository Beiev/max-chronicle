"""Light actions from the Atlas: each an event by the operator, and what agents, the brief and the map make of them."""

from __future__ import annotations

import json
import random

import pytest
from starlette.testclient import TestClient

from max_chronicle import atlas, curation, service
from max_chronicle.agent_surface import get_items, index_view
from max_chronicle.brief import BRIEF_PINS, build_brief
from max_chronicle.embeddings import active_profile, pack_vector
from max_chronicle.mcp_server import build_server
from max_chronicle.notes import read_note, sync_notes
from max_chronicle.recall import query_memory
from max_chronicle.store import (
    config_from_manifest,
    fetch_event,
    fetch_recent_events,
    open_connection,
    store_event_embedding,
)

BASE = "http://127.0.0.1:8093"
SAME_SITE = {"origin": BASE, "content-type": "application/json"}
DIM = 16


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _server(chronicle_sandbox, profile: str = "chronicler"):
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile=profile)
    return TestClient(server.streamable_http_app(), base_url=BASE)


@pytest.fixture()
def client(chronicle_sandbox):
    with _server(chronicle_sandbox) as test_client:
        yield test_client


def _record(manifest, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields},
                                append_compat=False)


def _vector(seed: int) -> list[float]:
    rng = random.Random(seed)
    return [rng.uniform(-1, 1) for _ in range(DIM)]


def _embed(manifest, event_id: str, vector: list[float]) -> None:
    store_event_embedding(config_from_manifest(manifest), event_id, vector, active_profile().key, len(vector))


def _act(manifest, action: str, target: str, **fields) -> dict:
    return curation.apply_action(manifest, action=action, target=target, **fields)


def _marks(manifest) -> curation.Marks:
    return curation.read_marks(config_from_manifest(manifest))


def _atlas_events(manifest) -> list[dict]:
    with open_connection(config_from_manifest(manifest)) as connection:
        return [dict(row) for row in connection.execute(
            "SELECT id, actor, category, source_kind, project, domain, text, payload_json FROM events "
            "WHERE category = 'curation' ORDER BY rowid")]


def _twins(manifest, *, project: str = "alpha") -> tuple[dict, dict]:
    """Two events that say the same thing, by their vectors."""
    original = _record(manifest, "Gateway listens on 9000", project=project)
    copy = _record(manifest, "The gateway listens on port 9000", project=project)
    vector = _vector(7)
    _embed(manifest, original["id"], vector)
    _embed(manifest, copy["id"], [value * 1.0001 for value in vector])
    return original, copy


# ---------------------------------------------------------------------------
# Every action is an event by the operator
# ---------------------------------------------------------------------------


def test_an_action_is_an_event_by_the_operator_on_the_memory_it_names(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha", domain="memory")

    result = _act(loaded_manifest, "pin", f"event:{stored['id']}")

    [event] = _atlas_events(loaded_manifest)
    assert result["status"] == "done" and result["event_id"] == event["id"]
    assert (event["actor"], event["source_kind"], event["project"], event["domain"]) == (
        "operator", curation.SOURCE_KIND, "alpha", "memory")
    assert json.loads(event["payload_json"])["curation"] == {
        "action": "pin", "target": f"event:{stored['id']}", "of": None}
    assert event["text"] == f"Atlas: event:{stored['id']} pinned to the brief."
    assert result["marks"]["pinned"]["event"] == f"event:{event['id']}"
    assert result["marks"]["pinned"]["by"] == "operator"


def test_taking_an_action_again_after_undoing_it_is_a_new_event(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    target = f"event:{stored['id']}"

    for action in ("pin", "unpin", "pin"):
        assert _act(loaded_manifest, action, target)["status"] == "done"

    assert [json.loads(e["payload_json"])["curation"]["action"] for e in _atlas_events(loaded_manifest)] == [
        "pin", "unpin", "pin"]
    assert target in _marks(loaded_manifest).pinned


def test_an_action_that_changes_nothing_writes_nothing(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    target = f"event:{stored['id']}"

    assert _act(loaded_manifest, "unpin", target)["status"] == "unchanged"
    assert _act(loaded_manifest, "fresh", target)["status"] == "unchanged"
    _act(loaded_manifest, "stale", target)
    assert _act(loaded_manifest, "stale", target)["status"] == "unchanged"

    assert len(_atlas_events(loaded_manifest)) == 1


def test_a_retried_request_is_taken_once(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    target = f"event:{stored['id']}"
    _act(loaded_manifest, "pin", target, request_id="click-1")
    _act(loaded_manifest, "unpin", target, request_id="click-2")

    again = _act(loaded_manifest, "pin", target, request_id="click-1")  # the first click, retried late

    assert again["replayed"] and target not in _marks(loaded_manifest).pinned
    assert len(_atlas_events(loaded_manifest)) == 2


def test_only_the_atlas_writes_actions(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    target = f"event:{stored['id']}"
    # An agent can record the category, even the action's shape: it is not an action.
    service.record_event(loaded_manifest, {"agent": "operator", "domain": "global", "category": "curation",
                                           "text": "Pin it", "curation": {"action": "pin", "target": target}},
                         append_compat=False, source_kind="chronicle_mcp")
    assert target not in _marks(loaded_manifest).pinned

    # Nor can another author through the Python API, even with the Atlas as its source.
    service.record_event(loaded_manifest, {"agent": "codex", "domain": "global", "category": "curation",
                                           "text": "Pin it", "curation": {"action": "pin", "target": target}},
                         append_compat=False, source_kind=curation.SOURCE_KIND)
    assert target not in _marks(loaded_manifest).pinned

    result = _act(loaded_manifest, "pin", target)
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE events SET visibility = 'raw_only' WHERE id = ?", (result["event_id"],))
    assert target not in _marks(loaded_manifest).pinned  # a quarantined action does not count


def test_actions_count_in_the_order_they_were_written_whatever_the_clock_said(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    target = f"event:{stored['id']}"
    _act(loaded_manifest, "pin", target)
    unpin = _act(loaded_manifest, "unpin", target)["event_id"]
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE events SET recorded_at_utc = '2020-01-01T00:00:00Z' WHERE id = ?", (unpin,))

    assert target not in _marks(loaded_manifest).pinned


@pytest.mark.parametrize(("action", "target", "of", "error"), [
    ("burn", "event:x", None, ValueError),
    ("pin", "fact:x", None, ValueError),
    ("pin", "event:", None, ValueError),
    ("pin", "event:../x", None, ValueError),
    ("pin", None, None, ValueError),
    ("pin", "event:missing", None, LookupError),
    ("duplicate", "event:a", None, ValueError),
    ("duplicate", "event:a", "event:a", ValueError),
    ("duplicate", "event:a", "note:b", ValueError),
])
def test_a_malformed_action_is_refused(loaded_manifest, action, target, of, error) -> None:
    _record(loaded_manifest, "Something", project="alpha")

    with pytest.raises(error):
        curation.apply_action(loaded_manifest, action=action, target=target, of=of)

    assert not _atlas_events(loaded_manifest)


def test_the_atlas_takes_no_action_on_its_own_events(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    own = _act(loaded_manifest, "pin", f"event:{stored['id']}")["event_id"]

    with pytest.raises(LookupError):
        _act(loaded_manifest, "stale", f"event:{own}")


def test_a_manifest_that_makes_the_operator_an_agent_is_refused(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    manifest = {**loaded_manifest, "agents": [{"id": "claude", "prefixes": ["claude"], "aliases": ["operator"]}]}

    with pytest.raises(PermissionError, match="aliases"):
        _act(manifest, "pin", f"event:{stored['id']}")

    assert not _atlas_events(loaded_manifest)


# ---------------------------------------------------------------------------
# Pinned and stale: the brief, recall and records
# ---------------------------------------------------------------------------


def _pinned_lines(text: str) -> list[str]:
    section = text.split("## Pinned by the operator\n", 1)
    return [] if len(section) == 1 else [line for line in section[1].split("\n\n")[0].splitlines()]


def test_the_brief_opens_with_what_the_operator_pinned(loaded_manifest) -> None:
    own = _record(loaded_manifest, "Alpha runs on SQLite", project="alpha")
    other = _record(loaded_manifest, "Beta runs on Postgres", project="beta")
    shared = _record(loaded_manifest, "Every service logs to journald")
    for stored in (shared, other, own):
        _act(loaded_manifest, "pin", f"event:{stored['id']}")

    alpha = build_brief(loaded_manifest, project="alpha")
    everything = build_brief(loaded_manifest)

    assert alpha["text"].index("## Pinned by the operator") < alpha["text"].index("## Protocol")
    assert _pinned_lines(alpha["text"]) == [f"- Alpha runs on SQLite (event {own['id']})",
                                            f"- Every service logs to journald (event {shared['id']})"]
    assert alpha["counts"]["pinned"] == 2
    assert _pinned_lines(everything["text"]) == [f"- [alpha] Alpha runs on SQLite (event {own['id']})",
                                                 f"- [beta] Beta runs on Postgres (event {other['id']})",
                                                 f"- Every service logs to journald (event {shared['id']})"]


def test_the_brief_keeps_a_few_pins_and_never_a_stale_or_hidden_one(loaded_manifest) -> None:
    pinned = [_record(loaded_manifest, f"Rule number {n}", project="alpha") for n in range(BRIEF_PINS + 2)]
    for stored in pinned:
        _act(loaded_manifest, "pin", f"event:{stored['id']}")
    _act(loaded_manifest, "stale", f"event:{pinned[-1]['id']}")

    lines = _pinned_lines(build_brief(loaded_manifest, project="alpha")["text"])

    assert len(lines) == BRIEF_PINS
    assert lines[0].endswith(f"(event {pinned[-2]['id']})")  # the latest pin that is not stale comes first
    assert not any(pinned[-1]["id"] in line for line in lines)


def test_a_stale_decision_leaves_the_brief(loaded_manifest) -> None:
    decision = _record(loaded_manifest, "Decided to ship on Fridays", project="alpha", category="decision")
    assert decision["id"] in build_brief(loaded_manifest, project="alpha")["text"]

    _act(loaded_manifest, "stale", f"event:{decision['id']}")

    assert decision["id"] not in build_brief(loaded_manifest, project="alpha")["text"]
    _act(loaded_manifest, "fresh", f"event:{decision['id']}")
    assert decision["id"] in build_brief(loaded_manifest, project="alpha")["text"]


def test_what_a_stale_event_set_leaves_the_brief_and_its_open_task_says_so(loaded_manifest) -> None:
    fact = _record(loaded_manifest, "Gateway on 9000", project="alpha",
                   fact={"slot": "gateway.port", "value": "9000", "kind": "decision"})
    checkpoint = _record(loaded_manifest, "Moving the gateway", project="alpha", task_id="move",
                         checkpoint={"goal": "Move the gateway", "next_steps": ["Ship"]})
    before = build_brief(loaded_manifest, project="alpha")["text"]
    assert "gateway.port = 9000" in before and "[checkpoint marked stale]" not in before

    for stored in (fact, checkpoint):
        _act(loaded_manifest, "stale", f"event:{stored['id']}")
    after = build_brief(loaded_manifest, project="alpha")["text"]

    assert "gateway.port" not in after
    assert "alpha/move" in after and "Move the gateway [checkpoint marked stale]" in after


def test_recall_marks_a_stale_memory_and_ranks_it_behind_current_ones(loaded_manifest) -> None:
    first = _record(loaded_manifest, "Gateway listens on port nine thousand for alpha", project="alpha")
    second = _record(loaded_manifest, "Gateway listens on port nine thousand for beta", project="alpha")
    before = [hit["event_id"] for hit in query_memory(loaded_manifest, query="gateway port nine thousand")["results"]]
    top, below = before[0], before[1]
    assert {top, below} == {first["id"], second["id"]}

    _act(loaded_manifest, "stale", f"event:{top}")
    _act(loaded_manifest, "pin", f"event:{below}")
    response = query_memory(loaded_manifest, query="gateway port nine thousand")
    hits = index_view(response)["results"]

    assert [hit["id"] for hit in hits] == [f"event:{below}", f"event:{top}"]
    assert hits[1]["stale"]["by"] == "operator" and "pinned" not in hits[1]
    assert hits[0]["pinned"]["by"] == "operator" and "stale" not in hits[0]


def test_a_record_carries_its_marks(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    plain = _record(loaded_manifest, "Nothing marked here", project="alpha")
    _act(loaded_manifest, "stale", f"event:{stored['id']}")

    items = {item["id"]: item for item in get_items(loaded_manifest, [f"event:{stored['id']}", plain["id"]])["items"]}
    recent = {event["id"]: event for event in fetch_recent_events(config_from_manifest(loaded_manifest), limit=10)}
    curation.annotate(recent.values(), _marks(loaded_manifest))

    assert items[f"event:{stored['id']}"]["stale"]["since"]
    assert "stale" not in items[f"event:{plain['id']}"] and "pinned" not in items[f"event:{plain['id']}"]
    assert recent[stored["id"]]["stale"]["by"] == "operator" and "stale" not in recent[plain["id"]]


def test_a_fact_is_stale_with_the_event_that_asserted_it_and_the_plain_page_says_so(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Gateway on 9000", project="alpha",
                     fact={"slot": "gateway.port", "value": "9000", "kind": "decision"})
    _act(loaded_manifest, "stale", f"event:{stored['id']}")
    _act(loaded_manifest, "pin", f"event:{stored['id']}")

    [fact] = get_items(loaded_manifest, [f"fact:{stored['fact_id']}"])["items"]
    page = client.get(f"/ui/event/{stored['id']}").text

    assert fact["stale"]["by"] == "operator" and "pinned" not in fact
    assert fact["event"]["stale"] and fact["event"]["pinned"]
    assert "pinned to the brief</span> since" in page and "stale</span> since" in page


# ---------------------------------------------------------------------------
# A copy hidden, and restored
# ---------------------------------------------------------------------------


def test_a_copy_of_an_event_is_hidden_everywhere_and_comes_back_as_it_was(loaded_manifest) -> None:
    original, copy = _twins(loaded_manifest)
    guard_before = fetch_event(config_from_manifest(loaded_manifest), event_id=copy["id"]).get("memory_guard")

    result = _act(loaded_manifest, "duplicate", f"event:{copy['id']}", of=f"event:{original['id']}")

    assert result["status"] == "done" and result["hidden"]
    hidden = fetch_event(config_from_manifest(loaded_manifest), event_id=copy["id"])
    assert hidden["visibility"] == "raw_only"
    assert hidden["memory_guard"]["source"] == curation.GUARD_SOURCE
    assert hidden["memory_guard"]["duplicate_of"] == f"event:{original['id']}"
    assert f"e:{copy['id']}" not in {node["id"] for node in atlas.graph_payload(loaded_manifest)["nodes"]}
    assert get_items(loaded_manifest, [f"event:{copy['id']}"])["missing"] == [f"event:{copy['id']}"]
    assert copy["id"] not in {hit["event_id"] for hit in query_memory(loaded_manifest, query="gateway 9000")["results"]}
    assert [entry["id"] for entry in atlas.item_payload(loaded_manifest, f"event:{original['id']}")["copies"]] == [
        f"event:{copy['id']}"]
    with pytest.raises(LookupError):  # a hidden copy takes no other action
        _act(loaded_manifest, "pin", f"event:{copy['id']}")

    restored = _act(loaded_manifest, "restore", f"event:{copy['id']}")

    assert restored["status"] == "done" and not restored["hidden"]
    back = fetch_event(config_from_manifest(loaded_manifest), event_id=copy["id"])
    assert back["visibility"] == "default" and back.get("memory_guard") == guard_before
    assert f"e:{copy['id']}" in {node["id"] for node in atlas.graph_payload(loaded_manifest)["nodes"]}
    assert atlas.item_payload(loaded_manifest, f"event:{original['id']}")["copies"] == []


def test_only_a_memory_that_says_the_same_thing_is_a_copy(loaded_manifest) -> None:
    original = _record(loaded_manifest, "Gateway listens on 9000", project="alpha")
    other = _record(loaded_manifest, "Lunch is at noon", project="alpha")
    bare = _record(loaded_manifest, "No vector for this one", project="alpha")
    _embed(loaded_manifest, original["id"], _vector(1))
    _embed(loaded_manifest, other["id"], _vector(2))

    with pytest.raises(PermissionError, match="similarity"):
        _act(loaded_manifest, "duplicate", f"event:{other['id']}", of=f"event:{original['id']}")
    with pytest.raises(PermissionError, match="no vectors"):
        _act(loaded_manifest, "duplicate", f"event:{bare['id']}", of=f"event:{original['id']}")

    assert not _atlas_events(loaded_manifest)


def test_a_copy_is_checked_again_in_the_write_transaction(loaded_manifest, monkeypatch) -> None:
    original, copy = _twins(loaded_manifest)
    measure = atlas.memory_similarity

    def meanwhile(connection, model_key, a, b):
        # Another writer hides the original between the check and the write.
        with open_connection(config_from_manifest(loaded_manifest)) as other, other:
            other.execute("UPDATE events SET visibility = 'raw_only' WHERE id = ?", (original["id"],))
        return measure(connection, model_key, a, b)

    monkeypatch.setattr(atlas, "memory_similarity", meanwhile)

    with pytest.raises(LookupError, match="no longer"):
        _act(loaded_manifest, "duplicate", f"event:{copy['id']}", of=f"event:{original['id']}")

    assert not _atlas_events(loaded_manifest)
    assert fetch_event(config_from_manifest(loaded_manifest), event_id=copy["id"])["visibility"] == "default"


def test_a_copy_of_a_hidden_memory_is_refused(loaded_manifest) -> None:
    original, copy = _twins(loaded_manifest)
    _act(loaded_manifest, "duplicate", f"event:{original['id']}", of=f"event:{copy['id']}")

    with pytest.raises(LookupError):
        _act(loaded_manifest, "duplicate", f"event:{copy['id']}", of=f"event:{original['id']}")


def test_a_copy_quarantined_since_for_another_reason_stays_hidden(loaded_manifest) -> None:
    original, copy = _twins(loaded_manifest)
    _act(loaded_manifest, "duplicate", f"event:{copy['id']}", of=f"event:{original['id']}")
    service.guard_event(loaded_manifest, event_id=copy["id"], verdict="local_only", reason="Noise", apply=True)

    with pytest.raises(PermissionError, match="another reason"):
        _act(loaded_manifest, "restore", f"event:{copy['id']}")

    assert fetch_event(config_from_manifest(loaded_manifest), event_id=copy["id"])["visibility"] == "raw_only"
    assert f"event:{copy['id']}" in _marks(loaded_manifest).copies
    # and the original no longer offers it back: it is not hidden as a copy now
    assert atlas.item_payload(loaded_manifest, f"event:{original['id']}")["copies"] == []


@pytest.fixture()
def twin_notes(loaded_manifest, tmp_path):
    folder = tmp_path / "notes"
    folder.mkdir()
    for name in ("ports", "ports-again"):
        (folder / f"{name}.md").write_text("# Ports\n\nThe gateway listens on 9000.\n", encoding="utf-8")
    manifest = {**loaded_manifest, "notes": {"paths": [str(folder / "*.md")]}}
    sync_notes(manifest, embed=False)
    vector = pack_vector(_vector(3))
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        ids = {path.rsplit("/", 1)[1]: identifier for identifier, path in
               connection.execute("SELECT id, path FROM documents ORDER BY path")}
        for (chunk_id,) in connection.execute("SELECT id FROM document_chunks").fetchall():
            connection.execute("INSERT INTO chunk_vectors(chunk_id, model_key, dim, vector, created_at_utc) "
                               "VALUES (?, ?, ?, ?, '2026-10-29T00:00:00Z')", (chunk_id, active_profile().key, DIM, vector))
    return manifest, folder, f"note:{ids['ports.md']}", f"note:{ids['ports-again.md']}"


def test_a_copy_of_a_note_is_left_out_where_notes_are_read_and_its_file_stays(twin_notes) -> None:
    manifest, folder, original, copy = twin_notes
    key = copy.partition(":")[2]

    _act(manifest, "duplicate", copy, of=original)

    assert read_note(manifest, key) is None
    assert get_items(manifest, [copy])["missing"] == [copy]
    assert {note["document_id"] for note in query_memory(manifest, query="gateway listens")["notes"]} == {
        original.partition(":")[2]}
    nodes = {node["id"] for node in atlas.graph_payload(manifest)["nodes"]}
    assert f"n:{key}" not in nodes and f"n:{original.partition(':')[2]}" in nodes
    with open_connection(config_from_manifest(manifest)) as connection:
        assert f"n:{key}" not in atlas.vector_digest(connection, active_profile().key)[1]
    assert (folder / "ports-again.md").read_text(encoding="utf-8").startswith("# Ports")
    sync_notes(manifest, embed=False)  # a sync of the unchanged file keeps it hidden
    assert read_note(manifest, key) is None

    _act(manifest, "restore", copy)

    assert read_note(manifest, key) is not None


# ---------------------------------------------------------------------------
# The map
# ---------------------------------------------------------------------------


def test_the_map_shows_marks_and_leaves_the_atlas_events_out(loaded_manifest) -> None:
    pinned = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    old = _record(loaded_manifest, "Decided to use MySQL", project="alpha")
    own = _act(loaded_manifest, "pin", f"event:{pinned['id']}")["event_id"]
    _act(loaded_manifest, "stale", f"event:{old['id']}")
    _embed(loaded_manifest, own, _vector(5))

    nodes = {node["id"]: node for node in atlas.graph_payload(loaded_manifest)["nodes"]}

    assert nodes[f"e:{pinned['id']}"].get("pi") == 1 and "st" not in nodes[f"e:{pinned['id']}"]
    assert nodes[f"e:{old['id']}"].get("st") == 1 and "pi" not in nodes[f"e:{old['id']}"]
    assert not any(node_id.startswith("e:") and node_id[2:] == own for node_id in nodes)
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        assert f"e:{own}" not in atlas.vector_digest(connection, active_profile().key)[1]


# ---------------------------------------------------------------------------
# The route
# ---------------------------------------------------------------------------


def _token(client) -> str:
    actions = client.get("/ui/api/atlas").json()["actions"]
    assert actions["enabled"]
    return actions["token"]


def test_the_page_takes_an_action_through_the_server(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")

    response = client.post("/ui/api/atlas/action", headers={**SAME_SITE, "x-chronicle-token": _token(client)},
                           json={"action": "pin", "target": f"event:{stored['id']}", "request_id": "r-1"})

    assert response.status_code == 200 and response.json()["status"] == "done"
    assert response.headers["content-security-policy"] == atlas.CSP
    assert _atlas_events(loaded_manifest)[0]["actor"] == "operator"
    item = client.get(f"/ui/api/item?id=event:{stored['id']}").json()
    assert item["pinned"]["by"] == "operator" and item["copies"] == []


@pytest.mark.parametrize("headers", [
    {"content-type": "application/json"},  # no Origin
    {**SAME_SITE, "origin": "http://evil.example"},
    {**SAME_SITE, "origin": "http://127.0.0.1:9999"},
    {**SAME_SITE, "host": "evil.example", "origin": "http://evil.example"},
    {**SAME_SITE, "sec-fetch-site": "cross-site"},
], ids=["no-origin", "foreign-origin", "other-port", "foreign-host", "cross-site"])
def test_an_action_from_anywhere_else_is_forbidden(client, loaded_manifest, headers) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")

    response = client.post("/ui/api/atlas/action", headers={**headers, "x-chronicle-token": _token(client)},
                           json={"action": "pin", "target": f"event:{stored['id']}"})

    assert response.status_code == 403 and not _atlas_events(loaded_manifest)


def test_an_action_without_the_token_is_forbidden(client, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    body = {"action": "pin", "target": f"event:{stored['id']}"}

    assert client.post("/ui/api/atlas/action", headers=SAME_SITE, json=body).status_code == 403
    assert client.post("/ui/api/atlas/action", headers={**SAME_SITE, "x-chronicle-token": "guess"},
                       json=body).status_code == 403
    assert not _atlas_events(loaded_manifest)


def test_the_route_says_what_is_wrong_with_a_request(client, loaded_manifest) -> None:
    original = _record(loaded_manifest, "Gateway listens on 9000", project="alpha")
    other = _record(loaded_manifest, "Lunch is at noon", project="alpha")
    _embed(loaded_manifest, original["id"], _vector(1))
    _embed(loaded_manifest, other["id"], _vector(2))
    headers = {**SAME_SITE, "x-chronicle-token": _token(client)}
    post = lambda **kw: client.post("/ui/api/atlas/action", headers={**headers, **kw.pop("headers", {})}, **kw)  # noqa: E731

    assert post(content=b"action=pin", headers={"content-type": "text/plain"}).status_code == 415
    assert post(content=b"[1]").status_code == 400
    assert post(content=b"{").status_code == 400
    assert post(content=b"{" + b" " * 5000 + b"}").status_code == 413
    assert post(json={"action": "burn", "target": f"event:{original['id']}"}).status_code == 400
    assert post(json={"action": "pin", "target": "event:missing"}).status_code == 404
    refused = post(json={"action": "duplicate", "target": f"event:{other['id']}", "of": f"event:{original['id']}"})
    assert refused.status_code == 409 and "same thing" in refused.json()["error"]
    assert client.get("/ui/api/atlas/action").status_code == 404
    assert not _atlas_events(loaded_manifest)


def test_a_read_only_server_takes_no_action(chronicle_sandbox, loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Decided to keep SQLite", project="alpha")
    with _server(chronicle_sandbox, profile="readonly") as reader:
        actions = reader.get("/ui/api/atlas").json()["actions"]
        response = reader.post("/ui/api/atlas/action", headers={**SAME_SITE, "x-chronicle-token": "x"},
                               json={"action": "pin", "target": f"event:{stored['id']}"})

    assert actions == {"enabled": False, "token": None}
    assert response.status_code == 403 and not _atlas_events(loaded_manifest)
