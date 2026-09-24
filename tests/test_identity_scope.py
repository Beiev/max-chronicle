"""AC-17: whichever spelling an agent, project, task or domain was written in, a filter by any spelling finds it.

Each test first writes history as it happened, before the registry declared
the aliases, then reads it back through a manifest that declares them.
"""

from __future__ import annotations

import asyncio
import base64
import json

import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import Implementation

from max_chronicle import service
from max_chronicle.brief import build_brief
from max_chronicle.mcp_server import build_server
from max_chronicle.memory import task_context
from max_chronicle.recall import query_memory
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import config_from_manifest, fetch_recent_events, open_connection


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _registered(manifest: dict) -> dict:
    """The same workspace with the registry declaring the aliases."""
    return {
        **manifest,
        "projects": [{"id": "max-chronicle", "aliases": ["status", "chronicle"]}],
        "domains": [dict(domain, aliases=["mem"]) if domain["id"] == "memory" else domain
                    for domain in manifest["domains"]],
    }


def _record(manifest: dict, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields})


def _checkpoint(manifest: dict, project: str, task_id: str, goal: str) -> dict:
    return _record(manifest, f"Checkpoint: {goal}", project=project, task_id=task_id,
                   checkpoint={"goal": goal, "next_steps": ["verify"]})


def _found(result: dict) -> set[str]:
    return {hit["event_id"] for hit in result["results"]}


def test_a_project_is_found_by_any_of_its_spellings(loaded_manifest) -> None:
    written = {_record(loaded_manifest, f"Ledger rotation note {spelling}", project=spelling)["id"]
               for spelling in ("status", "Chronicle", "max-chronicle")}
    other = _record(loaded_manifest, "Ledger rotation note elsewhere", project="other")["id"]
    manifest = _registered(loaded_manifest)

    for spelling in ("max-chronicle", "status", "CHRONICLE", "Max_Chronicle"):
        found = _found(query_memory(manifest, query="ledger rotation", project=spelling))
        assert found == written, spelling
        assert other not in found


def test_a_task_is_found_by_any_case_or_separator(loaded_manifest) -> None:
    same = {_record(loaded_manifest, f"Launch step {task}", project="alpha", task_id=task)["id"]
            for task in ("Launch_plan", "launch_plan", "launch plan")}
    video = _record(loaded_manifest, "Launch step video", project="alpha", task_id="Launch_plan-video")["id"]

    context = task_context(loaded_manifest, domain="global", project="alpha", task_id="LAUNCH-PLAN")
    assert {change["event_id"] for change in context["changes"]} == same
    assert _found(query_memory(loaded_manifest, query="launch step", project="alpha", task_id="launch_plan")) == same
    assert video not in _found(query_memory(loaded_manifest, query="launch step", project="alpha", task_id="Launch_plan"))


def test_open_tasks_merge_the_spellings_of_one_task(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "status", "Launch_plan", "Plan the launch")
    manifest = _registered(loaded_manifest)
    _checkpoint(manifest, "max-chronicle", "launch_plan", "Run the launch")
    _checkpoint(manifest, "max-chronicle", "Launch_plan-video", "Cut the video")

    tasks = task_context(manifest, domain="global", project="chronicle", task_id=None)["open_tasks"]
    assert [(task["project"], task["goal"]) for task in tasks] == [
        ("max-chronicle", "Cut the video"),
        ("max-chronicle", "Run the launch"),
    ]


def test_a_completion_written_before_the_registry_closes_the_task_under_every_spelling(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "max-chronicle", "launch_plan", "Run the launch")
    _checkpoint(loaded_manifest, "max-chronicle", "video", "Cut the video")
    _record(loaded_manifest, "Launch done", project="STATUS", task_id="LAUNCH PLAN",
            fact={"slot": "task.status", "value": "completed", "kind": "observed"})
    manifest = _registered(loaded_manifest)

    tasks = task_context(manifest, domain="global", project="max-chronicle", task_id=None)["open_tasks"]
    assert [task["goal"] for task in tasks] == ["Cut the video"]


def test_a_domain_is_found_by_any_of_its_spellings(loaded_manifest) -> None:
    stored = _record(loaded_manifest, "Memory domain note about vectors", domain="mem")["id"]
    manifest = _registered(loaded_manifest)
    config = config_from_manifest(manifest)

    assert stored in {event["id"] for event in fetch_recent_events(config, domain="memory", limit=20)}
    assert stored in _found(query_memory(manifest, query="vectors", domain="Memory"))
    assert stored not in _found(query_memory(manifest, query="vectors", domain="global"))


def test_a_write_stores_canonical_names_with_the_given_ones_beside_them(loaded_manifest) -> None:
    manifest = _registered(loaded_manifest)
    aliased = _record(manifest, "Aliased write", agent="Codex-CLI", project="status", domain="mem")["id"]
    canonical = _record(manifest, "Canonical write", agent="codex", project="max-chronicle")["id"]

    with open_connection(config_from_manifest(manifest)) as connection:
        rows = {row["id"]: row for row in connection.execute(
            "SELECT id, actor, title, circumstances, payload_json FROM events WHERE id IN (?, ?)", (aliased, canonical))}
    first, second = rows[aliased], rows[canonical]
    assert (first["actor"], first["title"], first["circumstances"]) == ("codex", "max-chronicle", "memory")
    payload = json.loads(first["payload_json"])
    assert (payload["actor_raw"], payload["project_raw"], payload["domain_raw"]) == ("Codex-CLI", "status", "mem")
    assert (second["actor"], second["title"]) == ("codex", "max-chronicle")
    assert not {"actor_raw", "project_raw", "domain_raw"} & set(json.loads(second["payload_json"]))


def test_a_fact_written_under_another_spelling_is_replaced_not_duplicated(loaded_manifest) -> None:
    old = _record(loaded_manifest, "Deploys go to staging", project="status",
                  fact={"slot": "deploy.target", "value": "staging", "kind": "decision"})["fact_id"]
    manifest = _registered(loaded_manifest)

    with pytest.raises(ValueError, match=f"supersedes=current fact {old}"):
        _record(manifest, "Deploys go to production", project="max-chronicle",
                fact={"slot": "deploy.target", "value": "production", "kind": "decision"})
    _record(manifest, "Deploys go to production", project="max-chronicle",
            fact={"slot": "deploy.target", "value": "production", "kind": "decision", "supersedes": old})

    facts = task_context(manifest, domain="global", project="status", task_id=None)["current_facts"]
    assert [(fact["slot"], fact["value"]) for fact in facts] == [("deploy.target", "production")]


def _legacy_cursor(scope: list, seq: int) -> str:
    """A cursor exactly as 0.12.0 issued it: the names as they were given."""
    return base64.urlsafe_b64encode(json.dumps({"v": 1, "scope": scope, "seq": seq}).encode()).decode()


def test_a_cursor_issued_before_the_registry_keeps_paging(loaded_manifest) -> None:
    before = _record(loaded_manifest, "Before the registry", project="status")["id"]
    manifest = _registered(loaded_manifest)
    newer = _record(manifest, "After the registry", project="max-chronicle")["id"]
    with open_connection(config_from_manifest(manifest)) as connection:
        position = connection.execute("SELECT seq FROM event_observations WHERE event_id=?", (before,)).fetchone()[0]
    cursor = _legacy_cursor(["global", "status", None], position)

    for spelling in ("max-chronicle", "chronicle", "status"):
        page = task_context(manifest, domain="global", project=spelling, task_id=None, since=cursor)
        assert [change["event_id"] for change in page["changes"]] == [newer], spelling
        again = task_context(manifest, domain="global", project="status", task_id=None, since=page["cursor"])
        assert again["changes"] == []

    with pytest.raises(ValueError, match="another scope"):
        task_context(manifest, domain="global", project="other", task_id=None, since=cursor)


def test_the_brief_of_an_alias_directory_is_the_canonical_project(loaded_manifest, tmp_path) -> None:
    _checkpoint(loaded_manifest, "status", "migrate", "Move the ledger")
    manifest = _registered(loaded_manifest)
    _checkpoint(manifest, "max-chronicle", "brief", "Ship the brief")

    brief = build_brief(manifest, cwd=str(tmp_path / "Projects" / "status"))

    assert (brief["project"], brief["project_source"]) == ("max-chronicle", "directory")
    assert "max-chronicle/migrate" in brief["text"] and "max-chronicle/brief" in brief["text"]


def _decode(result) -> dict:
    return json.loads(result.content[0].text)


def test_a_session_that_names_no_agent_is_attributed_to_its_client(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)

    async def scenario() -> dict:
        async with create_connected_server_and_client_session(
            server, client_info=Implementation(name="claude-code", version="2.1")
        ) as client:
            await client.call_tool("startup_bundle", {"mode": "brief"})
            return _decode(await client.call_tool("record_event", {"text": "Written without naming an agent"}))

    stored = asyncio.run(scenario())
    assert (stored["agent"], stored["actor_raw"], stored["agent_source"]) == ("claude", "claude-code", "client")


def test_a_retry_after_a_reconnect_is_the_same_request(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)
    arguments = {"text": "Retried after the transport dropped", "request_id": "retry-1"}

    async def scenario() -> tuple[dict, dict]:
        async with create_connected_server_and_client_session(
            server, client_info=Implementation(name="claude-code", version="2.1")
        ) as client:
            await client.call_tool("startup_bundle", {"mode": "brief", "agent": "memory-librarian"})
            first = _decode(await client.call_tool("record_event", arguments))
        async with create_connected_server_and_client_session(
            server, client_info=Implementation(name="claude-code", version="2.1")
        ) as client:
            await client.call_tool("startup_bundle", {"mode": "brief"})
            result = await client.call_tool("record_event", arguments)
            assert not result.isError, result.content[0].text
            return first, _decode(result)

    first, retried = asyncio.run(scenario())
    assert first["agent"] == "memory-librarian"
    assert (retried["id"], retried["chronicle_status"]) == (first["id"], "existing")


# ---------------------------------------------------------------------------
# Review of #13: each case failed before its fix
# ---------------------------------------------------------------------------


def _synthetic_key() -> str:
    return "sk-" + "proj-" + "Q7fZ2mK9xLp4Rt8Vw3Nc6Bh1Jd5Gy0Ua"


def test_a_given_spelling_passes_the_secret_filter(loaded_manifest, chronicle_sandbox) -> None:
    key = _synthetic_key()
    stored = _record(loaded_manifest, "A spelling with a key", agent=f"codex {key}")

    assert stored["agent"] == "codex" and sum(stored["redactions"].values()) >= 1
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        rows = [json.dumps([tuple(row) for row in connection.execute(f"SELECT * FROM {table}")])
                for table in ("events", "event_observations")]
    ledger = (chronicle_sandbox.status_root / "ssot-ledger.jsonl").read_text()
    assert not any(key in text for text in [*rows, ledger])


def test_a_retry_after_an_upgrade_or_a_new_alias_is_the_same_request(loaded_manifest) -> None:
    from max_chronicle.memory import _hash, _request_input

    arguments = {"text": "Retried across a deploy", "request_id": "retry-deploy", "agent": "claude-code",
                 "agent_source": "client", "project": "old-atlas"}
    first = _record(loaded_manifest, **arguments)
    # 0.12.0 hashed the same input with the agent its MCP layer had normalised.
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection, connection:
        stored = json.loads(connection.execute(
            "SELECT payload_json FROM event_observations WHERE request_id=?", ("retry-deploy",)).fetchone()[0])
        legacy = _hash(_request_input(stored) | {"agent": "claude"})
        connection.execute("UPDATE event_observations SET request_hash=? WHERE request_id=?", (legacy, "retry-deploy"))
    manifest = {**loaded_manifest, "projects": [{"id": "atlas", "aliases": ["old-atlas"]}]}

    retried = _record(manifest, **arguments)

    assert (retried["id"], retried["chronicle_status"]) == (first["id"], "existing")


def test_two_agents_confirming_the_same_text_are_two_observations(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)

    async def scenario() -> list[str]:
        async with create_connected_server_and_client_session(
            server, client_info=Implementation(name="claude-code", version="2.1")
        ) as client:
            await client.call_tool("startup_bundle", {"mode": "brief", "agent": "claude"})
            first = _decode(await client.call_tool("record_event", {"text": "The launch slips a week"}))
            await client.call_tool("startup_bundle", {"mode": "brief", "agent": "codex"})
            await client.call_tool("record_event", {"text": "The launch slips a week"})
            return first["id"]

    event_id = asyncio.run(scenario())
    with open_connection(config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))) as connection:
        authors = [row[0] for row in connection.execute(
            "SELECT actor FROM event_observations WHERE event_id=? ORDER BY seq", (event_id,))]
    assert authors == ["claude", "codex"]


def test_replacing_a_value_settles_every_spelling_of_its_slot(loaded_manifest) -> None:
    _record(loaded_manifest, "Old atlas deploys to staging", project="old-atlas",
            fact={"slot": "deploy.target", "value": "staging", "kind": "decision"})
    newest = _record(loaded_manifest, "Atlas deploys to production", project="atlas",
                     fact={"slot": "deploy.target", "value": "production", "kind": "decision"})["fact_id"]
    manifest = {**loaded_manifest, "projects": [{"id": "atlas", "aliases": ["old-atlas"]}]}

    def values() -> list[str]:
        facts = task_context(manifest, domain="global", project="atlas", task_id=None)["current_facts"]
        return sorted(fact["value"] for fact in facts if fact["slot"] == "deploy.target")

    assert values() == ["production", "staging"]  # the conflict stays visible until someone settles it
    _record(manifest, "Atlas deploys to canary", project="atlas",
            fact={"slot": "deploy.target", "value": "canary", "kind": "decision", "supersedes": newest})
    assert values() == ["canary"]


def test_a_cursor_holds_for_any_spelling_of_undeclared_names(loaded_manifest) -> None:
    _record(loaded_manifest, "First", project="Other_Project", domain="Custom_Domain")
    cursor = task_context(loaded_manifest, domain="Custom_Domain", project="Other_Project", task_id=None)["cursor"]
    newer = _record(loaded_manifest, "Second", project="other-project", domain="custom-domain")["id"]

    page = task_context(loaded_manifest, domain="custom domain", project="other project", task_id=None, since=cursor)

    assert [change["event_id"] for change in page["changes"]] == [newer]


def test_snapshots_under_an_old_domain_spelling_stay_visible(loaded_manifest) -> None:
    from max_chronicle.store import fetch_latest_snapshot, store_snapshot
    from max_chronicle.service import reconstruct_timeline

    config = config_from_manifest(loaded_manifest)
    store_snapshot(config, {"domain": "mem", "title": "historical-snapshot"})
    manifest = _registered(loaded_manifest)
    config = config_from_manifest(manifest)

    for spelling in ("mem", "memory"):
        assert fetch_latest_snapshot(config, domain=spelling)["title"] == "historical-snapshot"
    from datetime import datetime, timezone
    timeline = reconstruct_timeline(manifest, timestamp=datetime.now(timezone.utc), domain="memory")
    assert len(timeline["nearest_snapshots"]) == 1


def test_a_domain_alias_works_for_every_tool_that_takes_a_domain(chronicle_sandbox) -> None:
    from max_chronicle.service import build_sources_audit, capture_runtime_snapshot, query_context

    path = chronicle_sandbox.manifest_path
    path.write_text(path.read_text().replace('id = "memory"', 'id = "memory"\naliases = ["mem"]', 1))
    manifest = load_manifest(path)

    assert "mem" in manifest["domain_map"] and manifest["domain_map"]["Mem"]["id"] == "memory"
    assert query_context(manifest, query="chronicle", domain="mem") is not None
    assert build_sources_audit(manifest, domain_id="mem")["domain"]["id"] == "memory"
    assert capture_runtime_snapshot(manifest, domain_id="mem", agent="pytest")["id"]


def test_the_same_text_under_two_spellings_of_a_task_is_one_event(loaded_manifest) -> None:
    first = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Draft ready",
                                                   "project": "alpha", "task_id": "Launch_plan"}, dedupe=True)
    second = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Draft ready",
                                                    "project": "alpha", "task_id": "launch-plan"}, dedupe=True)

    assert second["id"] == first["id"] and second["dedupe_status"] == "exact_duplicate"


# Second review of #13.


@pytest.mark.parametrize("first_agent", [None, "mcp"])
def test_a_retry_cannot_change_the_author_of_an_earlier_request(loaded_manifest, first_agent) -> None:
    fields = {"domain": "global", "text": "Shipped the release", "request_id": "retry-author"}
    service.record_event(loaded_manifest, {**fields, **({"agent": first_agent} if first_agent else {})})

    with pytest.raises(ValueError, match="different input"):
        service.record_event(loaded_manifest, {**fields, "agent": "different-author"})


@pytest.mark.parametrize("agent", ["Review_Bot", "My Agent", "Орбита-бот"])
def test_a_retry_of_a_0_12_request_keeps_its_agent_spelling(loaded_manifest, agent) -> None:
    from max_chronicle.memory import _hash, _request_input

    arguments = {"text": "Retried across a deploy", "request_id": f"retry-{len(agent)}", "agent": agent}
    first = _record(loaded_manifest, **arguments)
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection, connection:
        stored = json.loads(connection.execute("SELECT payload_json FROM event_observations WHERE request_id=?",
                                               (arguments["request_id"],)).fetchone()[0])
        legacy = _hash(_request_input(stored) | {"agent": agent.strip().lower()})  # what 0.12.0's MCP server stored
        connection.execute("UPDATE event_observations SET request_hash=? WHERE request_id=?",
                           (legacy, arguments["request_id"]))

    retried = _record(loaded_manifest, **arguments)

    assert (retried["id"], retried["chronicle_status"]) == (first["id"], "existing")


def test_a_secret_in_a_name_is_never_stored(loaded_manifest, chronicle_sandbox) -> None:
    key = "sk-" + "proj-" + base64.urlsafe_b64encode(bytes(range(40))).decode().rstrip("=")
    receipt = service.record_event(loaded_manifest, {"agent": f"bot {key}", "domain": "global", "text": "Named badly",
                                                     "project": f"atlas {key}", "task_id": f"plan {key}"})

    assert sum(receipt["redactions"].values()) >= 3
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        stored = json.dumps([[tuple(row) for row in connection.execute(f"SELECT * FROM {table}")]
                             for table in ("events", "event_observations")])
    assert key not in stored
    ledger = chronicle_sandbox.status_root / "ssot-ledger.jsonl"
    assert not ledger.exists() or key not in ledger.read_text(encoding="utf-8")


def test_an_event_under_an_old_spelling_dedupes_with_its_canonical_name(loaded_manifest) -> None:
    first = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Draft ready",
                                                   "project": "old-atlas", "task_id": "plan"}, dedupe=True)
    manifest = {**loaded_manifest, "projects": [{"id": "atlas", "aliases": ["old-atlas"]}]}

    second = service.record_event(manifest, {"agent": "a", "domain": "global", "text": "Draft ready",
                                             "project": "atlas", "task_id": "plan"}, dedupe=True)

    assert second["id"] == first["id"] and second["dedupe_status"] == "exact_duplicate"


def test_content_hash_dedupe_folds_the_task_id(loaded_manifest, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_ENABLE_EVENT_HASH_DEDUP", "1")
    fields = {"agent": "a", "domain": "global", "text": "Draft ready", "project": "alpha"}
    first = service.record_event(loaded_manifest, {**fields, "task_id": "Launch_plan"}, source_kind="agent_command")

    second = service.record_event(loaded_manifest, {**fields, "task_id": "launch-plan"}, source_kind="agent_command")

    assert second["id"] == first["id"] and second["dedupe_status"] == "content_hash_match"
