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

    _record(manifest, "Launch done", project="STATUS", task_id="LAUNCH PLAN",
            fact={"slot": "task.status", "value": "completed", "kind": "observed"})
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
