"""The agent surface: six tools, writes without a gate, short recall hits, full records on request."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from max_chronicle import service
from max_chronicle.agent_surface import GIST_CHARS, get_items, gist, index_view
from max_chronicle.mcp_server import build_server


def _decode(result: Any) -> Any:
    result = getattr(result, "content", result)  # SDK 2: a CallToolResult
    if isinstance(result, tuple):
        return _decode(result[0])
    if isinstance(result, list) and result and hasattr(result[0], "text"):
        return json.loads(result[0].text)
    return result


def _tools(manifest_path, profile: str) -> set[str]:
    async def collect() -> set[str]:
        server = build_server(manifest_path=manifest_path, profile=profile)
        return {tool.name for tool in await server.list_tools()}

    return asyncio.run(collect())


def _call(server, name: str, arguments: dict) -> Any:
    return asyncio.run(server.call_tool(name, arguments))


AGENT_TOOLS = {"startup_bundle", "query_memory", "get_memory", "record_event", "recent_events", "state_at"}
ADMIN_TOOLS = {"query_context", "sources_audit", "activate_agent", "capture_snapshot", "entity_admin"}


def test_the_default_surface_is_the_six_tools_agents_use(chronicle_sandbox) -> None:
    assert _tools(chronicle_sandbox.manifest_path, "chronicler") == AGENT_TOOLS


def test_the_admin_profile_adds_the_maintenance_tools(chronicle_sandbox) -> None:
    assert _tools(chronicle_sandbox.manifest_path, "admin") >= AGENT_TOOLS | ADMIN_TOOLS


def test_the_read_only_profile_cannot_write(chronicle_sandbox) -> None:
    assert _tools(chronicle_sandbox.manifest_path, "readonly") == AGENT_TOOLS - {"record_event"}


def test_a_write_needs_no_startup_and_carries_a_reminder_until_one(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    async def run() -> tuple[dict, dict]:
        first = _decode(await server.call_tool("record_event", {
            "text": "Decided to ship on Tuesdays", "agent": "agent-a", "project": "alpha"}))
        await server.call_tool("startup_bundle", {"agent": "agent-a", "project": "alpha"})
        second = _decode(await server.call_tool("record_event", {
            "text": "Decided to keep the Tuesday release", "agent": "agent-a", "project": "alpha"}))
        return first, second

    first, second = asyncio.run(run())

    assert first["chronicle_status"] == "stored" and first["notice"] == "startup_not_read"
    assert "startup_bundle" in first["hint"]
    assert second["chronicle_status"] == "stored" and "notice" not in second


def test_the_reminder_is_per_agent_not_per_connection(chronicle_sandbox) -> None:
    """A reconnect (a new MCP session) keeps what the agent read; the old gate lost it."""
    first = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    asyncio.run(first.call_tool("startup_bundle", {"agent": "agent-a", "project": "alpha"}))

    stored = _decode(_call(first, "record_event", {"text": "Finding: cache warmed", "agent": "agent-a",
                                                   "project": "alpha"}))
    other_agent = _decode(_call(first, "record_event", {"text": "Finding: queue drained", "agent": "agent-b",
                                                        "project": "alpha"}))

    assert "notice" not in stored and other_agent["notice"] == "startup_not_read"


def test_startup_with_a_task_returns_its_checkpoint(chronicle_sandbox, loaded_manifest) -> None:
    service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "alpha", "task_id": "migrate",
        "text": "Handoff", "checkpoint": {"goal": "Move storage to SQLite", "next_steps": ["Run the backfill"]}})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    payload = _decode(_call(server, "startup_bundle", {"agent": "agent-b", "project": "alpha", "task_id": "migrate"}))

    assert "Chronicle memory brief" in payload["brief"]
    assert payload["task_context"]["checkpoint"]["checkpoint"]["goal"] == "Move storage to SQLite"
    assert payload["task_context"]["cursor"]


def test_startup_without_a_task_stays_the_brief(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    payload = _decode(_call(server, "startup_bundle", {"agent": "agent-a", "project": "alpha"}))

    assert set(payload) >= {"brief", "project", "counts"} and "task_context" not in payload


def test_gist_flattens_and_cuts_on_a_character_budget() -> None:
    text, cut = gist("line one\n\n   line two " + "x" * 400)

    assert "\n" not in text and len(text) == GIST_CHARS and text.endswith("…") and cut
    assert gist("short") == ("short", False) and gist(None) == (None, False)


def test_index_view_keeps_flags_and_one_line_per_hit() -> None:
    response = {
        "query": "cache", "project": "alpha",
        "results": [{"event_id": "e1", "text": "Decided to clear the cache " + "y" * 500, "why": "stale builds",
                     "project": "alpha", "task_id": None, "agent": "claude", "category": "decision",
                     "occurred_at_utc": "2026-10-01T10:00:00Z", "source_files": ["/tmp/a"],
                     "channels": {"fts_rank": 0, "vector_similarity": 0.71234}, "provenance": {"big": "x" * 999}}],
        "notes": [{"document_id": "d1", "title": "Cache", "heading": "Rule", "text": "Clear it…",
                   "project": None, "modified_at_utc": "2026-09-01T00:00:00Z",
                   "channels": {"fts_rank": None, "vector_similarity": 0.6}}],
        "no_confident_match": True, "relaxed": False, "degraded": False, "hint": "verify",
    }

    view = index_view(response)

    hit = view["results"][0]
    assert hit["id"] == "event:e1" and hit["truncated"] and hit["evidence_files"] == 1
    assert hit["match"] == {"words": True, "similarity": 0.712} and "provenance" not in hit
    assert view["notes"][0]["id"] == "note:d1" and view["notes"][0]["truncated"]
    assert view["no_confident_match"] is True and view["hint"] == "verify" and "get_memory" in view["next"]


def test_recall_over_mcp_defaults_to_the_index_and_full_on_request(chronicle_sandbox, loaded_manifest) -> None:
    stored = service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "project": "alpha",
                                                    "text": "Decided to move the gateway to port 9000"})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    index = _decode(_call(server, "query_memory", {"query": "gateway port", "project": "alpha"}))
    full = _decode(_call(server, "query_memory", {"query": "gateway port", "project": "alpha", "detail": "full"}))

    assert index["results"][0]["id"] == f"event:{stored['id']}" and "provenance" not in index["results"][0]
    assert full["results"][0]["event_id"] == stored["id"] and "provenance" in full["results"][0]


def test_get_memory_returns_full_records_and_names_the_missing(chronicle_sandbox, loaded_manifest) -> None:
    stored = service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "project": "alpha",
                                                    "text": "Decided to move the gateway", "why": "load test"})
    hidden = service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Quarantined",
                                                    "memory_guard": {"visibility": "raw_only"}})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    payload = _decode(_call(server, "get_memory", {"ids": [f"event:{stored['id']}", hidden["id"], "note:nope"]}))

    assert [item["id"] for item in payload["items"]] == [f"event:{stored['id']}"]
    assert payload["items"][0]["why"] == "load test" and "provenance" in payload["items"][0]
    assert payload["missing"] == [hidden["id"], "note:nope"]


def test_get_items_rejects_no_ids_and_too_many_ids(loaded_manifest) -> None:
    with pytest.raises(ValueError):
        get_items(loaded_manifest, [])
    with pytest.raises(ValueError):
        get_items(loaded_manifest, [f"event:{index}" for index in range(21)])


def test_get_items_reads_ids_as_agents_type_them(loaded_manifest) -> None:
    """One odd id used to fail the whole call of twenty."""
    stored = service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": "Moved the gateway"})
    event_id = stored["id"]

    payload = get_items(loaded_manifest, [f" event:{event_id}", f"EVENT: {event_id} ", event_id, "event:", "task:7"])

    assert [item["id"] for item in payload["items"]] == [f"event:{event_id}"]
    assert payload["missing"] == ["event:", "task:7"]


def test_a_fact_id_from_the_brief_returns_the_fact_and_its_event(loaded_manifest) -> None:
    stored = service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "alpha", "text": "Gateway port decided",
        "fact": {"slot": "gateway.port", "value": "9000", "kind": "decision"}})
    hidden = service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "text": "Quarantined fact",
        "fact": {"slot": "hidden.slot", "value": "x", "kind": "decision"},
        "memory_guard": {"visibility": "raw_only"}})

    payload = get_items(loaded_manifest, [stored["fact_id"], f"fact:{stored['fact_id']}", f"fact:{hidden['fact_id']}"])

    assert [item["id"] for item in payload["items"]] == [f"fact:{stored['fact_id']}"]
    fact = payload["items"][0]
    assert fact["slot"] == "gateway.port" and fact["value"] == "9000" and fact["current"]
    assert fact["event"]["id"] == f"event:{stored['id']}"
    assert payload["missing"] == [f"fact:{hidden['fact_id']}"]


def test_the_index_says_when_meaning_search_saw_part_of_the_memory() -> None:
    response = {"results": [], "vector_coverage": {"model_key": "m", "eligible": 10, "compatible": 4,
                                                   "missing_or_incompatible": 6}}

    assert index_view(response)["vector_coverage"] == {"eligible": 10, "missing_or_incompatible": 6}
    assert "vector_coverage" not in index_view({"results": [], "vector_coverage": {"missing_or_incompatible": 0}})


def test_a_cursor_saved_from_the_bundle_resumes_in_the_brief(chronicle_sandbox, loaded_manifest) -> None:
    """The default moved from the bundle to the brief; a saved cursor must survive it."""
    service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "alpha", "task_id": "migrate",
        "text": "Handoff", "checkpoint": {"goal": "Move storage", "next_steps": ["Backfill"]}})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    bundle = _decode(_call(server, "startup_bundle", {"agent": "agent-b", "project": "alpha", "task_id": "migrate",
                                                      "mode": "bundle"}))
    service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "alpha", "task_id": "migrate", "text": "Backfill started"})

    resumed = _decode(_call(server, "startup_bundle", {"agent": "agent-b", "project": "alpha", "task_id": "migrate",
                                                       "since": bundle["task_context"]["cursor"]}))

    assert [change["text"] for change in resumed["task_context"]["changes"]] == ["Backfill started"]


def test_a_brief_start_respects_the_limit_asked(chronicle_sandbox, loaded_manifest) -> None:
    for index in range(4):
        service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "project": "alpha",
                                               "task_id": "migrate", "text": f"Step {index}"})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    payload = _decode(_call(server, "startup_bundle", {"project": "alpha", "task_id": "migrate", "limit": 1}))

    assert [change["text"] for change in payload["task_context"]["changes"]] == ["Step 3"]


def test_a_bundle_only_argument_returns_the_bundle(chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    payload = _decode(_call(server, "startup_bundle", {"agent": "agent-a", "project": "alpha", "focus": "gateway"}))

    assert "brief" not in payload and "counts" not in payload


def test_prompts_treat_an_unfilled_placeholder_as_no_argument(chronicle_sandbox) -> None:
    """Slash-command clients sent the literal "$1"; every render failed on it."""
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    async def run():
        names = {prompt.name for prompt in await server.list_prompts()}
        rendered = await server.get_prompt("continue_work", {"project": "$1", "task_id": "$2"})
        return names, rendered

    names, rendered = asyncio.run(run())

    assert names == {"continue_work", "reconstruct_moment"}
    assert "Chronicle memory brief" in rendered.messages[0].content.text
