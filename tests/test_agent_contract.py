from __future__ import annotations

import asyncio
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

import max_chronicle.mcp_server as mcp_server_module
from max_chronicle.mcp_server import _startup_gate_key, build_server
from max_chronicle.runtime_context import load_manifest, search_mem0_dump
from max_chronicle.service import (
    backfill_mem0_queue,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    default_mem0_status,
    guard_event,
    materialize_normalized_entities,
    materialize_situation_model,
    query_context,
    record_scenario,
    record_event,
    reconstruct_timeline,
    review_scenario,
    run_lenses,
)
from max_chronicle.store import (
    config_from_manifest,
    fetch_normalized_entities,
    open_connection,
    store_snapshot,
    upsert_normalized_entity,
)


PROJECT_STATUS_ROOT = Path("/Users/maksymbeiev/Projects/status")
SSOT_HUB = PROJECT_STATUS_ROOT / "scripts" / "ssot_hub.py"


def _ssot_hub(manifest_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_STATUS_ROOT)
    return subprocess.run(
        [sys.executable, str(SSOT_HUB), "--manifest", str(manifest_path), *args],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )


def _chronicle_cli(manifest_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_STATUS_ROOT)
    return subprocess.run(
        [sys.executable, "-m", "max_chronicle.cli", "--manifest", str(manifest_path), *args],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )


def _sandbox_mcp_server(manifest_path: Path, *, profile: str = "chronicler"):
    return build_server(manifest_path=manifest_path, profile=profile)


def _decode_mcp_json(result: Any) -> Any:
    if isinstance(result, tuple):
        return _decode_mcp_json(result[0])
    if isinstance(result, list) and result and hasattr(result[0], "text"):
        return json.loads(result[0].text)
    return result


def test_build_activation_returns_versioned_attach_bundle(loaded_manifest) -> None:
    activation = build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Attach bundle test",
        focus="tests",
        capture=True,
    )

    assert activation["contract_name"] == "max-chronicle"
    assert activation["contract_version"] == "2026-03-16.v1"
    assert activation["attach_bundle"]["contract_version"] == activation["contract_version"]
    assert activation["attach_bundle"]["snapshot"]["id"] == activation["snapshot"]["id"]
    assert activation["attach_bundle"]["domain"]["id"] == "global"
    assert activation["attach_bundle"]["freshness_audit"]["domain"]["id"] == "global"
    assert activation["attach_bundle"]["source_catalog"][0]["trust_tier_source"] == "explicit"
    assert activation["attach_bundle"]["source_catalog"]
    assert activation["attach_bundle"]["attach_bundle_schema_version"] == "memory-v2"
    assert activation["attach_bundle"]["source_catalog_schema_version"] == "2026-03-21.v1"
    assert activation["attach_bundle"]["lane_policy_schema_version"] == "2026-03-21.v1"
    assert activation["attach_bundle"]["query_modes"] == [
        "truth_only",
        "truth_plus_interpretation",
        "truth_plus_interpretation_plus_scenarios",
    ]
    assert activation["attach_bundle"]["source_audit"]["coverage"]["total_sources"] >= len(activation["attach_bundle"]["source_catalog"])
    assert activation["attach_bundle"]["situation_model"]["domain"] == "global"
    assert len(activation["attach_bundle"]["latest_lens_runs"]) == 5
    assert "Activation contract: max-chronicle 2026-03-16.v1" in activation["prompt"]


def test_build_activation_without_capture_keeps_attach_path_non_mutating(loaded_manifest) -> None:
    activation = build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Read-only attach",
        focus="tests",
        capture=False,
    )

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert activation["snapshot"]["id"]
    assert activation["attach_bundle"]["snapshot"]["id"] == activation["snapshot"]["id"]
    assert snapshot_count == 0


def test_query_context_annotates_sources_and_returns_chronicle_hits(loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Chronicle test attach contract query regression.",
            "why": "Need a canonical event hit for query contract coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract",
    )

    payload = query_context(loaded_manifest, query="Chronicle test", domain="global", limit=5)
    assert payload["contract_name"] == "max-chronicle"
    assert payload["contract_version"] == "2026-03-16.v1"
    assert payload["ranking_basis"] == ["text_score", "freshness", "trust", "source_priority"]
    assert payload["freshness_audit"]["domain"]["id"] == "global"
    assert payload["chronicle_hits"]
    assert payload["chronicle_hits"][0]["source_class"] == "canonical_event"
    assert payload["chronicle_hits"][0]["trust_tier"] == "canonical"
    assert payload["status_hits"]
    assert payload["status_hits"][0]["source_class"] == "ssot_source"
    assert payload["status_hits"][0]["trust_tier"] in {"operator_curated", "reference", "unknown"}
    assert payload["status_hits"][0]["freshness_status"] in {"live", "recent", "stale", "archival", "unknown"}


def test_ssot_hub_activate_bundle_uses_service_contract(chronicle_sandbox) -> None:
    result = _ssot_hub(
        chronicle_sandbox.manifest_path,
        "activate",
        "--domain",
        "global",
        "--agent",
        "pytest",
        "--format",
        "bundle",
        "--no-capture",
    )
    payload = json.loads(result.stdout)
    assert payload["contract_name"] == "max-chronicle"
    assert payload["contract_version"] == "2026-03-16.v1"
    assert payload["attach_bundle"]["domain"]["id"] == "global"
    assert payload["prompt"].startswith("You are connecting to Max Chronicle")
    assert "deprecated" in result.stderr.casefold()


def test_ssot_hub_query_uses_service_query_context(chronicle_sandbox, loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Chronicle test query service path is active.",
            "why": "CLI should expose canonical Chronicle hits before compatibility-only fallbacks.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.cli",
    )

    result = _ssot_hub(
        chronicle_sandbox.manifest_path,
        "query",
        "Chronicle test",
        "--domain",
        "global",
        "--format",
        "json",
    )
    payload = json.loads(result.stdout)
    assert payload["contract_version"] == "2026-03-16.v1"
    assert payload["freshness_audit"]["domain"]["id"] == "global"
    assert payload["chronicle_hits"]
    assert payload["chronicle_hits"][0]["source_class"] == "canonical_event"
    assert "deprecated" in result.stderr.casefold()


def test_chronicle_cli_activate_startup_and_query_use_native_service(chronicle_sandbox, loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Chronicle CLI native contract test.",
            "why": "Need direct CLI coverage for activate/startup/query.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.native_cli",
    )

    activation = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "activate",
        "--domain",
        "global",
        "--format",
        "bundle",
        "--no-capture",
    )
    startup = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "startup",
        "--domain",
        "global",
        "--format",
        "json",
    )
    query = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "query",
        "Chronicle CLI native contract",
        "--domain",
        "global",
        "--format",
        "json",
    )

    activation_payload = json.loads(activation.stdout)
    startup_payload = json.loads(startup.stdout)
    query_payload = json.loads(query.stdout)
    assert activation_payload["attach_bundle"]["domain"]["id"] == "global"
    assert startup_payload["domain"]["id"] == "global"
    assert query_payload["chronicle_hits"]
    assert query_payload["chronicle_hits"][0]["source_class"] == "canonical_event"


def test_chronicle_cli_query_handles_domain_like_tokens(chronicle_sandbox, loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "milestone",
            "project": "rzmrn-portfolio",
            "text": "Portfolio shipped to production on rzmrn.com.",
            "why": "FTS query should not crash on domain-like tokens with dots.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.fts_domain_tokens",
    )

    result = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "query",
        "portfolio shipped rzmrn.com",
        "--domain",
        "global",
        "--format",
        "json",
    )
    payload = json.loads(result.stdout)
    assert payload["chronicle_hits"]
    assert any("rzmrn.com" in (item.get("text") or "") for item in payload["chronicle_hits"])


def test_build_startup_bundle_uses_service_contract(loaded_manifest) -> None:
    bundle = build_startup_bundle(loaded_manifest, domain_id="global", agent="pytest", capture=False, limit=3)
    assert bundle["contract_name"] == "max-chronicle"
    assert bundle["contract_version"] == "2026-03-16.v1"
    assert bundle["startup_bundle_schema_version"] == "2026-03-29.v1"
    assert bundle["domain"]["id"] == "global"
    assert bundle["source_health"]["status"] is not None
    assert bundle["sources"]
    assert "recent_events" in bundle
    assert "attach_bundle" not in bundle


def test_mcp_readonly_profile_exposes_only_read_surface() -> None:
    async def collect() -> tuple[list[str], list[str]]:
        server = build_server(profile="readonly")
        tools = await server.list_tools()
        resources = await server.list_resources()
        return [item.name for item in tools], [str(item.uri) for item in resources]

    tools, resources = asyncio.run(collect())

    assert tools == [
        "startup_bundle",
        "recent_events",
        "state_at",
        "query_context",
        "sources_audit",
    ]
    assert "chronicle://attach/current" in resources
    assert "chronicle://sources/audit" in resources


def test_mcp_chronicler_profile_exposes_write_surface() -> None:
    async def collect() -> list[str]:
        server = build_server(profile="chronicler")
        tools = await server.list_tools()
        return [item.name for item in tools]

    tools = asyncio.run(collect())

    assert "record_event" in tools
    assert "capture_snapshot" in tools
    assert "run_lenses" in tools
    assert "record_scenario" in tools


def test_mcp_chronicler_mutating_tool_descriptions_include_startup_guidance(chronicle_sandbox) -> None:
    async def collect() -> dict[str, str]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        tools = await server.list_tools()
        return {item.name: item.description or "" for item in tools}

    descriptions = asyncio.run(collect())

    for tool_name in (
        "record_event",
        "capture_snapshot",
        "normalize_entities",
        "build_situation_model",
        "run_lenses",
        "record_scenario",
        "review_scenario",
        "render_projections",
    ):
        assert "startup_bundle" in descriptions[tool_name]
        assert "activate_agent" in descriptions[tool_name]


def test_mcp_tool_input_schemas_include_param_descriptions_and_query_mode_enum(chronicle_sandbox) -> None:
    async def collect() -> dict[str, dict[str, Any]]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        tools = await server.list_tools()
        return {item.name: item.inputSchema for item in tools}

    schemas = asyncio.run(collect())

    startup_props = schemas["startup_bundle"]["properties"]
    assert startup_props["domain"]["description"] == "Chronicle domain id from the manifest."
    assert startup_props["capture"]["description"] == "Capture a fresh runtime snapshot before building the bundle."
    assert startup_props["compact"]["description"] == "Return the compact startup bundle variant."

    query_props = schemas["query_context"]["properties"]
    assert query_props["query"]["description"] == "Search string to match across Chronicle, status sources, and Mem0."
    assert query_props["mode"]["description"] == (
        "Retrieval mode that controls whether derived layers and scenario hits are included."
    )
    assert query_props["mode"]["enum"] == [
        "truth_only",
        "truth_plus_interpretation",
        "truth_plus_interpretation_plus_scenarios",
    ]

    record_props = schemas["record_event"]["properties"]
    assert "decision" in record_props["category"]["description"]
    assert record_props["source_files"]["description"] == "Optional source file paths to archive with the event."

    review_props = schemas["review_scenario"]["properties"]
    assert review_props["status"]["enum"] == ["matched", "partial", "missed"]
    assert "matched, partial, missed" in review_props["status"]["description"]


def test_startup_gate_key_fails_closed_without_session_context() -> None:
    class NoSessionContext:
        session = None

    assert _startup_gate_key(None) is None
    assert _startup_gate_key(NoSessionContext()) is None


def test_mcp_chronicler_record_event_auto_starts_once_per_session(
    chronicle_sandbox,
    loaded_manifest,
    monkeypatch,
) -> None:
    startup_calls: list[dict[str, Any]] = []
    original_build_startup_bundle = mcp_server_module.build_startup_bundle

    def tracking_build_startup_bundle(*args, **kwargs):
        startup_calls.append(dict(kwargs))
        return original_build_startup_bundle(*args, **kwargs)

    monkeypatch.setattr(mcp_server_module, "build_startup_bundle", tracking_build_startup_bundle)

    async def exercise() -> tuple[str, str]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        first = _decode_mcp_json(
            await server.call_tool(
                "record_event",
                {
                    "text": "Cold MCP write triggers auto-start.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "First mutating call should warm the session automatically.",
                    "agent": "pytest",
                    "source_files": [],
                },
            )
        )
        second = _decode_mcp_json(
            await server.call_tool(
                "record_event",
                {
                    "text": "Warm MCP write reuses unlocked session.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Auto-start should run only once per session.",
                    "agent": "pytest",
                    "source_files": [],
                },
            )
        )
        return first["id"], second["id"]

    first_id, second_id = asyncio.run(exercise())

    assert len(startup_calls) == 1
    assert startup_calls[0]["domain_id"] == "global"
    assert startup_calls[0]["agent"] == "pytest"
    assert startup_calls[0]["capture"] is False
    assert startup_calls[0]["limit"] == 3
    assert startup_calls[0]["compact"] is True

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        first_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (first_id,)).fetchone()[0]
        second_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (second_id,)).fetchone()[0]
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert first_count == 1
    assert second_count == 1
    assert snapshot_count == 0


def test_mcp_chronicler_capture_snapshot_auto_starts_without_extra_snapshot(
    chronicle_sandbox,
    loaded_manifest,
    monkeypatch,
) -> None:
    startup_calls: list[dict[str, Any]] = []
    original_build_startup_bundle = mcp_server_module.build_startup_bundle

    def tracking_build_startup_bundle(*args, **kwargs):
        startup_calls.append(dict(kwargs))
        return original_build_startup_bundle(*args, **kwargs)

    monkeypatch.setattr(mcp_server_module, "build_startup_bundle", tracking_build_startup_bundle)

    async def exercise() -> str:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        snapshot = _decode_mcp_json(
            await server.call_tool(
                "capture_snapshot",
                {
                    "domain": "global",
                    "agent": "pytest",
                    "title": "Auto-start snapshot",
                    "focus": "tests",
                },
            )
        )
        return snapshot["id"]

    snapshot_id = asyncio.run(exercise())

    assert len(startup_calls) == 1
    assert startup_calls[0]["domain_id"] == "global"
    assert startup_calls[0]["agent"] == "pytest"
    assert startup_calls[0]["capture"] is False
    assert startup_calls[0]["compact"] is True

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots WHERE id = ?", (snapshot_id,)).fetchone()[0]
        total_snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert snapshot_count == 1
    assert total_snapshot_count == 1


def test_mcp_chronicler_explicit_startup_bundle_avoids_duplicate_auto_start(
    chronicle_sandbox,
    loaded_manifest,
    monkeypatch,
) -> None:
    startup_calls: list[dict[str, Any]] = []
    original_build_startup_bundle = mcp_server_module.build_startup_bundle

    def tracking_build_startup_bundle(*args, **kwargs):
        startup_calls.append(dict(kwargs))
        return original_build_startup_bundle(*args, **kwargs)

    monkeypatch.setattr(mcp_server_module, "build_startup_bundle", tracking_build_startup_bundle)

    async def exercise() -> str:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        startup = _decode_mcp_json(
            await server.call_tool(
                "startup_bundle",
                {
                    "domain": "global",
                    "agent": "pytest",
                    "capture": False,
                    "limit": 2,
                },
            )
        )
        stored = _decode_mcp_json(
            await server.call_tool(
                "record_event",
                {
                    "text": "Explicit startup still unlocks mutating surface.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Explicit warmup should not trigger a second internal startup read.",
                    "agent": "pytest",
                    "source_files": [],
                },
            )
        )
        assert startup["domain"]["id"] == "global"
        return stored["id"]

    stored_id = asyncio.run(exercise())

    assert len(startup_calls) == 1
    assert startup_calls[0]["domain_id"] == "global"
    assert startup_calls[0]["agent"] == "pytest"
    assert startup_calls[0]["capture"] is False
    assert startup_calls[0]["limit"] == 2

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        event_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (stored_id,)).fetchone()[0]
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert event_count == 1
    assert snapshot_count == 0


def test_mcp_chronicler_activate_agent_unlocks_mutating_surface(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> str:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        activation = await server.call_tool(
            "activate_agent",
            {
                "domain": "global",
                "agent": "pytest",
                "focus": "tests",
                "capture": False,
            },
        )
        stored = await server.call_tool(
            "record_event",
            {
                "text": "Activated MCP write path.",
                "domain": "global",
                "category": "decision",
                "project": "status",
                "why": "activate_agent should unlock the guided startup gate.",
                "agent": "pytest",
                "source_files": [],
            },
        )
        activation_payload = _decode_mcp_json(activation)
        stored_payload = _decode_mcp_json(stored)
        assert activation_payload["contract_name"] == "max-chronicle"
        return stored_payload["id"]

    stored_id = asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        event_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (stored_id,)).fetchone()[0]

    assert event_count == 1


def test_mcp_chronicler_auto_start_is_session_scoped(chronicle_sandbox, loaded_manifest, monkeypatch) -> None:
    startup_calls: list[dict[str, Any]] = []
    original_build_startup_bundle = mcp_server_module.build_startup_bundle

    def tracking_build_startup_bundle(*args, **kwargs):
        startup_calls.append(dict(kwargs))
        return original_build_startup_bundle(*args, **kwargs)

    monkeypatch.setattr(mcp_server_module, "build_startup_bundle", tracking_build_startup_bundle)

    async def exercise() -> tuple[str, str, str]:
        first_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        first = _decode_mcp_json(
            await first_server.call_tool(
                "record_event",
                {
                    "text": "Session one first write auto-starts.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Need one auto-start per server session.",
                    "agent": "pytest-one",
                    "source_files": [],
                },
            )
        )
        second = _decode_mcp_json(
            await first_server.call_tool(
                "record_event",
                {
                    "text": "Session one second write reuses auto-start.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Warm session should not auto-start twice.",
                    "agent": "pytest-one",
                    "source_files": [],
                },
            )
        )

        second_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        third = _decode_mcp_json(
            await second_server.call_tool(
                "record_event",
                {
                    "text": "Session two gets its own auto-start.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Fresh server instance must warm itself independently.",
                    "agent": "pytest-two",
                    "source_files": [],
                },
            )
        )
        return first["id"], second["id"], third["id"]

    first_id, second_id, third_id = asyncio.run(exercise())

    assert len(startup_calls) == 2
    assert [call["agent"] for call in startup_calls] == ["pytest-one", "pytest-two"]

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        first_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (first_id,)).fetchone()[0]
        second_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (second_id,)).fetchone()[0]
        third_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (third_id,)).fetchone()[0]

    assert first_count == 1
    assert second_count == 1
    assert third_count == 1


def test_mcp_read_surfaces_remain_available_without_startup(chronicle_sandbox) -> None:
    async def exercise() -> tuple[list[dict], dict, dict]:
        chronicler = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        readonly = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="readonly")
        recent = await chronicler.call_tool("recent_events", {"domain": "global", "limit": 5})
        query = await chronicler.call_tool(
            "query_context",
            {
                "query": "Chronicle",
                "domain": "global",
                "limit": 5,
            },
        )
        startup = await readonly.call_tool(
            "startup_bundle",
            {
                "domain": "global",
                "agent": "pytest",
                "capture": False,
                "limit": 2,
            },
        )
        return _decode_mcp_json(recent), _decode_mcp_json(query), _decode_mcp_json(startup)

    recent, query, startup = asyncio.run(exercise())

    assert isinstance(recent, list)
    assert query["contract_name"] == "max-chronicle"
    assert startup["contract_name"] == "max-chronicle"


def test_sources_audit_reports_lane_metadata_and_sensitive_defaults(loaded_manifest) -> None:
    payload = build_sources_audit(loaded_manifest, domain_id="global")

    assert payload["domain"]["id"] == "global"
    assert payload["source_catalog_schema_version"] == "2026-03-21.v1"
    assert payload["lane_policy_schema_version"] == "2026-03-21.v1"
    assert payload["source_catalog"]
    assert any(item["lane"] == "life_admin" for item in payload["source_catalog"])
    assert "life_admin" in payload["coverage"]["disabled_sensitive_lanes"]
    status_source = next(item for item in payload["source_catalog"] if item["source_id"] == "status")
    assert status_source["class"] == "ssot_source"
    assert status_source["enabled"] is True
    assert status_source["questions_it_can_answer"]
    assert status_source["questions_it_cannot_answer"]
    company_intel = next(item for item in payload["source_catalog"] if item["source_id"] == "company_intel_json")
    assert company_intel["lane"] == "companies"
    assert company_intel["trust_tier"] == "reference"
    assert company_intel["owner"] == "chronicle"
    assert company_intel["questions_it_can_answer"]
    assert company_intel["trust_tier_source"] == "explicit"


def test_query_context_truth_only_filters_derived_layers(loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Decision substrate query mode test.",
            "why": "Need truth_only filtering coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.query_mode",
    )
    materialize_situation_model(loaded_manifest, domain_id="global")
    run_lenses(loaded_manifest, domain_id="global", persist=True)
    record_scenario(
        loaded_manifest,
        domain_id="global",
        scenario_name="Truth mode regression",
        assumptions=["Portfolio remains the current main gate."],
        expected_outcomes=["Derived scenario should not leak into truth-only query mode."],
    )

    payload = query_context(
        loaded_manifest,
        query="Decision substrate",
        domain="global",
        limit=5,
        mode="truth_only",
    )

    assert payload["query_mode"] == "truth_only"
    assert payload["mem0_dump_hits"] == []
    assert payload["normalized_entity_hits"] == []
    assert payload["interpretation_hits"] == []
    assert payload["scenario_hits"] == []
    assert payload["briefing_hits"] == []


def test_normalize_entities_dedupes_company_aliases_from_runtime_sources(chronicle_sandbox, loaded_manifest) -> None:
    (chronicle_sandbox.status_root / "company-intel.json").write_text(
        json.dumps(
            {
                "companies": {
                    "adobe": {"name": "Adobe"},
                },
                "lead_companies": {
                    "without_intel": ["Adobe Inc.", "Adobe, Inc."],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    openclaw_leads = [
        {"company": "Adobe"},
        {"company": "Adobe Inc."},
    ]
    (chronicle_sandbox.openclaw_root / "workspace" / "memory" / "leads.json").write_text(
        json.dumps(openclaw_leads, ensure_ascii=False),
        encoding="utf-8",
    )

    entities = materialize_normalized_entities(loaded_manifest, domain_id="global")
    adobe = [item for item in entities if item["entity_type"] == "company" and item["canonical_key"] == "adobe"]

    assert len(adobe) == 1
    assert set(alias.casefold() for alias in adobe[0]["aliases"]) >= {"adobe", "adobe inc"}


def test_normalize_entities_filters_noisy_runtime_company_aliases_and_inactivates_stale_rows(
    chronicle_sandbox,
    loaded_manifest,
) -> None:
    config = config_from_manifest(loaded_manifest)
    upsert_normalized_entity(
        config,
        entity_type="company",
        canonical_key="business",
        canonical_name="Business",
        aliases=["Business"],
        source_refs=[{"source_id": "openclaw_leads_json", "value": "Business"}],
    )
    (chronicle_sandbox.status_root / "company-intel.json").write_text(
        json.dumps(
            {
                "companies": {},
                "lead_companies": {
                    "without_intel": [
                        "Scale Army. ... AI-driven video production tools and rapid iteration strategies. Key",
                        "Armis - remotive.com",
                        "REMOTE - BMC Software",
                        "Built In Chicago.",
                    ],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (chronicle_sandbox.openclaw_root / "workspace" / "memory" / "leads.json").write_text(
        json.dumps(
            [
                {"company": "Business"},
                {"company": "Built In NYC."},
                {"company": "Collier Simon - remotive.com"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    entities = materialize_normalized_entities(loaded_manifest, domain_id="global")
    company_names = {item["canonical_name"] for item in entities if item["entity_type"] == "company"}

    assert "Business" not in company_names
    assert "Built In Chicago" not in company_names
    assert "Built In Nyc" not in company_names
    assert "Scale Army" in company_names
    assert "Armis" in company_names
    assert "BMC Software" in company_names
    assert "Collier Simon" in company_names

    all_entities = fetch_normalized_entities(config, status=None, limit=200)
    business = next(item for item in all_entities if item["entity_type"] == "company" and item["canonical_key"] == "business")
    assert business["status"] == "inactive"


def test_materialized_situation_model_and_lenses_include_evidence_refs(loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "blocker",
            "project": "portfolio",
            "text": "Portfolio has an active blocker for situation synthesis testing.",
            "why": "Need contradiction/risk coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "implementation",
            "project": "portfolio",
            "text": "Portfolio also progressed today despite the blocker.",
            "why": "Need contradiction/risk coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )

    situation = materialize_situation_model(loaded_manifest, domain_id="global")
    lens_runs = run_lenses(loaded_manifest, domain_id="global", situation_model=situation, persist=True)

    assert situation["situation_id"]
    assert situation["actors"]
    assert "contradictions" in situation
    assert len(lens_runs) == 5
    assert all(item["evidence_refs"] for item in lens_runs)


def test_situation_model_prefers_priority_targets_and_rules_over_why_bullets(
    chronicle_sandbox,
    loaded_manifest,
) -> None:
    (chronicle_sandbox.status_root / "priorities.md").write_text(
        "# Priorities\n\n"
        "## Strategic Context\n"
        "Portfolio is live.\n\n"
        "## Priority Stack (ordered)\n\n"
        "### 🔴 P0 — LinkedIn Rebuild\n"
        "- **Why:** Primary discovery channel for remote roles\n"
        "- **Target:** Rewrite profile and anchor featured section with rzmrn.com\n"
        "- **Rule:** No more broad portfolio polish before LinkedIn ships\n\n"
        "### 🟡 P1 — Cover Letter Framework\n"
        "- **Why:** Outreach quality\n"
        "- **Target:** Create 3 reusable templates\n"
        "- **Depends on:** LinkedIn narrative locked\n\n"
        "## Anti-Patterns (ADHD Guard Rails)\n"
        "- Do not disappear into comfort-zone infra work.\n\n"
        "## Key Constraints\n"
        "- LinkedIn must ship before broader application push.\n",
        encoding="utf-8",
    )

    situation = materialize_situation_model(loaded_manifest, domain_id="global")

    assert situation["goals"]
    assert situation["goals"][0].startswith("LinkedIn Rebuild")
    assert all("Why:" not in item for item in situation["goals"])
    assert any("portfolio polish" in item.casefold() for item in situation["constraints"])
    assert any("comfort-zone infra" in item.casefold() for item in situation["constraints"])


def test_situation_model_prefers_fresher_status_over_archival_priorities(
    chronicle_sandbox,
    loaded_manifest,
) -> None:
    priorities_path = chronicle_sandbox.status_root / "priorities.md"
    priorities_path.write_text(
        "# Priorities\n\n"
        "## Priority Stack (ordered)\n\n"
        "### 🔴 P0 — Portfolio Blocker\n"
        "- **Target:** Finish portfolio before LinkedIn\n"
        "- **Rule:** Do not start outreach yet\n",
        encoding="utf-8",
    )
    old_timestamp = datetime.now().timestamp() - (60 * 60 * 24 * 30)
    os.utime(priorities_path, (old_timestamp, old_timestamp))

    (chronicle_sandbox.status_root / "status.md").write_text(
        "## Active Projects\n\n"
        "### Portfolio (`~/Projects/rzmrn-portfolio/`) — V1 SHIPPED\n"
        "- **Career gate:** cleared. LinkedIn, cover letters, and outreach are now unblocked.\n\n"
        "## Priorities\n\n"
        "## Strategic Context\n"
        "Portfolio is live and no longer the blocker.\n\n"
        "## Priority Stack (ordered)\n\n"
        "### 🔴 P0 — LinkedIn Rebuild\n"
        "- **Target:** Rewrite profile and anchor featured section with rzmrn.com\n\n"
        "### 🟡 P1 — Cover Letter Framework\n"
        "- **Target:** Create 3 reusable templates\n",
        encoding="utf-8",
    )

    situation = materialize_situation_model(loaded_manifest, domain_id="global")

    assert situation["goals"]
    assert situation["goals"][0].startswith("LinkedIn Rebuild")
    assert "Portfolio Blocker" not in situation["goals"][0]
    assert situation["derived_from"]["strategy_source_ids"][0] == "status"


def test_build_activation_prompt_uses_dynamic_situation_focus_instead_of_hardcoded_gate(
    chronicle_sandbox,
    loaded_manifest,
) -> None:
    (chronicle_sandbox.status_root / "priorities.md").write_text(
        "# Priorities\n\n"
        "## Priority Stack (ordered)\n\n"
        "### 🔴 P0 — LinkedIn Rebuild\n"
        "- **Target:** Rewrite profile and anchor featured section with rzmrn.com\n",
        encoding="utf-8",
    )

    activation = build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Dynamic activation prompt",
        focus="tests",
        capture=True,
    )

    assert "Current focus from Chronicle:" in activation["prompt"]
    assert "Top priority: LinkedIn Rebuild: Rewrite profile and anchor featured section with rzmrn.com" in activation["prompt"]
    assert "Portfolio v1.0 is the main gate" not in activation["prompt"]


def test_build_startup_bundle_marks_source_freshness_per_source(
    chronicle_sandbox,
    loaded_manifest,
) -> None:
    priorities_path = chronicle_sandbox.status_root / "priorities.md"
    priorities_path.write_text("# Priorities\n\nArchive.\n", encoding="utf-8")
    old_timestamp = datetime.now().timestamp() - (60 * 60 * 24 * 30)
    os.utime(priorities_path, (old_timestamp, old_timestamp))

    bundle = build_startup_bundle(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        capture=False,
        limit=3,
        compact=False,
    )
    sources = {item["id"]: item for item in bundle["sources"]}

    assert sources["status"]["freshness_status"] in {"live", "recent"}
    assert sources["priorities"]["freshness_status"] == "archival"


def test_record_and_review_scenario_validate_inputs_and_link_outcome(loaded_manifest) -> None:
    outcome = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "milestone",
            "project": "status",
            "text": "Outcome event for scenario review.",
            "why": "Need replay/eval coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )

    with pytest.raises(ValueError, match="assumptions are required"):
        record_scenario(
            loaded_manifest,
            domain_id="global",
            scenario_name="Invalid scenario",
            assumptions=[],
        )

    scenario = record_scenario(
        loaded_manifest,
        domain_id="global",
        scenario_name="Scenario replay test",
        assumptions=["The current priority stack remains stable for the next iteration."],
        expected_outcomes=["System should keep returning a coherent current-state model."],
        failure_modes=["Source freshness degrades and weakens confidence."],
    )
    review = review_scenario(
        loaded_manifest,
        scenario_id=scenario["scenario_id"],
        status="matched",
        review_summary="Observed outcome matched the expected coherent state.",
        outcome_event_id=outcome["id"],
    )

    assert scenario["assumptions"]
    assert review["scenario_id"] == scenario["scenario_id"]
    assert review["outcome_event_id"] == outcome["id"]


def test_query_context_memory_domain_uses_only_canonical_memory_sources(loaded_manifest) -> None:
    payload = query_context(loaded_manifest, query="Chronicle", domain="memory", limit=10)
    source_ids = {item["source_id"] for item in payload["status_hits"]}
    assert source_ids <= {"memory_system", "chronicle_protocol", "chronicle_adr", "chronicle_schema", "chronicle_checklist", "status"}
    assert "codex_protocol" not in source_ids
    assert "handoff" not in source_ids
    assert "ecosystem" not in source_ids


def test_search_mem0_dump_dedupes_unified_collection_duplicates(tmp_path) -> None:
    dump_path = tmp_path / "mem0-dump.json"
    dump_path.write_text(
        json.dumps(
            {
                "snapshot_type": "unified",
                "memories": [
                    {
                        "id": "personal-1",
                        "memory": "Portfolio V1 shipped to production on rzmrn.com",
                        "metadata": {"project": "portfolio", "category": "milestone"},
                        "source_collection": "personal",
                        "source_collection_name": "napaarnik_personal",
                    },
                    {
                        "id": "digest-1",
                        "memory": "Portfolio V1 shipped to production on rzmrn.com",
                        "metadata": {"project": "portfolio", "category": "milestone"},
                        "source_collection": "digest",
                        "source_collection_name": "napaarnik_memory",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    hits = search_mem0_dump(dump_path, "portfolio shipped production", limit=5)
    assert len(hits) == 1
    assert hits[0]["duplicate_count"] == 2
    assert set(hits[0]["source_collections"]) == {"personal", "digest"}
    assert set(hits[0]["source_collection_names"]) == {"napaarnik_personal", "napaarnik_memory"}


def test_freshness_audit_surfaces_stale_inputs_and_prompt_warnings(chronicle_sandbox, loaded_manifest) -> None:
    status_path = Path(loaded_manifest["source_map"]["status"]["path"])
    mem0_dump_path = Path(loaded_manifest["paths"]["mem0_dump"])
    status_path.unlink()
    stale_ts = datetime(2026, 3, 1, 12, 0, 0).timestamp()
    os.utime(mem0_dump_path, (stale_ts, stale_ts))

    activation = build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Freshness warning test",
        focus="tests",
        capture=False,
    )

    audit = activation["attach_bundle"]["freshness_audit"]
    issue_kinds = {item["kind"] for item in audit["issues"]}
    assert audit["status"] == "critical"
    assert "attach_source_missing" in issue_kinds
    assert "mem0_dump_stale" in issue_kinds
    assert "Freshness warnings:" in activation["prompt"]
    assert "Attach source `Status` is missing." in activation["prompt"]


def test_freshness_audit_respects_runtime_evidence_policy_overrides(loaded_manifest) -> None:
    portfolio_manifest_path = Path(loaded_manifest["paths"]["portfolio_asset_manifest"])
    recent_ts = datetime.now().timestamp() - (6 * 24 * 3600)
    os.utime(portfolio_manifest_path, (recent_ts, recent_ts))

    activation = build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Freshness policy override test",
        focus="tests",
        capture=False,
    )

    audit = activation["attach_bundle"]["freshness_audit"]
    runtime_row = next(item for item in audit["runtime_evidence"] if item["id"] == "portfolio_asset_manifest")
    issue_sources = {item["source_id"] for item in audit["issues"]}
    assert runtime_row["freshness_status"] == "recent"
    assert runtime_row["freshness_thresholds_hours"]["recent"] == 168
    assert "portfolio_asset_manifest" not in issue_sources


def test_record_event_rejects_unknown_category(loaded_manifest) -> None:
    with pytest.raises(ValueError, match="Unsupported category"):
        record_event(
            loaded_manifest,
            {
                "agent": "pytest",
                "domain": "global",
                "category": "totally_new_bucket",
                "project": "status",
                "text": "Should not be accepted",
                "why": "Governance must reject unknown categories.",
                "source_files": [],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
            },
        )


@pytest.mark.parametrize(
    ("category", "expected_event_mem0_status", "expected_outbox_status"),
    [
        ("decision", "queued", "pending"),
        ("git_commit", "skipped", "skipped"),
    ],
)
def test_chronicle_mcp_record_event_uses_mem0_policy(
    chronicle_sandbox,
    loaded_manifest,
    category,
    expected_event_mem0_status,
    expected_outbox_status,
) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": category,
            "project": "status",
            "text": f"MCP policy test for {category}",
            "why": "Need deterministic default Mem0 policy coverage.",
            "source_files": [],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.mcp_policy",
    )

    assert stored["mem0_status"] == expected_event_mem0_status

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        outbox_row = connection.execute(
            "SELECT status FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    assert outbox_row["status"] == expected_outbox_status


def test_default_mem0_status_prefers_explicit_override_and_mcp_noise_filter() -> None:
    assert default_mem0_status({"category": "decision"}, source_kind="chronicle_mcp") == "queued"
    assert default_mem0_status({"category": "git_commit"}, source_kind="chronicle_mcp") == "off"
    assert default_mem0_status({"category": "decision", "mem0_status": "off"}, source_kind="chronicle_mcp") == "off"
    assert default_mem0_status({"category": "decision"}, source_kind="chronicle_cli") is None


def test_chronicle_mcp_record_event_local_only_guard_hides_noise_from_attach_and_query(chronicle_sandbox, loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "state_change",
            "project": "guard-shadow",
            "text": "## Protocol Update\nOld flow: max-chronicle activate.\nNew flow: chronicle-mcp-chronicler only.\nDO NOT use the old shell path.",
            "why": None,
            "source_files": [],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.local_only_guard",
    )

    assert stored["memory_guard"]["verdict"] == "local_only"
    assert stored["memory_guard"]["visibility"] == "raw_only"
    assert {signal["name"] for signal in stored["memory_guard"]["signals"]} >= {
        "meta_protocol_instruction",
        "negative_mirror_instruction",
        "markdown_prompt_chatter",
    }
    assert stored["mem0_status"] == "skipped"

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        outbox_row = connection.execute(
            "SELECT status, payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    outbox_payload = json.loads(outbox_row["payload_json"])
    assert outbox_row["status"] == "skipped"
    assert outbox_payload["memory_guard"]["verdict"] == "local_only"

    recent = _chronicle_cli(chronicle_sandbox.manifest_path, "recent", "--format", "json")
    recent_payload = json.loads(recent.stdout)
    assert any(item["id"] == stored["id"] for item in recent_payload)

    startup = build_startup_bundle(loaded_manifest, domain_id="global", agent="pytest", capture=False, limit=10)
    assert all(item["id"] != stored["id"] for item in startup["recent_events"])

    query = query_context(
        loaded_manifest,
        query="chronicle-mcp-chronicler only",
        domain="global",
        limit=10,
        mode="truth_plus_interpretation",
    )
    assert all(item["id"] != stored["id"] for item in query["chronicle_hits"])

    entities = materialize_normalized_entities(loaded_manifest, domain_id="global")
    assert all(item["canonical_key"] != "guard-shadow" for item in entities)

    situation = materialize_situation_model(loaded_manifest, domain_id="global")
    transitions = situation.get("trajectory", {}).get("recent_transitions", [])
    assert all("guard-shadow" not in transition for transition in transitions)

    timeline = reconstruct_timeline(
        loaded_manifest,
        timestamp=datetime.fromisoformat(stored["recorded_at"].replace("Z", "+00:00")),
        domain="global",
        window_hours=1,
        limit=3,
    )
    assert any(item["id"] == stored["id"] for item in timeline["events"])


def test_chronicle_mcp_record_event_keeps_high_signal_decision_durable(chronicle_sandbox, loaded_manifest) -> None:
    evidence_path = chronicle_sandbox.status_root / "evidence.txt"
    evidence_path.write_text("Guardrail evidence.\n", encoding="utf-8")

    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Adopt a soft Chronicle memory guardrail for noisy MCP protocol chatter.",
            "why": "Preserve append-only truth while keeping low-signal protocol chatter out of derived recall.",
            "source_files": [str(evidence_path)],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.high_signal_guard",
    )

    assert stored["memory_guard"]["verdict"] == "durable"
    assert stored["memory_guard"]["visibility"] == "default"
    assert stored["mem0_status"] == "queued"

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        outbox_row = connection.execute(
            "SELECT status FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    assert outbox_row["status"] == "pending"


def test_recent_near_duplicate_signal_does_not_force_local_only_without_other_risks(loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "note",
            "project": "status",
            "text": "Keep recruiter intro concise and concrete.",
            "why": None,
            "source_files": [],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.duplicate_seed",
    )

    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "note",
            "project": "status",
            "text": "Keep recruiter intro concise concrete and focused.",
            "why": None,
            "source_files": [],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.duplicate_probe",
    )

    signal_names = {signal["name"] for signal in stored["memory_guard"]["signals"]}
    assert "recent_near_duplicate" in signal_names
    assert stored["memory_guard"]["verdict"] == "durable"


def test_chronicle_cli_record_uses_governance_and_dedupe(chronicle_sandbox) -> None:
    first = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "record",
        "CLI dedupe fact",
        "--domain",
        "global",
        "--project",
        "status",
        "--why",
        "CLI should not create exact duplicates.",
        "--mem0-status",
        "off",
    )
    second = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "record",
        "CLI dedupe fact",
        "--domain",
        "global",
        "--project",
        "status",
        "--why",
        "CLI should not create exact duplicates.",
        "--mem0-status",
        "off",
    )

    first_payload = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    assert first_payload["category"] == "note"
    assert first_payload["chronicle_status"] == "stored"
    assert second_payload["chronicle_status"] == "existing"
    assert second_payload["dedupe_status"] == "exact_duplicate"
    assert second_payload["id"] == first_payload["id"]

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE text = ?",
            ("CLI dedupe fact",),
        ).fetchone()[0]
    assert count == 1


def test_chronicle_cli_guard_event_marks_existing_event_local_only(chronicle_sandbox) -> None:
    created = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "record",
        "Manual guard target",
        "--domain",
        "global",
        "--project",
        "status",
        "--why",
        "Need deterministic manual guard coverage.",
    )
    event_id = json.loads(created.stdout)["id"]

    dry_run = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "guard-event",
        "--event-id",
        event_id,
        "--verdict",
        "local_only",
        "--reason",
        "Operator review: protocol chatter",
    )
    apply_run = _chronicle_cli(
        chronicle_sandbox.manifest_path,
        "guard-event",
        "--event-id",
        event_id,
        "--verdict",
        "local_only",
        "--reason",
        "Operator review: protocol chatter",
        "--apply",
    )
    dry_payload = json.loads(dry_run.stdout)
    apply_payload = json.loads(apply_run.stdout)
    assert dry_payload["status"] == "dry_run"
    assert apply_payload["status"] == "ok"
    assert apply_payload["memory_guard"]["source"] == "manual_operator_guard"

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        event_row = connection.execute(
            "SELECT mem0_status, payload_json FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT status, payload_json FROM mem0_outbox WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    event_payload = json.loads(event_row["payload_json"])
    outbox_payload = json.loads(outbox_row["payload_json"])
    assert event_row["mem0_status"] == "skipped"
    assert outbox_row["status"] == "skipped"
    assert event_payload["memory_guard"]["verdict"] == "local_only"
    assert outbox_payload["memory_guard"]["source"] == "manual_operator_guard"


def test_render_projections_marks_generated_status_as_canonical(chronicle_sandbox, loaded_manifest) -> None:
    status_path = chronicle_sandbox.status_root / "status.md"
    status_path.write_text(
        "# Status\n> Last updated: 2026-03-05\n\n## Notes\n- Legacy summary\n\n<!-- BEGIN GENERATED:CHRONICLE_STATUS -->\nplaceholder\n<!-- END GENERATED:CHRONICLE_STATUS -->\n",
        encoding="utf-8",
    )

    build_activation(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Projection canonical note",
        focus="tests",
        capture=True,
    )

    content = status_path.read_text(encoding="utf-8")
    assert "> Human summary last reviewed: 2026-03-05" in content
    assert "> Chronicle generated view below is the canonical live status signal." in content
    assert "## Chronicle Generated View" in content


def test_chronicle_cli_repair_categories_normalizes_legacy_null_category(chronicle_sandbox, loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "project": "status",
            "text": "Legacy null category repair target",
            "why": "Need a deterministic repair command regression test.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection, connection:
        event_row = connection.execute(
            "SELECT payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
        event_payload = json.loads(event_row["payload_json"])
        outbox_payload = json.loads(outbox_row["payload_json"])
        event_payload.pop("category", None)
        outbox_payload.pop("category", None)
        connection.execute(
            "UPDATE events SET category = '', payload_json = ? WHERE id = ?",
            (json.dumps(event_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )
        connection.execute(
            "UPDATE mem0_outbox SET payload_json = ? WHERE event_id = ?",
            (json.dumps(outbox_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )

    dry_run = _chronicle_cli(chronicle_sandbox.manifest_path, "repair-categories", "--dry-run")
    apply_run = _chronicle_cli(chronicle_sandbox.manifest_path, "repair-categories")
    dry_payload = json.loads(dry_run.stdout)
    apply_payload = json.loads(apply_run.stdout)
    assert dry_payload["status"] == "dry_run"
    assert dry_payload["repaired_count"] == 1
    assert apply_payload["status"] == "ok"
    assert apply_payload["repaired_count"] == 1

    with open_connection(config) as connection:
        repaired_row = connection.execute(
            "SELECT category, payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        repaired_outbox = connection.execute(
            "SELECT payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    repaired_payload = json.loads(repaired_row["payload_json"])
    repaired_outbox_payload = json.loads(repaired_outbox["payload_json"])
    assert repaired_row["category"] == "note"
    assert repaired_payload["category"] == "note"
    assert repaired_outbox_payload["category"] == "note"


def test_chronicle_cli_repair_mem0_state_reconciles_events_from_outbox(chronicle_sandbox, loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "note",
            "project": "status",
            "text": "Mem0 drift repair target",
            "why": "Need deterministic mem0 reconciliation coverage.",
            "source_files": [],
            "mem0_status": "queued",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection, connection:
        event_row = connection.execute(
            "SELECT payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
        event_payload = json.loads(event_row["payload_json"])
        outbox_payload = json.loads(outbox_row["payload_json"])
        event_payload["mem0_status"] = "queued"
        event_payload["mem0_error"] = "stale"
        outbox_payload["mem0_raw"] = "stored raw"
        outbox_payload["mem0_synced_at"] = "2026-03-16T10:00:00Z"
        connection.execute(
            "UPDATE events SET mem0_status = 'queued', mem0_error = 'stale', payload_json = ? WHERE id = ?",
            (json.dumps(event_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )
        connection.execute(
            "UPDATE mem0_outbox SET status = 'synced', last_error = NULL, synced_at_utc = '2026-03-16T10:00:00Z', payload_json = ? WHERE event_id = ?",
            (json.dumps(outbox_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )

    dry_run = _chronicle_cli(chronicle_sandbox.manifest_path, "repair-mem0-state", "--dry-run")
    apply_run = _chronicle_cli(chronicle_sandbox.manifest_path, "repair-mem0-state", "--apply")
    dry_payload = json.loads(dry_run.stdout)
    apply_payload = json.loads(apply_run.stdout)
    assert dry_payload["status"] == "dry_run"
    assert dry_payload["repaired_count"] == 1
    assert apply_payload["status"] == "ok"
    assert apply_payload["repaired_count"] == 1

    with open_connection(config) as connection:
        repaired_row = connection.execute(
            "SELECT mem0_status, mem0_error, payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
    repaired_payload = json.loads(repaired_row["payload_json"])
    assert repaired_row["mem0_status"] == "stored"
    assert repaired_row["mem0_error"] is None
    assert repaired_payload["mem0_status"] == "stored"
    assert repaired_payload["mem0_raw"] == "stored raw"
    assert repaired_payload["mem0_synced_at"] == "2026-03-16T10:00:00Z"


def test_backfill_mem0_queue_requeues_eligible_mcp_events(chronicle_sandbox, loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "career",
            "category": "milestone",
            "project": "rzmrn-portfolio",
            "text": "Legacy MCP milestone that should have reached Mem0.",
            "why": "Simulate historical hardcoded mem0_status=off rows before policy fix.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.backfill_mem0",
    )

    dry_run = backfill_mem0_queue(loaded_manifest, dry_run=True)
    apply_run = _chronicle_cli(chronicle_sandbox.manifest_path, "backfill-mem0-queue", "--apply")
    apply_payload = json.loads(apply_run.stdout)
    assert dry_run["status"] == "dry_run"
    assert dry_run["requeued_count"] == 1
    assert apply_payload["status"] == "ok"
    assert apply_payload["requeued_count"] == 1

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        event_row = connection.execute(
            "SELECT mem0_status, payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT status, attempts, last_error, synced_at_utc FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    event_payload = json.loads(event_row["payload_json"])
    assert event_row["mem0_status"] == "queued"
    assert event_payload["mem0_status"] == "queued"
    assert outbox_row["status"] == "pending"
    assert outbox_row["attempts"] == 0
    assert outbox_row["last_error"] is None
    assert outbox_row["synced_at_utc"] is None


def test_backfill_mem0_queue_skips_local_only_guarded_events(chronicle_sandbox, loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "state_change",
            "project": "status",
            "text": "Noisy MCP protocol chatter",
            "why": "Create a skipped MCP event that should stay local_only and never be requeued.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.backfill_mem0.local_only",
    )

    guard_payload = guard_event(
        loaded_manifest,
        event_id=stored["id"],
        verdict="local_only",
        reason="Operator review: noisy protocol chatter.",
        apply=True,
    )
    assert guard_payload["status"] == "ok"

    dry_run = backfill_mem0_queue(loaded_manifest, dry_run=True)
    apply_run = _chronicle_cli(chronicle_sandbox.manifest_path, "backfill-mem0-queue", "--apply")
    apply_payload = json.loads(apply_run.stdout)
    assert dry_run["status"] == "dry_run"
    assert dry_run["requeued_count"] == 0
    assert apply_payload["status"] == "ok"
    assert apply_payload["requeued_count"] == 0

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        event_row = connection.execute(
            "SELECT mem0_status, payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT status, payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    event_payload = json.loads(event_row["payload_json"])
    outbox_payload = json.loads(outbox_row["payload_json"])
    assert event_row["mem0_status"] == "skipped"
    assert outbox_row["status"] == "skipped"
    assert event_payload["memory_guard"]["verdict"] == "local_only"
    assert outbox_payload["memory_guard"]["verdict"] == "local_only"


def test_reconstruct_timeline_filters_by_domain(loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Global timeline event",
            "why": "Need domain-specific timeline coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
            "recorded_at": "2026-03-15T12:00:00Z",
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.timeline.global",
    )
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "memory",
            "category": "decision",
            "project": "status",
            "text": "Memory timeline event",
            "why": "Need domain-specific timeline coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
            "recorded_at": "2026-03-15T12:00:00Z",
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.timeline.memory",
    )

    config = config_from_manifest(loaded_manifest)
    store_snapshot(
        config,
        {
            "id": "global-snapshot",
            "captured_at_utc": "2026-03-15T12:00:00Z",
            "captured_at_local": "2026-03-15T13:00:00+01:00",
            "timezone": "Europe/Warsaw",
            "agent": "pytest",
            "domain": "global",
            "title": "Global snapshot",
            "label": "Whole System",
            "recent_ledger": [],
            "source_excerpts": [],
            "repos": [],
        },
    )
    store_snapshot(
        config,
        {
            "id": "memory-snapshot",
            "captured_at_utc": "2026-03-15T12:00:00Z",
            "captured_at_local": "2026-03-15T13:00:00+01:00",
            "timezone": "Europe/Warsaw",
            "agent": "pytest",
            "domain": "memory",
            "title": "Memory snapshot",
            "label": "Memory",
            "recent_ledger": [],
            "source_excerpts": [],
            "repos": [],
        },
    )

    payload = reconstruct_timeline(
        loaded_manifest,
        timestamp=datetime.fromisoformat("2026-03-15T12:00:00+00:00"),
        domain="global",
        window_hours=1,
        limit=5,
    )
    assert payload["contract_version"] == "2026-03-16.v1"
    assert payload["domain"] == "global"
    assert all(item.get("domain") == "global" for item in payload["nearest_snapshots"])
    assert payload["events"]
    assert all(item.get("domain") == "global" for item in payload["events"])


def test_ssot_hub_startup_and_timeline_use_service_paths(chronicle_sandbox, loaded_manifest) -> None:
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "memory",
            "category": "decision",
            "project": "status",
            "text": "Memory domain startup timeline event",
            "why": "Need ssot_hub startup/timeline service coverage.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
            "recorded_at": "2026-03-15T09:00:00Z",
        },
        append_compat=True,
        source_kind="pytest",
        imported_from="tests.test_agent_contract.ssot_hub.memory",
    )

    startup = _ssot_hub(
        chronicle_sandbox.manifest_path,
        "startup",
        "--domain",
        "memory",
        "--format",
        "json",
    )
    startup_payload = json.loads(startup.stdout)
    assert startup_payload["contract_version"] == "2026-03-16.v1"
    assert startup_payload["domain"]["id"] == "memory"
    assert startup_payload["source_health"]["status"] is not None
    assert "deprecated" in startup.stderr.casefold()

    timeline = _ssot_hub(
        chronicle_sandbox.manifest_path,
        "timeline",
        "--at",
        "2026-03-15T10:00:00+01:00",
        "--domain",
        "memory",
        "--format",
        "json",
    )
    timeline_payload = json.loads(timeline.stdout)
    assert timeline_payload["contract_version"] == "2026-03-16.v1"
    assert timeline_payload["domain"] == "memory"
    assert "deprecated" in timeline.stderr.casefold()
