from __future__ import annotations

import asyncio
from datetime import datetime
import json
import os
import time
from pathlib import Path
import subprocess
import sys
from typing import Any

import pytest

from mcp.server.fastmcp.exceptions import ToolError
from max_chronicle.mcp_server import _gate_session, build_server
from max_chronicle.runtime_context import load_manifest, search_mem0_dump
from max_chronicle.service import (
    backfill_mem0_queue,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    default_mem0_status,
    guard_event,
    materialize_normalized_entities,
    query_context,
    record_event,
    reconstruct_timeline,
)
from max_chronicle.store import (
    config_from_manifest,
    fetch_normalized_entities,
    open_connection,
    store_snapshot,
    upsert_normalized_entity,
)


PROJECT_STATUS_ROOT = Path(__file__).resolve().parents[1]


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


def _decode_error_envelope(error: BaseException) -> dict[str, Any]:
    """Tool failures raise, so isError is set; the envelope rides in the message."""
    text = str(error)
    return json.loads(text[text.index("{"):])


def _assert_startup_required(error: BaseException, tool_name: str, *, domain: str = "global") -> None:
    payload = _decode_error_envelope(error)
    assert payload["status"] == "error"
    assert payload["error_type"] == "startup_required"
    assert payload["retryable"] is True
    assert isinstance(payload["server_uptime_s"], int)
    message = payload["error"]
    assert "startup_required:" in message
    assert f"`{tool_name}`" in message
    assert f'startup_bundle(domain="{domain}")' in message
    assert f'activate_agent(domain="{domain}")' in message
    assert "No Chronicle mutation was performed." in message


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
            "project": "demo-portfolio",
            "text": "Portfolio shipped to production on example.com.",
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
        "portfolio shipped example.com",
        "--domain",
        "global",
        "--format",
        "json",
    )
    payload = json.loads(result.stdout)
    assert payload["chronicle_hits"]
    assert any("example.com" in (item.get("text") or "") for item in payload["chronicle_hits"])


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
        "query_memory",
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
    assert "entity_admin" in tools
    # Consolidated away in the 15->10 tool-surface simplification:
    assert "normalize_entities" not in tools
    assert "render_projections" not in tools
    assert "add_entity_alias" not in tools
    assert "merge_entities" not in tools
    assert "entity_resolution_report" not in tools


def test_mcp_chronicler_mutating_tool_descriptions_include_startup_guidance(chronicle_sandbox) -> None:
    async def collect() -> dict[str, str]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        tools = await server.list_tools()
        return {item.name: item.description or "" for item in tools}

    descriptions = asyncio.run(collect())

    for tool_name in (
        "record_event",
        "capture_snapshot",
        "entity_admin",
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
    assert query_props["query"]["description"] == (
        "Search string matched across Chronicle events, status markdown sections and the Mem0 dump."
    )
    assert query_props["mode"]["description"].startswith("truth_only: Chronicle events and status sources only.")
    assert query_props["mode"]["enum"] == [
        "truth_only",
        "truth_plus_interpretation",
        "truth_plus_interpretation_plus_scenarios",
    ]

    record_props = schemas["record_event"]["properties"]
    assert "decision" in record_props["category"]["description"]
    assert record_props["source_files"]["description"] == "Optional source file paths to archive with the event."


def test_startup_gate_session_fails_closed_without_session_context() -> None:
    class NoSessionContext:
        session = None

    class RaisingContext:
        @property
        def session(self):
            raise ValueError("Context is not available outside of a request")

    assert _gate_session(None) is None
    assert _gate_session(NoSessionContext()) is None
    assert _gate_session(RaisingContext()) is None


def test_mcp_chronicler_record_event_requires_startup_until_unlocked(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> str:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        arguments = {
            "text": "Blocked MCP write before startup.",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "why": "Gate should redirect before mutating Chronicle.",
            "agent": "pytest",
            "source_files": [],
        }

        with pytest.raises(ToolError) as first_error:
            await server.call_tool("record_event", arguments)
        with pytest.raises(ToolError) as second_error:
            await server.call_tool("record_event", arguments)

        _assert_startup_required(first_error.value, "record_event")
        _assert_startup_required(second_error.value, "record_event")

        startup = await server.call_tool(
            "startup_bundle",
            {
                "domain": "global",
                "agent": "pytest",
                "capture": False,
                "limit": 2,
            },
        )
        stored = _decode_mcp_json(await server.call_tool("record_event", arguments))
        startup_payload = _decode_mcp_json(startup)
        assert startup_payload["domain"]["id"] == "global"
        return stored["id"]

    stored_id = asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        event_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE text = ?",
            ("Blocked MCP write before startup.",),
        ).fetchone()[0]
        outbox_count = connection.execute("SELECT COUNT(*) FROM mem0_outbox WHERE event_id = ?", (stored_id,)).fetchone()[0]
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert event_count == 1
    assert outbox_count == 1
    assert snapshot_count == 0


def test_mcp_chronicler_capture_snapshot_requires_startup_without_side_effects(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> None:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError) as error:
            await server.call_tool(
                "capture_snapshot",
                {
                    "domain": "global",
                    "agent": "pytest",
                    "title": "Blocked snapshot",
                    "focus": "tests",
                },
            )
        _assert_startup_required(error.value, "capture_snapshot")

    asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        event_count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert snapshot_count == 0
    assert event_count == 0


def test_mcp_chronicler_entity_admin_mutations_require_startup_but_report_is_free(
    chronicle_sandbox, loaded_manifest
) -> None:
    async def exercise() -> Any:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError) as blocked:
            await server.call_tool("entity_admin", {"action": "normalize", "domain": "global"})
        _assert_startup_required(blocked.value, "entity_admin")
        # report is read-only QA and must work without the gate
        report = _decode_mcp_json(await server.call_tool("entity_admin", {"action": "report"}))
        assert report["action"] == "report"
        return report

    asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        normalized_count = connection.execute("SELECT COUNT(*) FROM normalized_entities").fetchone()[0]
    assert normalized_count == 0


def test_mcp_chronicler_entity_admin_validates_action_params(chronicle_sandbox) -> None:
    async def exercise() -> tuple[Any, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await server.call_tool(
            "startup_bundle",
            {"domain": "global", "agent": "pytest", "capture": False, "limit": 1},
        )
        with pytest.raises(ToolError) as alias_missing:
            await server.call_tool("entity_admin", {"action": "alias"})
        with pytest.raises(ToolError) as merge_missing:
            await server.call_tool("entity_admin", {"action": "merge"})
        return _decode_error_envelope(alias_missing.value), _decode_error_envelope(merge_missing.value)

    alias_missing, merge_missing = asyncio.run(exercise())
    assert alias_missing["status"] == "error"
    assert alias_missing["error_type"] == "invalid_argument"
    assert "alias_text" in alias_missing["hint"]
    assert merge_missing["status"] == "error"
    assert "source_entity_id" in merge_missing["hint"]


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


def test_mcp_chronicler_startup_gate_is_session_scoped(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> tuple[str, str]:
        first_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await first_server.call_tool(
            "startup_bundle",
            {
                "domain": "global",
                "agent": "pytest",
                "capture": False,
                "limit": 1,
            },
        )
        stored = await first_server.call_tool(
            "record_event",
            {
                "text": "Session one unlocked write.",
                "domain": "global",
                "category": "decision",
                "project": "status",
                "why": "Need to confirm session-local unlock behavior.",
                "agent": "pytest",
                "source_files": [],
            },
        )
        stored_payload = _decode_mcp_json(stored)

        second_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError) as error:
            await second_server.call_tool(
                "record_event",
                {
                    "text": "Session two should still be blocked.",
                    "domain": "global",
                    "category": "decision",
                    "project": "status",
                    "why": "Fresh server instance must not inherit startup unlock state.",
                    "agent": "pytest",
                    "source_files": [],
                },
            )
        _assert_startup_required(error.value, "record_event")
        return stored_payload["id"], "Session two should still be blocked."

    stored_id, blocked_text = asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        first_count = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (stored_id,)).fetchone()[0]
        blocked_count = connection.execute("SELECT COUNT(*) FROM events WHERE text = ?", (blocked_text,)).fetchone()[0]

    assert first_count == 1
    assert blocked_count == 0


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


def test_readonly_startup_bundle_capture_true_does_not_mutate_but_chronicler_can(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)

    def counts() -> tuple[int, int]:
        with open_connection(config) as connection:
            event_count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        return event_count, snapshot_count

    async def call_startup(profile: str) -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile=profile)
        result = await server.call_tool(
            "startup_bundle",
            {
                "domain": "global",
                "agent": "pytest",
                "capture": True,
                "limit": 2,
            },
        )
        return _decode_mcp_json(result)

    before = counts()
    readonly_payload = asyncio.run(call_startup("readonly"))
    after_readonly = counts()
    chronicler_payload = asyncio.run(call_startup("chronicler"))
    after_chronicler = counts()

    assert readonly_payload["domain"]["id"] == "global"
    assert after_readonly == before
    assert chronicler_payload["domain"]["id"] == "global"
    assert after_chronicler[1] > after_readonly[1]


def test_sources_audit_reports_lane_metadata_and_sensitive_defaults(loaded_manifest) -> None:
    payload = build_sources_audit(loaded_manifest, domain_id="global")

    assert payload["domain"]["id"] == "global"
    assert payload["source_catalog_schema_version"] == "2026-03-21.v1"
    assert payload["lane_policy_schema_version"] == "2026-03-21.v1"
    assert payload["source_catalog"]
    assert any(item["lane"] == "work" for item in payload["source_catalog"])
    # Sensitive lanes defined in the manifest (finances, health, life_admin) are disabled by default;
    # they appear in disabled_sensitive_lanes even without sources because coverage computes
    # from all lane_summary entries. At minimum no non-sensitive lane is in this list.
    for lane in payload["coverage"]["disabled_sensitive_lanes"]:
        lane_entry = next((item for item in payload["coverage"]["lanes"] if item["lane"] == lane), None)
        if lane_entry:
            assert lane_entry["sensitive"] is True
    status_source = next(item for item in payload["source_catalog"] if item["source_id"] == "status")
    assert status_source["class"] == "ssot_source"
    assert status_source["enabled"] is True
    assert status_source["questions_it_can_answer"]
    assert status_source["questions_it_cannot_answer"]


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
    # Seed Adobe via company-intel.json (both canonical name and lead_companies aliases).
    # AgentHub was removed 2026-04-17; company-intel.json is now the sole runtime source.
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
        source_refs=[{"source_id": "agenthub_leads_json", "value": "Business"}],
    )
    # Seed companies via company-intel.json. Includes noisy names that should be filtered
    # and clean canonical names that should survive. AgentHub was removed 2026-04-17.
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
                        "Collier Simon - remotive.com",
                    ],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    entities = materialize_normalized_entities(loaded_manifest, domain_id="global")
    company_names = {item["canonical_name"] for item in entities if item["entity_type"] == "company"}

    assert "Business" not in company_names
    assert "Built In Chicago" not in company_names
    assert "Scale Army" in company_names
    assert "Armis" in company_names
    assert "BMC Software" in company_names
    assert "Collier Simon" in company_names

    all_entities = fetch_normalized_entities(config, status=None, limit=200)
    business = next(item for item in all_entities if item["entity_type"] == "company" and item["canonical_key"] == "business")
    assert business["status"] == "inactive"


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
                        "memory": "Portfolio V1 shipped to production on example.com",
                        "metadata": {"project": "portfolio", "category": "milestone"},
                        "source_collection": "personal",
                        "source_collection_name": "chronicle_personal",
                    },
                    {
                        "id": "digest-1",
                        "memory": "Portfolio V1 shipped to production on example.com",
                        "metadata": {"project": "portfolio", "category": "milestone"},
                        "source_collection": "digest",
                        "source_collection_name": "chronicle_digest",
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
    assert set(hits[0]["source_collection_names"]) == {"chronicle_personal", "chronicle_digest"}


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


def test_record_event_falls_back_on_unknown_category(loaded_manifest) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "totally_new_bucket",
            "project": "status",
            "text": "Unknown category falls back to note",
            "why": "Soft validation stores the event instead of bouncing the write.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )
    assert stored["chronicle_status"] == "stored"
    assert stored["category"] == "note"
    assert stored["category_fallback"] == {"requested": "totally_new_bucket", "stored": "note"}


def test_record_event_accepts_insight_and_constraint(loaded_manifest) -> None:
    for category in ("insight", "constraint"):
        stored = record_event(
            loaded_manifest,
            {
                "agent": "pytest",
                "domain": "global",
                "category": category,
                "project": "status",
                "text": f"Category {category} is a first-class lane now",
                "why": "Agents used these names in the wild; they used to bounce.",
                "source_files": [],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
            },
        )
        assert stored["chronicle_status"] == "stored"
        assert stored["category"] == category
        assert "category_fallback" not in stored


def test_record_event_still_rejects_malformed_category(loaded_manifest) -> None:
    with pytest.raises(ValueError, match="Invalid category"):
        record_event(
            loaded_manifest,
            {
                "agent": "pytest",
                "domain": "global",
                "category": "×бесовщина×",
                "project": "status",
                "text": "Regex-invalid category must still bounce",
                "why": "Soft fallback only covers well-formed names.",
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
            "project": "demo-portfolio",
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
# ===== Phase-1 stability: thread offload, error envelope, health route =====


def test_slow_tool_does_not_block_fast_tool(chronicle_sandbox, monkeypatch) -> None:
    """The headline fix: one slow tool body must not freeze other MCP calls.

    Before the offload layer, sync tool bodies ran directly on the event loop,
    so a single slow call serialized every session behind it.
    """
    import max_chronicle.mcp_server as mcp_server_module

    def slow_audit(manifest, *, domain_id="global"):
        time.sleep(3.0)
        return {"domain": domain_id, "slow": True}

    monkeypatch.setattr(mcp_server_module, "build_sources_audit", slow_audit)

    async def exercise() -> float:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        loop = asyncio.get_running_loop()
        slow_task = asyncio.create_task(server.call_tool("sources_audit", {"domain": "global"}))
        await asyncio.sleep(0.2)  # let the slow tool enter its worker thread
        started = loop.time()
        await server.call_tool("recent_events", {"domain": "global", "limit": 1})
        fast_elapsed = loop.time() - started
        await slow_task
        return fast_elapsed

    fast_elapsed = asyncio.run(exercise())
    assert fast_elapsed < 1.0, f"fast tool waited {fast_elapsed:.2f}s behind the slow tool"


def test_tool_error_envelope_on_locked_db(chronicle_sandbox, monkeypatch) -> None:
    import sqlite3 as sqlite3_module

    import max_chronicle.mcp_server as mcp_server_module

    def locked_record_event(*args, **kwargs):
        raise sqlite3_module.OperationalError("database is locked")

    monkeypatch.setattr(mcp_server_module, "record_event", locked_record_event)

    async def exercise() -> dict[str, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await server.call_tool(
            "startup_bundle",
            {"domain": "global", "agent": "pytest", "capture": False, "limit": 1},
        )
        with pytest.raises(ToolError) as error:
            await server.call_tool(
                "record_event",
                {
                    "text": "Write into a locked database",
                    "domain": "global",
                    "category": "decision",
                    "agent": "pytest",
                    "source_files": [],
                },
            )
        return _decode_error_envelope(error.value)

    payload = asyncio.run(exercise())
    assert payload["status"] == "error"
    assert payload["error_type"] == "db_locked"
    assert payload["retryable"] is True
    assert "retry" in payload["hint"].casefold()


def test_tool_error_envelope_on_unexpected_crash(chronicle_sandbox, monkeypatch) -> None:
    import max_chronicle.mcp_server as mcp_server_module

    def crashing_query(*args, **kwargs):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(mcp_server_module, "query_memory", crashing_query)

    async def exercise() -> dict[str, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError) as error:
            await server.call_tool("query_memory", {"query": "anything"})
        return _decode_error_envelope(error.value)

    payload = asyncio.run(exercise())
    assert payload["status"] == "error"
    assert payload["error_type"] == "RuntimeError"
    assert payload["retryable"] is False
    assert payload["error"] == "simulated crash"


def test_health_route_reports_db_ok(chronicle_sandbox) -> None:
    from starlette.testclient import TestClient

    server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app()) as client:
        response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["db_ok"] is True
    assert payload["profile"] == "chronicler"
    assert payload["pid"] == os.getpid()
    assert isinstance(payload["uptime_s"], int)
    assert payload["version"]
    # Which database, manifest and installed code answered: a stale checkout
    # shadowing the release, or a wrong root, is visible without a shell.
    assert payload["db_path"] == str(chronicle_sandbox.chronicle_db)
    assert payload["manifest_path"] == str(chronicle_sandbox.manifest_path)
    assert payload["schema_version"] >= 10
    assert payload["schema_version"] == payload["target_schema_version"]
    assert Path(payload["module_path"]).name == "max_chronicle"


def test_offload_limiters_are_scoped_per_event_loop(chronicle_sandbox) -> None:
    """A CapacityLimiter belongs to the loop that created it.

    Caching them in a module global would hand a limiter from a finished loop
    to the next one; RunVars keep one set per async run.
    """
    import max_chronicle.mcp_server as mcp_server_module

    async def exercise() -> object:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await server.call_tool("recent_events", {"domain": "global", "limit": 1})
        return mcp_server_module._get_limiter("read")

    # Hold both limiters: an id() of a collected object can be reused, which
    # made this check fail at random when it compared ids.
    first = asyncio.run(exercise())
    second = asyncio.run(exercise())
    assert first is not second, "limiter leaked across event loops"


def test_mcp_activate_agent_does_not_return_the_full_document_bundle(
    chronicle_sandbox, loaded_manifest
) -> None:
    """The deprecated entry point used to hand back ~130 KB at session start.

    `attach_bundle` carried the verbatim text of every status document and the
    snapshot carried capture-time copies of the ledger and semantic recall — a
    context bomb on the one call older agents still make by this name. It now
    returns the prompt, a runtime digest and the freshness audit.
    """
    async def exercise() -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        activation = await server.call_tool(
            "activate_agent",
            {"domain": "global", "agent": "pytest", "capture": False},
        )
        return _decode_mcp_json(activation)

    payload = asyncio.run(exercise())

    assert "attach_bundle" not in payload
    assert payload["prompt"]
    assert payload["deprecated"]["superseded_by"] == "startup_bundle"
    assert "freshness_audit" in payload
    for bulk_key in ("recent_ledger", "mem0_snapshot_hits", "source_excerpts"):
        assert bulk_key not in payload["snapshot"]


def test_mcp_activate_agent_still_unlocks_and_still_carries_runtime_state(
    chronicle_sandbox, loaded_manifest
) -> None:
    """Trimming the payload must not cost the caller what the tool is for."""
    async def exercise() -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        activation = await server.call_tool(
            "activate_agent",
            {"domain": "global", "agent": "pytest", "capture": False},
        )
        stored = await server.call_tool(
            "record_event",
            {
                "text": "Write surface still unlocked after the payload was trimmed.",
                "domain": "global",
                "category": "decision",
                "project": "status",
                "why": "Regression guard for the activate_agent slimming.",
                "agent": "pytest",
                "source_files": [],
            },
        )
        return {
            "activation": _decode_mcp_json(activation),
            "stored": _decode_mcp_json(stored),
        }

    result = asyncio.run(exercise())

    assert result["stored"]["chronicle_status"] == "stored"
    snapshot = result["activation"]["snapshot"]
    assert snapshot["id"]
    assert "repos" in snapshot


def test_mcp_capture_snapshot_returns_a_receipt_not_the_whole_snapshot(
    chronicle_sandbox, loaded_manifest
) -> None:
    """Agents capture before a handoff — the reply must not eat the context they have left.

    The stored snapshot embeds a copy of the ledger, document excerpts and every
    normalized entity with its full source_refs list. The caller needs the id and
    what was written, not a replay.
    """
    async def exercise() -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await server.call_tool("startup_bundle", {"domain": "global", "agent": "pytest"})
        captured = await server.call_tool(
            "capture_snapshot",
            {"domain": "global", "agent": "pytest", "title": "Receipt shape"},
        )
        return _decode_mcp_json(captured)

    payload = asyncio.run(exercise())

    assert payload["chronicle_status"] == "stored"
    assert payload["id"]
    assert payload["projection_runs"]
    for bulk_key in ("recent_ledger", "mem0_snapshot_hits", "source_excerpts"):
        assert bulk_key not in payload
    assert set(payload["normalized_entities"]) == {"count", "ids"}


def test_query_context_leaves_old_situation_models_out(loaded_manifest) -> None:
    from max_chronicle.store import store_situation_model

    store_situation_model(config_from_manifest(loaded_manifest), {
        "domain": "global", "status": "active", "summary_text": "Quarantine drill is the current focus",
        "valid_at_utc": "2026-06-04T00:20:00Z"})

    payload = query_context(loaded_manifest, query="Quarantine drill", domain="global", limit=5)

    assert payload["interpretation_hits"] == []


def test_tools_say_what_a_call_may_change_and_bound_their_numbers(chronicle_sandbox) -> None:
    async def collect() -> dict[str, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        return {item.name: item for item in await server.list_tools()}

    tools = asyncio.run(collect())
    hints = {name: (tool.annotations.readOnlyHint, tool.annotations.destructiveHint, tool.annotations.openWorldHint)
             for name, tool in tools.items()}

    assert hints["query_memory"] == (True, None, False)
    assert hints["record_event"] == (False, False, False)
    assert hints["startup_bundle"] == (False, False, False)  # capture=true writes a snapshot
    assert hints["entity_admin"] == (False, True, False)
    assert hints.get("search_mem0_live", (True, None, True)) == (True, None, True)
    recall_query = tools["query_memory"].inputSchema["properties"]["query"]["description"]
    assert "Mem0" not in recall_query and "status" not in recall_query
    window = tools["state_at"].inputSchema["properties"]["window_hours"]
    assert (window["minimum"], window["maximum"]) == (1, 720)


def _mem0_key() -> str:
    import secrets

    return "sk-" + "proj-" + secrets.token_urlsafe(36)


def test_query_context_filters_the_mem0_dump_and_status_files(chronicle_sandbox, loaded_manifest) -> None:
    key, other = _mem0_key(), _mem0_key()
    (chronicle_sandbox.status_root / "mem0-dump.json").write_text(json.dumps({"memories": [
        {"id": "m-1", "memory": f"Marmoset gateway credentials: {key}", "metadata": {}}]}), encoding="utf-8")
    status = chronicle_sandbox.status_root / "status.md"
    status.write_text(status.read_text(encoding="utf-8") + f"\n## Marmoset gateway\nThe marmoset key is {other}\n",
                      encoding="utf-8")

    payload = query_context(loaded_manifest, query="marmoset gateway", domain="global", limit=5,
                            mode="truth_plus_interpretation")

    text = json.dumps(payload)
    assert payload["mem0_dump_hits"] and payload["status_hits"]
    assert key not in text and other not in text and "[REDACTED:" in text


def test_live_mem0_results_pass_the_secret_filter(loaded_manifest, monkeypatch) -> None:
    import max_chronicle.service as service_module

    key = _mem0_key()

    class _Bridge:
        returncode, stderr = 0, ""
        stdout = json.dumps({"results": [{"id": "m-1", "memory": f"API key {key}", "metadata": {}}], "count": 1})

    monkeypatch.setattr(service_module.subprocess, "run", lambda *args, **kwargs: _Bridge())
    monkeypatch.setenv("CHRONICLE_FEATURE_SEARCH_MEM0_LIVE", "1")

    out = service_module.search_mem0_live_service(loaded_manifest, query="api key", timeout_s=5)

    assert key not in json.dumps(out) and out["redactions"]
