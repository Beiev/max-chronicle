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

from mcp.server.mcpserver.exceptions import ToolError
from max_chronicle.mcp_server import _gate_session, build_server
from max_chronicle.runtime_context import load_manifest
from max_chronicle.service import (
    build_activation,
    build_sources_audit,
    build_startup_bundle,
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
    result = getattr(result, "content", result)  # SDK 2: a CallToolResult
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
    async def collect(profile: str) -> tuple[list[str], list[str]]:
        server = build_server(profile=profile)
        tools = await server.list_tools()
        resources = await server.list_resources()
        return [item.name for item in tools], [str(item.uri) for item in resources]

    tools, resources = asyncio.run(collect("readonly"))
    _, admin_resources = asyncio.run(collect("admin"))

    assert tools == ["startup_bundle", "recent_events", "state_at", "query_memory", "get_memory"]
    # The legacy attach/state/audit documents are an operator's, not an agent's.
    assert "chronicle://attach/current" not in resources
    assert "chronicle://attach/current" in admin_resources
    assert "chronicle://sources/audit" in admin_resources


def test_mcp_chronicler_profile_exposes_write_surface() -> None:
    async def collect(profile: str) -> list[str]:
        server = build_server(profile=profile)
        tools = await server.list_tools()
        return [item.name for item in tools]

    tools = asyncio.run(collect("chronicler"))
    admin_tools = asyncio.run(collect("admin"))

    assert "record_event" in tools
    # Maintenance lives in the admin profile: agents never called it (client logs, 2026-10).
    for name in ("capture_snapshot", "entity_admin", "query_context", "sources_audit", "activate_agent"):
        assert name not in tools and name in admin_tools
    # Consolidated away in the 15->10 tool-surface simplification:
    for tool_list in (tools, admin_tools):
        assert "normalize_entities" not in tool_list
        assert "render_projections" not in tool_list
        assert "add_entity_alias" not in tool_list
        assert "merge_entities" not in tool_list
        assert "entity_resolution_report" not in tool_list


def test_mcp_chronicler_mutating_tool_descriptions_include_startup_guidance(chronicle_sandbox) -> None:
    async def collect() -> dict[str, str]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        tools = await server.list_tools()
        return {item.name: item.description or "" for item in tools}

    descriptions = asyncio.run(collect())

    # A write needs no prior call; continuing a task means reading its checkpoint first.
    assert "No prior call is required" in descriptions["record_event"]
    assert "startup_bundle(project, task_id)" in descriptions["record_event"]
    assert "activate_agent" not in descriptions["record_event"]


def test_mcp_tool_input_schemas_include_param_descriptions_and_query_mode_enum(chronicle_sandbox) -> None:
    async def collect() -> dict[str, dict[str, Any]]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        tools = await server.list_tools()
        return {item.name: item.input_schema for item in tools}

    schemas = asyncio.run(collect())

    async def collect_admin() -> dict[str, dict[str, Any]]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
        return {item.name: item.input_schema for item in await server.list_tools()}

    admin_schemas = asyncio.run(collect_admin())

    startup_props = schemas["startup_bundle"]["properties"]
    assert startup_props["domain"]["description"] == "Chronicle domain id from the manifest."
    assert startup_props["capture"]["description"] == "Capture a fresh runtime snapshot before building the bundle."
    assert startup_props["compact"]["description"] == "Return the compact startup bundle variant."

    query_props = admin_schemas["query_context"]["properties"]
    assert query_props["query"]["description"] == (
        "Search string matched across Chronicle events and status markdown sections."
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


def test_mcp_chronicler_record_event_needs_no_startup(chronicle_sandbox, loaded_manifest) -> None:
    """The gate is gone: every reconnect and restart re-locked it (22 refusals in a month)."""
    async def exercise() -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        arguments = {
            "text": "MCP write before any startup.",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "why": "Writes no longer wait for a startup call.",
            "agent": "pytest",
            "source_files": [],
        }
        return _decode_mcp_json(await server.call_tool("record_event", arguments))

    stored = asyncio.run(exercise())

    assert stored["chronicle_status"] == "stored"
    assert stored["notice"] == "startup_not_read" and "startup_bundle" in stored["hint"]
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        event_count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE text = ?", ("MCP write before any startup.",)).fetchone()[0]
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]

    assert event_count == 1
    assert snapshot_count == 0


def test_mcp_capture_snapshot_is_an_admin_tool(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> None:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError, match="Unknown tool"):
            await server.call_tool(
                "capture_snapshot",
                {"domain": "global", "agent": "pytest", "title": "Not here", "focus": "tests"},
            )

    asyncio.run(exercise())

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        event_count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    assert snapshot_count == 0
    assert event_count == 0


def test_mcp_entity_admin_lives_in_the_admin_profile(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> dict:
        chronicler = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        with pytest.raises(ToolError, match="Unknown tool"):
            await chronicler.call_tool("entity_admin", {"action": "normalize", "domain": "global"})
        admin = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
        report = _decode_mcp_json(await admin.call_tool("entity_admin", {"action": "report"}))
        normalized = _decode_mcp_json(await admin.call_tool("entity_admin", {"action": "normalize", "domain": "global"}))
        return {"report": report, "normalized": normalized}

    result = asyncio.run(exercise())

    assert result["report"]["action"] == "report"
    assert result["normalized"]["action"] == "normalize"


def test_mcp_chronicler_entity_admin_validates_action_params(chronicle_sandbox) -> None:
    async def exercise() -> tuple[Any, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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


def test_mcp_chronicler_activate_agent_then_writes(chronicle_sandbox, loaded_manifest) -> None:
    async def exercise() -> str:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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


def test_mcp_a_fresh_server_accepts_writes_without_startup(chronicle_sandbox, loaded_manifest) -> None:
    """What the old session-scoped gate refused: a new connection writing straight away."""
    async def exercise() -> tuple[dict, dict]:
        first_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        await first_server.call_tool("startup_bundle", {"agent": "pytest", "project": "status"})
        first = _decode_mcp_json(await first_server.call_tool("record_event", {
            "text": "Session one write.", "domain": "global", "category": "decision", "project": "status",
            "agent": "pytest", "source_files": []}))
        second_server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="chronicler")
        second = _decode_mcp_json(await second_server.call_tool("record_event", {
            "text": "Session two write after a reconnect.", "domain": "global", "category": "decision",
            "project": "status", "agent": "pytest", "source_files": []}))
        return first, second

    first, second = asyncio.run(exercise())

    assert first["chronicle_status"] == second["chronicle_status"] == "stored"
    assert "notice" not in first
    assert second["notice"] == "startup_not_read"  # a new server knows nothing yet; it reminds, never refuses


def test_mcp_read_surfaces_remain_available_without_startup(chronicle_sandbox) -> None:
    async def exercise() -> tuple[list[dict], dict, dict]:
        chronicler = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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
                "mode": "bundle",
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
                "mode": "bundle",
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


def test_freshness_audit_surfaces_stale_inputs_and_prompt_warnings(chronicle_sandbox, loaded_manifest) -> None:
    status_path = Path(loaded_manifest["source_map"]["status"]["path"])
    priorities_path = Path(loaded_manifest["source_map"]["priorities"]["path"])
    status_path.unlink()
    stale_ts = datetime(2026, 3, 1, 12, 0, 0).timestamp()
    os.utime(priorities_path, (stale_ts, stale_ts))

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
    assert "attach_source_stale" in issue_kinds
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
            },
        )


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
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_agent_contract.high_signal_guard",
    )

    assert stored["memory_guard"]["verdict"] == "durable"
    assert stored["memory_guard"]["visibility"] == "default"


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
            "SELECT visibility, payload_json FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
    event_payload = json.loads(event_row["payload_json"])
    assert event_row["visibility"] == "raw_only"
    assert event_payload["memory_guard"]["verdict"] == "local_only"
    assert event_payload["memory_guard"]["source"] == "manual_operator_guard"


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
        },
    )

    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection, connection:
        event_row = connection.execute(
            "SELECT payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        event_payload = json.loads(event_row["payload_json"])
        event_payload.pop("category", None)
        connection.execute(
            "UPDATE events SET category = '', payload_json = ? WHERE id = ?",
            (json.dumps(event_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
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
    repaired_payload = json.loads(repaired_row["payload_json"])
    assert repaired_row["category"] == "note"
    assert repaired_payload["category"] == "note"


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
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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
    for bulk_key in ("recent_ledger", "source_excerpts"):
        assert bulk_key not in payload["snapshot"]


def test_mcp_activate_agent_still_writes_and_still_carries_runtime_state(
    chronicle_sandbox, loaded_manifest
) -> None:
    """Trimming the payload must not cost the caller what the tool is for."""
    async def exercise() -> dict:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
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
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
        captured = await server.call_tool(
            "capture_snapshot",
            {"domain": "global", "agent": "pytest", "title": "Receipt shape"},
        )
        return _decode_mcp_json(captured)

    payload = asyncio.run(exercise())

    assert payload["chronicle_status"] == "stored"
    assert payload["id"]
    assert payload["projection_runs"]
    for bulk_key in ("recent_ledger", "source_excerpts"):
        assert bulk_key not in payload
    assert set(payload["normalized_entities"]) == {"count", "ids"}


def test_tools_say_what_a_call_may_change_and_bound_their_numbers(chronicle_sandbox) -> None:
    async def collect() -> dict[str, Any]:
        server = _sandbox_mcp_server(chronicle_sandbox.manifest_path, profile="admin")
        return {item.name: item for item in await server.list_tools()}

    tools = asyncio.run(collect())
    hints = {name: (tool.annotations.read_only_hint, tool.annotations.destructive_hint, tool.annotations.open_world_hint)
             for name, tool in tools.items()}

    assert hints["query_memory"] == (True, None, False)
    assert hints["record_event"] == (False, False, False)
    assert hints["startup_bundle"] == (False, False, False)  # capture=true writes a snapshot
    assert hints["entity_admin"] == (False, True, False)
    recall_query = tools["query_memory"].input_schema["properties"]["query"]["description"]
    assert "Mem0" not in recall_query and "status" not in recall_query
    window = tools["state_at"].input_schema["properties"]["window_hours"]
    assert (window["minimum"], window["maximum"]) == (1, 720)


def _secret_key() -> str:
    import secrets

    return "sk-" + "proj-" + secrets.token_urlsafe(36)


def test_query_context_filters_the_status_files(chronicle_sandbox, loaded_manifest) -> None:
    key = _secret_key()
    status = chronicle_sandbox.status_root / "status.md"
    status.write_text(status.read_text(encoding="utf-8") + f"\n## Marmoset gateway\nThe marmoset key is {key}\n",
                      encoding="utf-8")

    payload = query_context(loaded_manifest, query="marmoset gateway", domain="global", limit=5,
                            mode="truth_plus_interpretation")

    text = json.dumps(payload)
    assert payload["status_hits"]
    assert key not in text and "[REDACTED:" in text
