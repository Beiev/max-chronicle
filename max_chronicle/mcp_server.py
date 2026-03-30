from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Annotated, Any, Literal

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from .config import default_manifest_path
from .runtime_context import load_manifest, parse_when
from .service import (
    DEFAULT_QUERY_MODE,
    build_attach_bundle,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    capture_runtime_snapshot,
    current_state,
    default_mem0_status,
    materialize_normalized_entities,
    materialize_situation_model,
    project_state,
    query_context,
    record_scenario,
    record_event,
    reconstruct_timeline,
    review_scenario,
    render_projections,
    run_lenses,
)
from .store import config_from_manifest, fetch_recent_events, fetch_latest_snapshot

READ_ONLY_PROFILE = "readonly"
CHRONICLER_PROFILE = "chronicler"
MCP_PROFILES = {READ_ONLY_PROFILE, CHRONICLER_PROFILE}
STARTUP_GATE_DESCRIPTION = (
    "Call `startup_bundle(...)` or `activate_agent(...)` to warm the session explicitly. "
    "If a mutating tool is called first, Chronicle runs an internal compact startup read once for that session before mutating."
)
AUTO_STARTUP_AGENT = "mcp-auto-startup"
AUTO_STARTUP_LIMIT = 3
QUERY_CONTEXT_MODE = Literal[
    "truth_only",
    "truth_plus_interpretation",
    "truth_plus_interpretation_plus_scenarios",
]
REVIEW_STATUS = Literal["matched", "partial", "missed"]
DOMAIN_ARG = Annotated[str, Field(description="Chronicle domain id from the manifest.")]
OPTIONAL_DOMAIN_ARG = Annotated[str | None, Field(description="Optional Chronicle domain id from the manifest.")]
AGENT_ARG = Annotated[str, Field(description="Agent name recorded in the output.")]
OPTIONAL_TITLE_ARG = Annotated[str | None, Field(description="Optional snapshot title.")]
OPTIONAL_FOCUS_ARG = Annotated[str | None, Field(description="Optional focus string.")]
CAPTURE_ARG = Annotated[
    bool,
    Field(description="Capture a fresh runtime snapshot before building the bundle."),
]
LIMIT_ARG = Annotated[int, Field(description="Maximum number of recent items or hits to include.")]
COMPACT_ARG = Annotated[bool, Field(description="Return the compact startup bundle variant.")]
TIMESTAMP_ARG = Annotated[str, Field(description="ISO timestamp to reconstruct around.")]
WINDOW_HOURS_ARG = Annotated[int, Field(description="Search window in hours around the timestamp.")]
QUERY_ARG = Annotated[str, Field(description="Search string to match across Chronicle, status sources, and Mem0.")]
QUERY_MODE_ARG = Annotated[
    QUERY_CONTEXT_MODE,
    Field(description="Retrieval mode that controls whether derived layers and scenario hits are included."),
]
TEXT_ARG = Annotated[str, Field(description="Durable event text to write into Chronicle.")]
WHY_ARG = Annotated[str | None, Field(description="Optional reason or rationale for the change.")]
CATEGORY_ARG = Annotated[
    str | None,
    Field(
        description=(
            "Optional event category for lane and recall routing. "
            "Common values include decision, implementation, maintenance, milestone, note, anomaly, digest_run, and git_commit."
        )
    ),
]
PROJECT_ARG = Annotated[str | None, Field(description="Optional project slug attached to the event.")]
SOURCE_FILES_ARG = Annotated[
    list[str] | None,
    Field(description="Optional source file paths to archive with the event."),
]
SCENARIO_NAME_ARG = Annotated[str, Field(description="Scenario name.")]
ASSUMPTIONS_ARG = Annotated[list[str], Field(description="Scenario assumptions to store with the run.")]
CHANGED_VARIABLES_ARG = Annotated[
    dict[str, str] | None,
    Field(description="Optional key-value map of changed variables."),
]
EXPECTED_OUTCOMES_ARG = Annotated[
    list[str] | None,
    Field(description="Optional expected outcomes for the scenario."),
]
FAILURE_MODES_ARG = Annotated[
    list[str] | None,
    Field(description="Optional failure modes to watch for."),
]
CONFIDENCE_ARG = Annotated[float, Field(description="Scenario confidence from 0 to 1.")]
REVIEW_DUE_AT_ARG = Annotated[str | None, Field(description="Optional ISO timestamp for the review due date.")]
SCENARIO_ID_ARG = Annotated[str, Field(description="Scenario id to review.")]
STATUS_ARG = Annotated[
    REVIEW_STATUS,
    Field(description="Review status to store. Allowed values: matched, partial, missed."),
]
SUMMARY_ARG = Annotated[str, Field(description="Review summary to store.")]
OUTCOME_EVENT_ID_ARG = Annotated[str | None, Field(description="Optional outcome event id linked to the review.")]


def _startup_gate_key(ctx: Context | None) -> str | None:
    if ctx is None:
        return None
    try:
        session = ctx.session
    except Exception:
        session = None
    if session is not None:
        return f"session:{id(session)}"
    return None


def build_server(manifest_path: Path | None = None, *, profile: str = CHRONICLER_PROFILE) -> FastMCP:
    if profile not in MCP_PROFILES:
        raise ValueError(f"Unsupported MCP profile: {profile}")
    resolved_manifest_path = manifest_path or default_manifest_path()

    def manifest() -> dict:
        return load_manifest(resolved_manifest_path)

    startup_gate_unlocked: set[str] = set()
    sessionless_gate_key = f"server:{id(startup_gate_unlocked)}"

    def unlock_startup_gate(ctx: Context | None) -> None:
        startup_gate_unlocked.add(_startup_gate_key(ctx) or sessionless_gate_key)

    def ensure_startup_gate(
        ctx: Context | None,
        *,
        domain: str = "global",
        agent: str = AUTO_STARTUP_AGENT,
    ) -> None:
        key = _startup_gate_key(ctx) or sessionless_gate_key
        if key in startup_gate_unlocked:
            return
        # Warm the session with the same compact read path exposed to agents,
        # but keep it non-mutating by forcing capture=False.
        build_startup_bundle(
            manifest(),
            domain_id=domain,
            agent=agent,
            capture=False,
            limit=AUTO_STARTUP_LIMIT,
            compact=True,
        )
        unlock_startup_gate(ctx)

    server = FastMCP(
        name="Max Chronicle" if profile == CHRONICLER_PROFILE else "Max Chronicle Read Only",
        instructions=(
            "Chronicle is the canonical local-first memory system for Maksym Beiev. "
            "Use Chronicle DB as truth, status markdown as readable projections, and Mem0 as semantic recall. "
            "Read surfaces are informational only and do not unlock the session; "
            "call `startup_bundle` or `activate_agent` when you want the startup context payload explicitly, "
            "and note that mutating tools auto-start Chronicle with a compact startup read on first use."
        ),
        log_level="WARNING",
    )

    @server.resource(
        "chronicle://attach/current",
        title="Current Attach Bundle",
        description="Machine-readable agent attach bundle for the global domain.",
    )
    def resource_attach_current() -> dict:
        return build_attach_bundle(manifest(), domain_id="global", agent="mcp-resource", capture=False)

    @server.resource(
        "chronicle://attach/domain/{domain}",
        title="Domain Attach Bundle",
        description="Machine-readable agent attach bundle for a domain.",
    )
    def resource_attach_domain(domain: str) -> dict:
        return build_attach_bundle(manifest(), domain_id=domain, agent="mcp-resource", capture=False)

    @server.resource(
        "chronicle://state/current",
        title="Current Chronicle State",
        description="Database summary, latest snapshot, and recent durable events.",
    )
    def resource_state_current() -> dict:
        return current_state(manifest(), domain="global")

    @server.resource(
        "chronicle://state/domain/{domain}",
        title="Domain Chronicle State",
        description="Latest snapshot and recent events for a domain.",
    )
    def resource_state_domain(domain: str) -> dict:
        return current_state(manifest(), domain=domain)

    @server.resource(
        "chronicle://sources/audit",
        title="Chronicle Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for the global domain.",
    )
    def resource_sources_audit() -> dict:
        return build_sources_audit(manifest(), domain_id="global")

    @server.resource(
        "chronicle://sources/audit/{domain}",
        title="Domain Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for a domain.",
    )
    def resource_sources_audit_domain(domain: str) -> dict:
        return build_sources_audit(manifest(), domain_id=domain)

    @server.resource(
        "chronicle://timeline/{timestamp}",
        title="Chronicle Timeline",
        description="Nearest global snapshots and events around a timestamp.",
    )
    def resource_timeline(timestamp: str) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(loaded, timestamp=target, domain="global", window_hours=6, limit=3)

    @server.resource(
        "chronicle://timeline/domain/{domain}/{timestamp}",
        title="Domain Timeline",
        description="Nearest snapshots and events around a timestamp for a domain.",
    )
    def resource_timeline_domain(domain: str, timestamp: str) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(loaded, timestamp=target, domain=domain, window_hours=6, limit=3)

    @server.resource(
        "chronicle://project/{project}",
        title="Project State",
        description="Recent events and relations for a project slug.",
    )
    def resource_project(project: str) -> dict:
        return project_state(manifest(), project=project)

    @server.resource(
        "chronicle://world/latest",
        title="Latest World Context",
        description="Digest context from the latest Chronicle snapshot.",
    )
    def resource_world_latest() -> dict:
        loaded = manifest()
        config = config_from_manifest(loaded)
        snapshot = fetch_latest_snapshot(config)
        return {
            "snapshot_id": snapshot.get("id") if snapshot else None,
            "captured_at_local": snapshot.get("captured_at_local") if snapshot else None,
            "digest": (snapshot or {}).get("digest", {}),
        }

    @server.tool(
        name="startup_bundle",
        description=(
            "Build the startup brief bundle for an agent without mutating Chronicle by default. "
            "Calling this tool unlocks the chronicler write surface for the current session."
        ),
    )
    def tool_startup_bundle(
        domain: DOMAIN_ARG = "global",
        agent: AGENT_ARG = "mcp",
        title: OPTIONAL_TITLE_ARG = None,
        focus: OPTIONAL_FOCUS_ARG = None,
        capture: CAPTURE_ARG = False,
        limit: LIMIT_ARG = 3,
        compact: COMPACT_ARG = True,
        ctx: Context | None = None,
    ) -> dict:
        payload = build_startup_bundle(
            manifest(),
            domain_id=domain,
            agent=agent,
            title=title,
            focus=focus,
            capture=capture,
            limit=limit,
            compact=compact,
        )
        unlock_startup_gate(ctx)
        return payload

    @server.tool(
        name="recent_events",
        description="Read recent Chronicle events for a domain.",
    )
    def tool_recent_events(domain: DOMAIN_ARG = "global", limit: LIMIT_ARG = 10) -> list[dict]:
        loaded = manifest()
        config = config_from_manifest(loaded)
        return fetch_recent_events(config, limit=limit, domain=domain, visibility="raw")

    @server.tool(
        name="state_at",
        description="Reconstruct Chronicle state around an ISO timestamp.",
    )
    def tool_state_at(
        timestamp: TIMESTAMP_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        window_hours: WINDOW_HOURS_ARG = 6,
        limit: LIMIT_ARG = 3,
    ) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(loaded, timestamp=target, domain=domain, window_hours=window_hours, limit=limit)

    @server.tool(
        name="query_context",
        description="Search Chronicle events, status sources, and mem0 dump for relevant context.",
    )
    def tool_query_context(
        query: QUERY_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        limit: LIMIT_ARG = 5,
        mode: QUERY_MODE_ARG = DEFAULT_QUERY_MODE,
    ) -> dict:
        return query_context(manifest(), query=query, domain=domain, limit=limit, mode=mode)

    @server.tool(
        name="sources_audit",
        description="Inspect source coverage, lane enablement, freshness, and trust metadata.",
    )
    def tool_sources_audit(domain: DOMAIN_ARG = "global") -> dict:
        return build_sources_audit(manifest(), domain_id=domain)

    if profile == CHRONICLER_PROFILE:
        @server.tool(
            name="activate_agent",
            description=(
                "Capture current state if needed and return the universal activation prompt. "
                "Calling this tool unlocks the chronicler write surface for the current session."
            ),
        )
        def tool_activate_agent(
            domain: DOMAIN_ARG = "global",
            agent: AGENT_ARG = "mcp",
            title: OPTIONAL_TITLE_ARG = None,
            focus: OPTIONAL_FOCUS_ARG = None,
            capture: CAPTURE_ARG = True,
            ctx: Context | None = None,
        ) -> dict:
            payload = build_activation(
                manifest(),
                domain_id=domain,
                agent=agent,
                title=title,
                focus=focus,
                capture=capture,
            )
            unlock_startup_gate(ctx)
            return payload

        @server.tool(
            name="record_event",
            description=(
                "Write a durable Chronicle event and archive linked source files. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_record_event(
            text: TEXT_ARG,
            why: WHY_ARG = None,
            domain: DOMAIN_ARG = "global",
            category: CATEGORY_ARG = None,
            project: PROJECT_ARG = None,
            agent: AGENT_ARG = "mcp",
            source_files: SOURCE_FILES_ARG = None,
            ctx: Context | None = None,
        ) -> dict:
            ensure_startup_gate(ctx, domain=domain, agent=agent)
            return record_event(
                manifest(),
                {
                    "agent": agent,
                    "domain": domain,
                    "category": category,
                    "project": project,
                    "text": text,
                    "why": why,
                    "source_files": source_files or [],
                    "mem0_status": default_mem0_status(
                        {"category": category},
                        source_kind="chronicle_mcp",
                    ),
                    "mem0_error": None,
                    "mem0_raw": None,
                },
                append_compat=True,
                dedupe=True,
                source_kind="chronicle_mcp",
                imported_from="max_chronicle.mcp.record_event",
            )

        @server.tool(
            name="capture_snapshot",
            description=(
                "Capture live runtime state into Chronicle, archive evidence, and refresh projections. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_capture_snapshot(
            domain: DOMAIN_ARG = "global",
            agent: AGENT_ARG = "mcp",
            title: OPTIONAL_TITLE_ARG = None,
            focus: OPTIONAL_FOCUS_ARG = None,
            ctx: Context | None = None,
        ) -> dict:
            ensure_startup_gate(ctx, domain=domain, agent=agent)
            return capture_runtime_snapshot(
                manifest(),
                domain_id=domain,
                agent=agent,
                title=title,
                focus=focus,
            )

        @server.tool(
            name="normalize_entities",
            description=(
                "Materialize normalized entities from Chronicle truth and runtime evidence. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_normalize_entities(domain: DOMAIN_ARG = "global", ctx: Context | None = None) -> dict:
            ensure_startup_gate(ctx, domain=domain)
            entities = materialize_normalized_entities(manifest(), domain_id=domain)
            return {"domain": domain, "normalized_entities": entities, "count": len(entities)}

        @server.tool(
            name="build_situation_model",
            description=(
                "Materialize the latest situation model for a domain. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_build_situation_model(domain: DOMAIN_ARG = "global", ctx: Context | None = None) -> dict:
            ensure_startup_gate(ctx, domain=domain)
            return materialize_situation_model(manifest(), domain_id=domain)

        @server.tool(
            name="run_lenses",
            description=(
                "Materialize deterministic lens runs for the latest situation model. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_run_lenses(
            domain: DOMAIN_ARG = "global",
            persist: Annotated[bool, Field(description="When true, persist the lens runs to Chronicle.")] = True,
            ctx: Context | None = None,
        ) -> dict:
            ensure_startup_gate(ctx, domain=domain)
            runs = run_lenses(manifest(), domain_id=domain, persist=persist)
            return {"domain": domain, "lens_runs": runs, "count": len(runs)}

        @server.tool(
            name="record_scenario",
            description=(
                "Store a what-if scenario against the latest situation model. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_record_scenario(
            name: SCENARIO_NAME_ARG,
            assumptions: ASSUMPTIONS_ARG,
            domain: DOMAIN_ARG = "global",
            changed_variables: CHANGED_VARIABLES_ARG = None,
            expected_outcomes: EXPECTED_OUTCOMES_ARG = None,
            failure_modes: FAILURE_MODES_ARG = None,
            confidence: CONFIDENCE_ARG = 0.6,
            review_due_at: REVIEW_DUE_AT_ARG = None,
            ctx: Context | None = None,
        ) -> dict:
            ensure_startup_gate(ctx, domain=domain)
            return record_scenario(
                manifest(),
                domain_id=domain,
                scenario_name=name,
                assumptions=assumptions,
                changed_variables=changed_variables,
                expected_outcomes=expected_outcomes,
                failure_modes=failure_modes,
                confidence=confidence,
                review_due_at=review_due_at,
            )

        @server.tool(
            name="review_scenario",
            description=(
                "Store a replay/eval review for a previously recorded scenario. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_review_scenario(
            scenario_id: SCENARIO_ID_ARG,
            status: STATUS_ARG,
            summary: SUMMARY_ARG,
            outcome_event_id: OUTCOME_EVENT_ID_ARG = None,
            ctx: Context | None = None,
        ) -> dict:
            ensure_startup_gate(ctx)
            return review_scenario(
                manifest(),
                scenario_id=scenario_id,
                status=status,
                review_summary=summary,
                outcome_event_id=outcome_event_id,
            )

        @server.tool(
            name="render_projections",
            description=(
                "Render markdown projections from the latest Chronicle snapshot. "
                f"{STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_render_projections(ctx: Context | None = None) -> list[dict]:
            ensure_startup_gate(ctx)
            return render_projections(manifest())

    @server.prompt(
        name="activate",
        description="Return the current activation prompt for a new agent.",
    )
    def prompt_activate(domain: str = "global") -> list[dict]:
        activation = build_activation(manifest(), domain_id=domain, agent="mcp-prompt", capture=False)
        return [{"role": "user", "content": activation["prompt"]}]

    @server.prompt(
        name="continue_work",
        description="Provide a compact continuation brief for ongoing work.",
    )
    def prompt_continue_work(domain: str = "global") -> list[dict]:
        state = build_startup_bundle(manifest(), domain_id=domain, agent="mcp-prompt", capture=False, limit=6, compact=True)
        content = json.dumps(state, ensure_ascii=False, indent=2)
        return [{"role": "user", "content": f"Continue from this Chronicle state:\n{content}"}]

    @server.prompt(
        name="reconstruct_moment",
        description="Provide the timeline state around a specific timestamp.",
    )
    def prompt_reconstruct_moment(timestamp: str) -> list[dict]:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        content = json.dumps(
            reconstruct_timeline(loaded, timestamp=target, domain="global", window_hours=6, limit=3),
            ensure_ascii=False,
            indent=2,
        )
        return [{"role": "user", "content": f"Reconstruct this moment from Chronicle:\n{content}"}]

    return server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve Max Chronicle over MCP.")
    parser.add_argument("--manifest", type=Path, default=default_manifest_path(), help="Path to SSOT_MANIFEST.toml")
    parser.add_argument(
        "--profile",
        choices=sorted(MCP_PROFILES),
        default=CHRONICLER_PROFILE,
        help="MCP capability profile",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="MCP transport",
    )
    return parser


def _main(default_profile: str = CHRONICLER_PROFILE) -> int:
    parser = build_parser()
    parser.set_defaults(profile=default_profile)
    args = parser.parse_args()
    server = build_server(args.manifest, profile=args.profile)
    server.run(transport=args.transport)
    return 0


def main() -> int:
    return _main()


def main_readonly() -> int:
    original_argv = sys.argv[:]
    try:
        if "--profile" not in sys.argv[1:]:
            sys.argv = [sys.argv[0], "--profile", READ_ONLY_PROFILE, *sys.argv[1:]]
        return _main(READ_ONLY_PROFILE)
    finally:
        sys.argv = original_argv


def main_chronicler() -> int:
    original_argv = sys.argv[:]
    try:
        if "--profile" not in sys.argv[1:]:
            sys.argv = [sys.argv[0], "--profile", CHRONICLER_PROFILE, *sys.argv[1:]]
        return _main(CHRONICLER_PROFILE)
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    raise SystemExit(main())
