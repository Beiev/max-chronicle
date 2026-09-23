from __future__ import annotations

import argparse
import functools
import json
import logging
import os
import sqlite3
import sys
import threading
import time
import weakref
import uuid
from pathlib import Path
from typing import Annotated, Any, Callable, Literal

import anyio
import anyio.to_thread
from anyio.lowlevel import RunVar
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse

from . import __version__

from .config import EXIT_CONFIG_ERROR, EXIT_SCHEMA_ACTION, ChronicleConfigError, default_manifest_path
from .db import MigrationError
from .runtime_context import load_manifest, load_manifest_cached, parse_when
from .service import (
    DEFAULT_QUERY_MODE,
    add_entity_alias_service,
    build_attach_bundle,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    capture_runtime_snapshot,
    current_state,
    default_mem0_status,
    entity_resolution_report_service,
    materialize_normalized_entities,
    merge_entities_service,
    project_state,
    query_context,
    query_memory,
    record_event,
    reconstruct_timeline,
    search_mem0_live_service,
)
from .store import (
    digest_snapshot,
    config_from_manifest,
    fetch_latest_snapshot,
    fetch_recent_events,
    open_connection,
    prepare_database,
    set_read_only_process,
    target_schema_version,
)
from .memory import Checkpoint, FactInput

READ_ONLY_PROFILE = "readonly"
CHRONICLER_PROFILE = "chronicler"
MCP_PROFILES = {READ_ONLY_PROFILE, CHRONICLER_PROFILE}
STARTUP_GATE_DESCRIPTION = (
    "Call `startup_bundle(...)` or `activate_agent(...)` once per session before mutating Chronicle."
)
QUERY_CONTEXT_MODE = Literal[
    "truth_only",
    "truth_plus_interpretation",
    "truth_plus_interpretation_plus_scenarios",
]
DOMAIN_ARG = Annotated[str, Field(description="Chronicle domain id from the manifest.")]
OPTIONAL_DOMAIN_ARG = Annotated[str | None, Field(description="Optional Chronicle domain id from the manifest.")]
AGENT_ARG = Annotated[str, Field(description=(
    "Agent name recorded in the output. Canonical names: claude, codex, glm, "
    "deepseek, opencode, memory-librarian, transcript-analyst, or a stable "
    "pipeline id. Session-flavored variants (claude-<session>, Codex, "
    "opencode-glm*) are normalized to the canonical actor; put session "
    "context in why/text instead."
))]
OPTIONAL_TITLE_ARG = Annotated[str | None, Field(description="Optional snapshot title.")]
OPTIONAL_FOCUS_ARG = Annotated[str | None, Field(description="Optional focus string.")]
CAPTURE_ARG = Annotated[
    bool,
    Field(description="Capture a fresh runtime snapshot before building the bundle."),
]
LIMIT_ARG = Annotated[int, Field(ge=1, le=100, description="Maximum number of recent items or hits (1–100).")]
TASK_ARG = Annotated[str | None, Field(description="Stable task ID within project. Requires project; reuse it across agents and sessions.")]
SESSION_ARG = Annotated[str | None, Field(description="Originating session ID; defaults to the current MCP session identity.")]
REQUEST_ARG = Annotated[str | None, Field(description="Unique write request ID. Reuse unchanged on retries; changed input requires a new ID.")]
CURSOR_ARG = Annotated[str | None, Field(description="Cursor from task_context.cursor to read changes since a previous startup in the same scope.")]
COMPACT_ARG = Annotated[bool, Field(description="Return the compact startup bundle variant.")]
TIMESTAMP_ARG = Annotated[str, Field(description="ISO timestamp to reconstruct around.")]
WINDOW_HOURS_ARG = Annotated[int, Field(description="Search window in hours around the timestamp.")]
SNAPSHOT_DETAIL_ARG = Annotated[
    str,
    Field(
        description=(
            "How much of each snapshot to return: 'digest' (default) drops the "
            "capture-time copies of the ledger and semantic recall, 'full' returns "
            "the stored payload verbatim."
        )
    ),
]
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
_LOGGER = logging.getLogger("max_chronicle.mcp")
_PROCESS_STARTED_AT = time.time()

# Writers get a single slot (SQLite is single-writer anyway, and queueing here
# beats surfacing `database is locked` to agents); readers get 8 so a slow read
# cannot monopolise the pool. A CapacityLimiter belongs to the event loop that
# created it, so these live in RunVars — one set per async run. A plain module
# global would hand a limiter from a dead loop to a new one (the daemon has a
# single loop, but tests spin up an asyncio.run per case).
_LIMITER_TOKENS = {"read": 8, "write": 1}
_limiter_vars = {kind: RunVar(f"chronicle_{kind}_limiter") for kind in _LIMITER_TOKENS}


def _get_limiter(kind: str) -> anyio.CapacityLimiter:
    run_var = _limiter_vars[kind]
    try:
        return run_var.get()
    except LookupError:
        limiter = anyio.CapacityLimiter(_LIMITER_TOKENS[kind])
        run_var.set(limiter)
        return limiter


def _gate_session(ctx: Context | None) -> Any | None:
    """Return the live session object for gate keying, or None outside a request."""
    if ctx is None:
        return None
    try:
        return ctx.session
    except Exception:
        return None


def _envelope_error(
    tool_name: str,
    *,
    error_type: str,
    error: str,
    retryable: bool,
    hint: str | None = None,
    **extra: Any,
) -> ToolError:
    """Build a ToolError whose message is the machine-readable failure envelope.

    Raised rather than returned: a returned dict comes back as a *successful*
    tool call (isError stays false), so a failed write could be mistaken for a
    recorded one — unacceptable in the system of record. Raising sets isError
    while the JSON body keeps error_type/retryable/hint actionable. FastMCP
    prefixes the message with "Error executing tool <name>: ", so parse from
    the first '{'.
    """
    payload: dict[str, Any] = {
        "status": "error",
        "tool": tool_name,
        "error_type": error_type,
        "error": error,
        "retryable": retryable,
    }
    if hint:
        payload["hint"] = hint
    payload.update(extra)
    return ToolError(json.dumps(payload, ensure_ascii=False))


def _offload(fn, *, tool_name: str, writes: bool | Callable[[dict[str, Any]], bool]):
    """Run a sync tool body in a worker thread and translate failures to envelopes.

    mcp 1.26 executes plain `def` tools directly on the event loop, so one slow
    body (git capture, Ollama embed, cold `uv run`) used to freeze every session
    at once — clients saw 30s connection timeouts while launchd saw a healthy
    process. Failures raise a ToolError whose message is the JSON envelope:
    raising sets isError (a returned dict would look like a successful call, so
    a failed write could be mistaken for a recorded one) while the body keeps
    error_type/retryable/hint machine-readable for agents.
    """

    @functools.wraps(fn)
    async def wrapper(**kwargs: Any) -> Any:
        # `writes` may be a predicate: startup_bundle only mutates when the
        # caller asks for a capture, and must take the write slot when it does.
        mutating = writes(kwargs) if callable(writes) else writes
        limiter = _get_limiter("write" if mutating else "read")
        try:
            return await anyio.to_thread.run_sync(functools.partial(fn, **kwargs), limiter=limiter)
        except ToolError as exc:
            message = str(exc)
            if "startup_required" in message:
                raise _envelope_error(
                    tool_name,
                    error_type="startup_required",
                    error=message,
                    retryable=True,
                    hint=(
                        "Call `startup_bundle` for this domain, then retry this call. "
                        "If you already called it in this session, the Chronicle server "
                        "restarted and cleared the in-memory gate — calling it again is safe."
                    ),
                    server_uptime_s=round(time.time() - _PROCESS_STARTED_AT),
                ) from exc
            raise
        except sqlite3.OperationalError as exc:
            text = str(exc)
            locked = "locked" in text.lower() or "busy" in text.lower()
            _LOGGER.warning("tool %s failed with OperationalError: %s", tool_name, text)
            raise _envelope_error(
                tool_name,
                error_type="db_locked" if locked else "db_error",
                error=text,
                retryable=locked,
                hint=(
                    "Chronicle DB is briefly locked by a maintenance job; wait a few seconds and retry."
                    if locked
                    else "Non-transient SQLite error — inspect the Chronicle DB before retrying."
                ),
            ) from exc
        except ValueError as exc:
            raise _envelope_error(
                tool_name,
                error_type="invalid_argument",
                error=str(exc),
                retryable=False,
                hint=str(exc),
            ) from exc
        except Exception as exc:  # noqa: BLE001 — every failure gets an envelope
            _LOGGER.exception("tool %s crashed", tool_name)
            raise _envelope_error(
                tool_name,
                error_type=type(exc).__name__,
                error=str(exc),
                retryable=False,
            ) from exc

    return wrapper


def _offload_read(fn):
    """Thread-offload for resources and prompts — no envelope, errors propagate."""

    @functools.wraps(fn)
    async def wrapper(**kwargs: Any) -> Any:
        return await anyio.to_thread.run_sync(functools.partial(fn, **kwargs), limiter=_get_limiter("read"))

    return wrapper


_AGENT_SOLO_ALIASES = {"operator": "claude"}


def _normalize_agent(agent: str | None) -> str:
    """Collapse ad-hoc agent spellings to a canonical actor name.

    Keeps the actor column analyzable: 25+ historical variants (claude-mac,
    claude-sprint4-night, Codex, opencode-glm5.2, ...) all meant one of a few
    actors. Session flavor belongs in why/text, not in the actor id.
    """
    a = (agent or "mcp").strip().lower()
    if a in _AGENT_SOLO_ALIASES:
        return _AGENT_SOLO_ALIASES[a]
    if a.startswith("claude"):
        return "claude"
    if "glm" in a:
        return "glm"
    if "deepseek" in a:
        return "deepseek"
    if a.startswith("codex"):
        return "codex"
    if a.startswith("gemini"):
        return "gemini"
    if a.startswith("opencode"):
        return "opencode"
    if a.startswith("transcript-analyst"):
        return "transcript-analyst"
    return a


def _startup_required_message(tool_name: str, *, domain: str = "global") -> str:
    return (
        f"startup_required: `{tool_name}` requires Chronicle startup in this session. "
        f"Call `startup_bundle(domain=\"{domain}\")` first — it unlocks the write surface "
        f"and returns the startup brief (`activate_agent(domain=\"{domain}\")` also unlocks). "
        "If you already called it in this session, the Chronicle server has restarted "
        "since; calling it again is safe and re-unlocks. "
        "No Chronicle mutation was performed."
    )


def _audit_summary(audit: dict | None) -> dict | None:
    """Verdict and warnings only — `sources_audit` serves the per-source detail.

    The full audit repeats every source's metadata, and its warnings are
    already spelled out at the end of the activation prompt.
    """
    if not audit:
        return audit
    return {key: audit[key] for key in ("status", "issue_count", "warnings", "issues") if key in audit}


def build_server(manifest_path: Path | None = None, *, profile: str = CHRONICLER_PROFILE) -> FastMCP:
    if profile not in MCP_PROFILES:
        raise ValueError(f"Unsupported MCP profile: {profile}")
    resolved_manifest_path = manifest_path or default_manifest_path()

    def manifest() -> dict:
        return load_manifest_cached(resolved_manifest_path)

    # Gate state: WeakSet of live session objects (pruned automatically when a
    # transport drops its session — the old id()-keyed set leaked forever and a
    # recycled memory address could spuriously unlock a fresh session). Tool
    # bodies run in worker threads now, so mutations go through a lock.
    gate_lock = threading.Lock()
    unlocked_sessions: weakref.WeakSet = weakref.WeakSet()
    identities: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
    fallback_identity = {"session_id": str(uuid.uuid4()), "agent": "mcp"}
    sessionless_unlocked = False

    def session_identity(ctx: Context | None, agent: str = "mcp") -> dict:
        session = _gate_session(ctx)
        with gate_lock:
            if session is None:
                identity = fallback_identity
            else:
                if session not in identities:
                    identities[session] = {"session_id": str(uuid.uuid4()), "agent": "mcp"}
                identity = identities[session]
            if agent != "mcp":
                identity["agent"] = _normalize_agent(agent)
            return dict(identity)

    def unlock_startup_gate(ctx: Context | None) -> None:
        nonlocal sessionless_unlocked
        session = _gate_session(ctx)
        with gate_lock:
            if session is not None:
                unlocked_sessions.add(session)
            else:
                sessionless_unlocked = True

    def require_startup_gate(ctx: Context | None, *, tool_name: str, domain: str = "global") -> None:
        session = _gate_session(ctx)
        with gate_lock:
            if session is not None:
                if session in unlocked_sessions:
                    return
            elif sessionless_unlocked:
                return
        raise ToolError(_startup_required_message(tool_name, domain=domain))

    server = FastMCP(
        name="Max Chronicle" if profile == CHRONICLER_PROFILE else "Max Chronicle Read Only",
        instructions=(
            "Chronicle is the canonical local-first memory system for this installation. "
            "Use Chronicle DB as truth, status markdown as readable projections, and Mem0 as semantic recall. "
            "Session protocol: call `startup_bundle` ONCE at session start — it returns the brief "
            "and unlocks the write surface (read tools work without it but do not unlock). "
            "Recall: `query_memory` is the primary search; `query_context` adds status-markdown and "
            "mem0-dump context; `recent_events` is the cheap latest-N feed; `state_at` reconstructs "
            "a moment in time. Write durable facts with `record_event`; use `entity_admin` for entity maintenance. "
            "Task handoffs: Use the same project/task_id across agents. Startup returns "
            "task_context (checkpoint, current_facts, changes, cursor); since resumes its change feed. "
            "Record a checkpoint at handoff with completed work, actual verification, unknowns, and next steps. "
            "Use request_id for write retries and fact.supersedes to replace an explicit current fact. "
            "A database record is an attributed assertion, not proof of correctness; inspect evidence and fact.kind. "
            "A failing tool raises, so the call is flagged isError and the "
            "message carries a JSON envelope after the FastMCP prefix — parse from the first '{': "
            "`status`, `error_type`, `retryable`, `hint`. Follow the hint instead of giving up. "
            "Renamed 2026-08-13 (older agents may hold the previous names): "
            "normalize_entities -> entity_admin(action=\"normalize\"); "
            "add_entity_alias -> entity_admin(action=\"alias\"); "
            "merge_entities -> entity_admin(action=\"merge\"); "
            "entity_resolution_report -> entity_admin(action=\"report\"); "
            "render_projections -> capture_snapshot; activate_agent -> startup_bundle."
        ),
        log_level="INFO",
    )

    def register_tool(
        *,
        name: str,
        description: str,
        writes: bool | Callable[[dict[str, Any]], bool],
        structured_output: bool | None = None,
    ):
        """Register a sync tool body wrapped in the thread-offload + envelope layer."""

        def decorator(fn):
            tool_kwargs: dict[str, Any] = {"name": name, "description": description}
            if structured_output is not None:
                tool_kwargs["structured_output"] = structured_output
            server.tool(**tool_kwargs)(_offload(fn, tool_name=name, writes=writes))
            return fn

        return decorator

    def register_resource(uri: str, *, title: str, description: str):
        def decorator(fn):
            server.resource(uri, title=title, description=description)(_offload_read(fn))
            return fn

        return decorator

    def register_prompt(*, name: str, description: str):
        def decorator(fn):
            server.prompt(name=name, description=description)(_offload_read(fn))
            return fn

        return decorator

    @server.custom_route("/health", methods=["GET"])
    async def route_health(_: Request) -> JSONResponse:
        # Lives on the Starlette app, outside the MCP session machinery: it
        # answers even when sessions are wedged, and hangs together with the
        # event loop — exactly the signal the external watchdog polls for.
        def _db_check() -> dict[str, Any]:
            config = config_from_manifest(manifest())
            with open_connection(config) as connection:
                connection.execute("SELECT 1").fetchone()
                schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            return {
                "db_path": str(config.db_path),
                "manifest_path": str(resolved_manifest_path),
                "schema_version": schema_version,
                "target_schema_version": target_schema_version(config),
            }

        db_ok = False
        details: dict[str, Any] = {}
        error: str | None = None
        try:
            with anyio.fail_after(2.0):
                details = await anyio.to_thread.run_sync(_db_check, abandon_on_cancel=True)
            db_ok = True
        except Exception as exc:  # noqa: BLE001 — health must always answer
            error = f"{type(exc).__name__}: {exc}"
        payload: dict[str, Any] = {
            "status": "ok" if db_ok else "degraded",
            "db_ok": db_ok,
            "uptime_s": round(time.time() - _PROCESS_STARTED_AT),
            "version": __version__,
            "pid": os.getpid(),
            "profile": profile,
            # Where the running code was imported from: a stale checkout on
            # sys.path shadowing the installed release shows up here.
            "module_path": str(Path(__file__).resolve().parent),
            **details,
        }
        if error:
            payload["error"] = error
        return JSONResponse(payload, status_code=200 if db_ok else 503)

    @register_resource(
        "chronicle://attach/current",
        title="Current Attach Bundle",
        description="Machine-readable agent attach bundle for the global domain.",
    )
    def resource_attach_current() -> dict:
        return build_attach_bundle(manifest(), domain_id="global", agent="mcp-resource", capture=False)

    @register_resource(
        "chronicle://attach/domain/{domain}",
        title="Domain Attach Bundle",
        description="Machine-readable agent attach bundle for a domain.",
    )
    def resource_attach_domain(domain: str) -> dict:
        return build_attach_bundle(manifest(), domain_id=domain, agent="mcp-resource", capture=False)

    @register_resource(
        "chronicle://state/current",
        title="Current Chronicle State",
        description="Database summary, latest snapshot, and recent durable events.",
    )
    def resource_state_current() -> dict:
        return current_state(manifest(), domain="global")

    @register_resource(
        "chronicle://state/domain/{domain}",
        title="Domain Chronicle State",
        description="Latest snapshot and recent events for a domain.",
    )
    def resource_state_domain(domain: str) -> dict:
        return current_state(manifest(), domain=domain)

    @register_resource(
        "chronicle://sources/audit",
        title="Chronicle Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for the global domain.",
    )
    def resource_sources_audit() -> dict:
        return build_sources_audit(manifest(), domain_id="global")

    @register_resource(
        "chronicle://sources/audit/{domain}",
        title="Domain Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for a domain.",
    )
    def resource_sources_audit_domain(domain: str) -> dict:
        return build_sources_audit(manifest(), domain_id=domain)

    @register_resource(
        "chronicle://timeline/{timestamp}",
        title="Chronicle Timeline",
        description="Nearest global snapshots and events around a timestamp.",
    )
    def resource_timeline(timestamp: str) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(loaded, timestamp=target, domain="global", window_hours=6, limit=3)

    @register_resource(
        "chronicle://timeline/domain/{domain}/{timestamp}",
        title="Domain Timeline",
        description="Nearest snapshots and events around a timestamp for a domain.",
    )
    def resource_timeline_domain(domain: str, timestamp: str) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(loaded, timestamp=target, domain=domain, window_hours=6, limit=3)

    @register_resource(
        "chronicle://project/{project}",
        title="Project State",
        description="Recent events and relations for a project slug.",
    )
    def resource_project(project: str) -> dict:
        return project_state(manifest(), project=project)

    @register_resource(
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

    @register_tool(
        # Reads by default, but capture=true persists a snapshot and rewrites
        # projections — that has to serialize behind the write limiter.
        writes=lambda kwargs: bool(kwargs.get("capture")),
        name="startup_bundle",
        description=(
            "THE session entry point — call once at session start. Returns the startup brief "
            "(domain state, recent durable events, freshness audit) and unlocks the chronicler "
            "write surface for this session. Does not mutate Chronicle unless capture=true."
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
        project: PROJECT_ARG = None,
        task_id: TASK_ARG = None,
        since: CURSOR_ARG = None,
        ctx: Context | None = None,
    ) -> dict:
        effective_capture = capture if profile == CHRONICLER_PROFILE else False
        payload = build_startup_bundle(
            manifest(),
            domain_id=domain,
            agent=_normalize_agent(agent),
            title=title,
            focus=focus,
            capture=effective_capture,
            limit=limit,
            compact=compact,
            project=project,
            task_id=task_id,
            since=since,
        )
        payload.update(session_identity(ctx, agent))
        unlock_startup_gate(ctx)
        return payload

    @register_tool(
        writes=False,
        name="recent_events",
        description=(
            "Cheap latest-N feed of Chronicle events for a domain — situational awareness "
            "without search. For 'what do we know about X' use query_memory instead."
        ),
    )
    def tool_recent_events(domain: DOMAIN_ARG = "global", limit: LIMIT_ARG = 10) -> list[dict]:
        loaded = manifest()
        config = config_from_manifest(loaded)
        return fetch_recent_events(config, limit=limit, domain=domain, visibility="raw")

    @register_tool(
        writes=False,
        name="state_at",
        description=(
            "Timeline archaeology: reconstruct what was true around an ISO timestamp "
            "(nearest snapshots + events in a window). Use for 'what was happening on <date>'; "
            "for topic search use query_memory. Snapshots come back digested — their "
            "capture-time copies of the ledger and of semantic recall are replaced by a "
            "count, since live recall serves those better; pass detail=\"full\" to get the "
            "stored payload verbatim (tens of KB per snapshot)."
        ),
    )
    def tool_state_at(
        timestamp: TIMESTAMP_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        window_hours: WINDOW_HOURS_ARG = 6,
        limit: LIMIT_ARG = 3,
        detail: SNAPSHOT_DETAIL_ARG = "digest",
    ) -> dict:
        loaded = manifest()
        target = parse_when(timestamp, loaded)
        return reconstruct_timeline(
            loaded,
            timestamp=target,
            domain=domain,
            window_hours=window_hours,
            limit=limit,
            detail=detail,
        )

    @register_tool(
        writes=False,
        name="query_context",
        description=(
            "Broad context sweep: searches Chronicle events PLUS status-markdown sections and the "
            "mem0 dump, with truth-layer ordering. Use when you need document/status context around "
            "a topic; for pure event recall query_memory is faster and ranks better."
        ),
    )
    def tool_query_context(
        query: QUERY_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        limit: LIMIT_ARG = 5,
        mode: QUERY_MODE_ARG = DEFAULT_QUERY_MODE,
    ) -> dict:
        return query_context(manifest(), query=query, domain=domain, limit=limit, mode=mode)

    @register_tool(
        writes=False,
        name="query_memory",
        description=(
            "PRIMARY recall — start here for 'what do we know about X'. RRF-fused FTS + vector + "
            "temporal search over Chronicle events; works offline (FTS + temporal) when Ollama is "
            "down, adds the vector channel automatically when it is up. The `degraded` flag reports "
            "skipped channels. Independent of the external Mem0/Qdrant stack."
        ),
    )
    def tool_query_memory(
        query: QUERY_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        limit: LIMIT_ARG = 10,
        project: PROJECT_ARG = None,
        task_id: TASK_ARG = None,
    ) -> dict:
        return query_memory(manifest(), query=query, domain=domain, limit=limit,
                            project=project, task_id=task_id)

    @register_tool(
        writes=False,
        name="sources_audit",
        description="Inspect source coverage, lane enablement, freshness, and trust metadata.",
    )
    def tool_sources_audit(domain: DOMAIN_ARG = "global") -> dict:
        return build_sources_audit(manifest(), domain_id=domain)

    if profile == CHRONICLER_PROFILE:
        @register_tool(
            writes=True,
            name="activate_agent",
            description=(
                "DEPRECATED alias — prefer `startup_bundle` as the single session entry point. "
                "Kept for transition: captures current state (capture=true by default), unlocks "
                "the chronicler write surface, and returns the activation prompt plus a digest of "
                "runtime state. Call `startup_bundle` for the full brief."
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
            activation = build_activation(
                manifest(),
                domain_id=domain,
                agent=_normalize_agent(agent),
                title=title,
                focus=focus,
                capture=capture,
            )
            # The full activation payload is ~130 KB: the attach bundle carries the
            # verbatim text of every status document (~87 KB) and the snapshot carries
            # capture-time copies of the ledger and semantic recall (~40 KB). Handing
            # that to an agent at session start costs more context than the session
            # has to spend, and it is the entry point older agents still call by this
            # name. What this tool is actually for is the prompt and the unlock; the
            # documents are served by `startup_bundle`, `query_context` and the
            # resources. The CLI keeps the unabridged payload.
            snapshot = activation.get("snapshot") or {}
            payload = {
                "contract_name": activation.get("contract_name"),
                "contract_version": activation.get("contract_version"),
                "prompt": activation.get("prompt"),
                "snapshot": digest_snapshot(snapshot),
                "freshness_audit": _audit_summary(
                    (activation.get("attach_bundle") or {}).get("freshness_audit")
                ),
                "deprecated": {
                    "superseded_by": "startup_bundle",
                    "note": (
                        "Returns the activation prompt and a runtime digest. For the full "
                        "brief call `startup_bundle`; for document text use `query_context`."
                    ),
                },
            }
            unlock_startup_gate(ctx)
            return payload

        @register_tool(
            writes=True,
            name="record_event",
            description=(
                "Write a durable Chronicle event and archive linked source files. "
                "Unknown-but-well-formed categories are stored as `note` with a "
                "`category_fallback` report instead of failing. "
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
            task_id: TASK_ARG = None,
            session_id: SESSION_ARG = None,
            request_id: REQUEST_ARG = None,
            checkpoint: Annotated[Checkpoint | None, Field(description="Structured handoff for the next agent; requires project/task_id.")] = None,
            fact: Annotated[FactInput | None, Field(description="Explicit current assertion. Changing a slot requires the previous fact ID in supersedes.")] = None,
            ctx: Context | None = None,
        ) -> dict:
            require_startup_gate(ctx, tool_name="record_event", domain=domain)
            identity = session_identity(ctx, agent)
            return record_event(
                manifest(),
                {
                    "agent": identity["agent"],
                    "task_id": task_id,
                    "session_id": session_id or identity["session_id"],
                    "request_id": request_id,
                    "checkpoint": checkpoint.model_dump() if checkpoint else None,
                    "fact": fact.model_dump() if fact else None,
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

        @register_tool(
            writes=True,
            name="capture_snapshot",
            description=(
                "Capture live runtime state into Chronicle, archive evidence, and refresh the "
                "markdown projections (status.md and friends — the former render_projections tool "
                "is folded in here). "
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
            require_startup_gate(ctx, tool_name="capture_snapshot", domain=domain)
            captured = capture_runtime_snapshot(
                manifest(),
                domain_id=domain,
                agent=_normalize_agent(agent),
                title=title,
                focus=focus,
            )
            # Agents are told to capture before a handoff or a context switch —
            # exactly when context is scarcest. Echoing the whole stored snapshot
            # back (~30 KB: the embedded ledger copy, document excerpts, and every
            # normalized entity with its full source_refs list) spends that budget
            # on data the caller just supplied or can read back on demand. What a
            # caller needs here is confirmation: the id, where it landed, and what
            # was written. The CLI still prints the full payload.
            payload = digest_snapshot(captured)
            entities = payload.get("normalized_entities")
            if isinstance(entities, list):
                payload["normalized_entities"] = {
                    "count": len(entities),
                    "ids": [entity.get("id") for entity in entities],
                }
            return payload

        @register_tool(
            writes=True,
            name="entity_admin",
            description=(
                "Entity maintenance multiplexer — one tool for the rare admin operations. "
                "action='report': read-only QA (alias/entity counts, fragmentation candidates like "
                "Ivan/Ivan/Иван needing a merge). "
                "action='normalize': materialize normalized entities from Chronicle truth. "
                "action='alias': register an alternate spelling for a normalized entity "
                "(requires alias_text + canonical_entity_id; NFKC+casefold key, so Ivan/IVAN/Иван "
                "collapse into one active row per domain+type). "
                "action='merge': fold one normalized entity into another (requires source_entity_id "
                "+ target_entity_id + reason; aliases re-pointed, source marked inactive, v7 truth "
                "tables untouched). "
                f"Mutating actions only: {STARTUP_GATE_DESCRIPTION}"
            ),
        )
        def tool_entity_admin(
            action: Annotated[
                Literal["report", "normalize", "alias", "merge"],
                Field(description="Which entity operation to run."),
            ],
            domain: Annotated[
                str | None,
                Field(description="Domain scope for report/normalize/alias ('global' default where relevant)."),
            ] = None,
            alias_text: Annotated[str | None, Field(description="alias: human-facing alias text (e.g., 'Иван', 'Ivan').")] = None,
            canonical_entity_id: Annotated[str | None, Field(description="alias: target normalized_entities.id (e.g., 'person:ivan').")] = None,
            entity_type: Annotated[str, Field(description="alias: entity type ('person','company','project',…).")] = "person",
            source: Annotated[str | None, Field(description="alias: origin label ('manual','llm','ingest').")] = "manual",
            confidence: Annotated[float, Field(description="alias: confidence in [0..1].")] = 1.0,
            alias_key_kind: Annotated[str, Field(description="alias: 'base' | 'compact' | 'translit' | 'diacritic_fold'.")] = "base",
            source_entity_id: Annotated[str | None, Field(description="merge: normalized_entities.id to retire.")] = None,
            target_entity_id: Annotated[str | None, Field(description="merge: normalized_entities.id to keep.")] = None,
            reason: Annotated[str | None, Field(description="merge: why — stored in source metadata.")] = None,
            actor: Annotated[str | None, Field(description="merge: operator/agent that triggered the merge.")] = None,
            dry_run: Annotated[bool, Field(description="alias/merge: preview without writing.")] = False,
            ctx: Context | None = None,
        ) -> dict:
            if action == "report":
                report = entity_resolution_report_service(manifest(), domain=domain)
                return {**report, "action": "report"}
            require_startup_gate(ctx, tool_name="entity_admin", domain=domain or "global")
            if action == "normalize":
                resolved_domain = domain or "global"
                entities = materialize_normalized_entities(manifest(), domain_id=resolved_domain)
                return {
                    "action": "normalize",
                    "domain": resolved_domain,
                    "normalized_entities": entities,
                    "count": len(entities),
                }
            if action == "alias":
                if not alias_text or not canonical_entity_id:
                    raise ValueError("action='alias' requires alias_text and canonical_entity_id.")
                result = add_entity_alias_service(
                    manifest(),
                    alias_text=alias_text,
                    canonical_entity_id=canonical_entity_id,
                    entity_type=entity_type,
                    domain=domain or "global",
                    source=source,
                    confidence=confidence,
                    alias_key_kind=alias_key_kind,
                    dry_run=dry_run,
                )
                return {**result, "action": "alias", "alias_action": result.get("action")}
            if action == "merge":
                if not source_entity_id or not target_entity_id or not reason:
                    raise ValueError("action='merge' requires source_entity_id, target_entity_id, and reason.")
                result = merge_entities_service(
                    manifest(),
                    source_entity_id=source_entity_id,
                    target_entity_id=target_entity_id,
                    reason=reason,
                    actor=actor,
                    dry_run=dry_run,
                )
                return {**result, "action": "merge"}
            raise ValueError(f"Unknown entity_admin action: {action}")

        @register_tool(
            writes=False,
            name="search_mem0_live",
            description=(
                "Live semantic search over Mem0 (Qdrant + Gemini embeddings) via scripts/mem0_bridge.py. "
                "Fail-closed: timeouts, non-zero exits, or unparseable output return status='degraded' with "
                "results=[] instead of raising, so Chronicle stays usable when Mem0 is down."
            ),
        )
        def tool_search_mem0_live(
            query: Annotated[str, Field(description="Semantic query text.")],
            limit: Annotated[int, Field(description="Max results (default 10).")] = 10,
            collection: Annotated[Literal["personal", "digest", "both"], Field(description="Which Mem0 collection to query.")] = "personal",
            category: Annotated[str | None, Field(description="Optional metadata.category filter.")] = None,
            timeout_s: Annotated[float | None, Field(description="Override bridge timeout (seconds).")] = None,
        ) -> dict:
            return search_mem0_live_service(
                manifest(),
                query=query,
                limit=limit,
                collection=collection,
                category=category,
                timeout_s=timeout_s,
            )

    @register_prompt(
        name="activate",
        description="Return the current activation prompt for a new agent.",
    )
    def prompt_activate(domain: str = "global") -> list[dict]:
        activation = build_activation(manifest(), domain_id=domain, agent="mcp-prompt", capture=False)
        return [{"role": "user", "content": activation["prompt"]}]

    @register_prompt(
        name="continue_work",
        description="Provide a compact continuation brief for ongoing work.",
    )
    def prompt_continue_work(domain: str = "global") -> list[dict]:
        state = build_startup_bundle(manifest(), domain_id=domain, agent="mcp-prompt", capture=False, limit=6, compact=True)
        content = json.dumps(state, ensure_ascii=False, indent=2)
        return [{"role": "user", "content": f"Continue from this Chronicle state:\n{content}"}]

    @register_prompt(
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
    # Resolved in _main: computing it here made `--help` fail whenever the
    # default workspace could not be resolved (CHRONICLE_REQUIRE_ROOT).
    parser.add_argument("--manifest", type=Path, default=None, help="Path to SSOT_MANIFEST.toml (default: the workspace's)")
    parser.add_argument(
        "--migrate",
        action="store_true",
        help=(
            "Upgrade a database that is behind this code at start, after an online backup. "
            "Without it the server refuses to start on an outdated schema."
        ),
    )
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    # Prepare the schema before serving. Only a launcher that passes --migrate
    # upgrades an outdated database (after an online backup): a stdio server
    # started by some client from some checkout must not. A read-only server
    # only checks. Serving against a wrong schema would fail every call, so
    # refuse to start and say why.
    read_only = args.profile == READ_ONLY_PROFILE
    try:
        manifest_path = args.manifest or default_manifest_path()
        config = config_from_manifest(load_manifest(manifest_path))
        prepared = prepare_database(config, allow_upgrade=args.migrate and not read_only, read_only=read_only)
    except (ChronicleConfigError, MigrationError, sqlite3.Error, OSError, ValueError) as exc:
        _LOGGER.error("chronicle-mcp cannot start: %s: %s", type(exc).__name__, exc)
        # The same exit codes as the CLI, so launchers and logs read them alike.
        if isinstance(exc, ChronicleConfigError):
            return EXIT_CONFIG_ERROR
        return EXIT_SCHEMA_ACTION if isinstance(exc, MigrationError) else 1
    if read_only:
        set_read_only_process(True)
    if prepared.applied:
        _LOGGER.warning(
            "chronicle-mcp migrated %s from v%d to v%d; backup=%s",
            config.db_path,
            prepared.state_before.current_version,
            max(item.version for item in prepared.applied),
            prepared.backup_path,
        )
    server = build_server(manifest_path, profile=args.profile)
    endpoint = args.transport
    if args.transport in ("sse", "streamable-http"):
        host = os.environ.get("MCP_HOST", "127.0.0.1")
        port = int(os.environ.get("MCP_PORT", "8000"))
        allowed_hosts = [
            host,
            f"{host}:*",
            "127.0.0.1",
            "127.0.0.1:*",
            "localhost",
            "localhost:*",
            "[::1]",
            "[::1]:*",
        ]
        allowed_hosts.extend(
            item.strip() for item in os.environ.get("MCP_ALLOWED_HOSTS", "").split(",") if item.strip()
        )
        server.settings.host = host
        server.settings.port = port
        server.settings.transport_security = TransportSecuritySettings(allowed_hosts=allowed_hosts)
        endpoint = f"{args.transport} {host}:{port}"
    # Startup banner: with no banner and WARNING-level logs, restarts were
    # invisible — an empty err.log looked like health when it proved nothing.
    _LOGGER.info(
        "chronicle-mcp starting: version=%s pid=%d profile=%s transport=%s db=%s manifest=%s module=%s",
        __version__,
        os.getpid(),
        args.profile,
        endpoint,
        config.db_path,
        manifest_path,
        Path(__file__).resolve().parent,
    )
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
