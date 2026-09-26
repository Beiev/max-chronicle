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
import re
from anyio.lowlevel import RunVar
from mcp.server.fastmcp import Context, FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse

from . import __version__

from .config import EXIT_CONFIG_ERROR, EXIT_SCHEMA_ACTION, ChronicleConfigError, default_manifest_path
from .db import MigrationError
from .runtime_context import load_manifest, load_manifest_cached, parse_when
from .brief import BRIEF_DEFAULT_CHARS, build_brief
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
from .identity import UNKNOWN_AGENT, Registry
from .notes import note_settings, read_note, sync_notes
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
    "pipeline id. The registry maps other spellings (claude-<session>, Codex, "
    "opencode-glm*) to the canonical actor and keeps the given one as actor_raw; "
    "put session context in why/text instead. Omitted, the session's agent or "
    "the MCP client's name is used."
))]
OPTIONAL_TITLE_ARG = Annotated[str | None, Field(description="Optional snapshot title.")]
OPTIONAL_FOCUS_ARG = Annotated[str | None, Field(description="Optional focus string.")]
STARTUP_MODE_ARG = Annotated[
    Literal["bundle", "brief"],
    Field(description="bundle: the full startup bundle; brief: only the compact memory brief (FR-12), cheaper."),
]
BRIEF_TIMEOUT_S = 2.0  # a session-start hook waits for this, so it must stay short
# /brief is for processes on this machine. A browser page must not read it: its
# fetch always sends Origin, and DNS rebinding arrives with a foreign Host (O4).
_LOOPBACK_HOST = re.compile(r"(127\.0\.0\.1|localhost|\[::1\])(:\d{1,5})?")
CAPTURE_ARG = Annotated[
    bool,
    Field(description="Capture a fresh runtime snapshot before building the bundle."),
]
LIMIT_ARG = Annotated[int, Field(ge=1, le=100, description="Maximum number of recent items or hits (1–100).")]
TASK_ARG = Annotated[str | None, Field(description="Stable task ID within project. Requires project; reuse it across agents and sessions. Case, spaces and underscores do not matter.")]
SESSION_ARG = Annotated[str | None, Field(description="Originating session ID; defaults to the current MCP session identity.")]
REQUEST_ARG = Annotated[str | None, Field(description="Unique write request ID. Reuse unchanged on retries; changed input requires a new ID.")]
CURSOR_ARG = Annotated[str | None, Field(description="Cursor from task_context.cursor to read changes since a previous startup in the same scope.")]
BEFORE_ARG = Annotated[str | None, Field(description="Cursor from task_context.before to read the older changes of the same scope; exclusive with since.")]
COMPACT_ARG = Annotated[bool, Field(description="Return the compact startup bundle variant.")]
TIMESTAMP_ARG = Annotated[str, Field(description="ISO timestamp to reconstruct around.")]
WINDOW_HOURS_ARG = Annotated[int, Field(ge=1, le=720, description="Search window in hours around the timestamp (1–720).")]
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
TIMELINE_MODE_ARG = Annotated[
    Literal["around", "as_of"],
    Field(description=(
        "around: the window on both sides of the timestamp. as_of: only what Chronicle knew at "
        "the timestamp, plus the facts current then."
    )),
]
QUERY_ARG = Annotated[str, Field(description="Search string matched across Chronicle events, status markdown sections and the Mem0 dump.")]
RECALL_QUERY_ARG = Annotated[str, Field(description="What to recall: words or a question, in any language.")]
QUERY_MODE_ARG = Annotated[
    QUERY_CONTEXT_MODE,
    Field(description=(
        "truth_only: Chronicle events and status sources only. The other modes add the Mem0 dump and "
        "matching entities; their interpretation and scenario layers are retired and stay empty."
    )),
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
PROJECT_ARG = Annotated[str | None, Field(description="Optional project slug; any spelling the manifest registers for it finds the same project.")]
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


def _client_name(ctx: Context | None) -> str | None:
    """The name the MCP client gave in its initialize request, if any."""
    session = _gate_session(ctx)
    try:
        name = session.client_params.clientInfo.name
    except AttributeError:
        return None
    return name if isinstance(name, str) and name.strip() else None


def _public_identity(identity: dict) -> dict:
    return {"session_id": identity["session_id"], "agent": identity["agent"]}


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
    fallback_identity = {"session_id": str(uuid.uuid4()), "agent": UNKNOWN_AGENT}
    sessionless_unlocked = False

    def registry() -> Registry:
        return Registry.from_manifest(manifest())

    def session_identity(ctx: Context | None, agent: str = UNKNOWN_AGENT) -> dict:
        """The session's id and agent; an *agent* named here becomes the session's.

        A session that never names its agent is attributed to its MCP client
        through the registry (clientInfo.name, such as claude-code), not to an
        anonymous "mcp" (W5). ``agent_raw`` keeps the spelling given.
        """
        session = _gate_session(ctx)
        names = registry()
        with gate_lock:
            if session is None:
                identity = fallback_identity
            else:
                if session not in identities:
                    identities[session] = {"session_id": str(uuid.uuid4()), "agent": UNKNOWN_AGENT}
                identity = identities[session]
            if agent != UNKNOWN_AGENT:
                identity.update(agent=names.agent(agent), agent_raw=agent, named=True)
            elif not identity.get("named") and "agent_raw" not in identity:
                client = _client_name(ctx)
                if client:
                    identity.update(agent=names.agent(client), agent_raw=client)
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
            "task_context (checkpoint, current_facts, changes, cursor); since resumes its change feed, "
            "before pages back through older changes. Without task_id there is no checkpoint to "
            "resume: task_context lists open_tasks instead; start again with the chosen task. "
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
        destructive: bool = False,
        open_world: bool = False,
    ):
        """Register a sync tool body wrapped in the thread-offload + envelope layer.

        The annotations tell clients what a call may change: a tool that may
        write is not read-only, one that retires or rewrites records is
        destructive, and one that reaches a service outside Chronicle is open.
        """

        def decorator(fn):
            read_only = writes is False
            annotations = ToolAnnotations(
                readOnlyHint=read_only,
                destructiveHint=None if read_only else destructive,
                openWorldHint=open_world,
            )
            tool_kwargs: dict[str, Any] = {"name": name, "description": description, "annotations": annotations}
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

    @server.custom_route("/brief", methods=["GET"])
    async def route_brief(request: Request):
        if "origin" in request.headers or not _LOOPBACK_HOST.fullmatch(request.headers.get("host", "")):
            return PlainTextResponse("Forbidden\n", status_code=403)
        params = request.query_params
        try:
            budget = int(params.get("budget", BRIEF_DEFAULT_CHARS))
            with anyio.fail_after(BRIEF_TIMEOUT_S):
                brief = await anyio.to_thread.run_sync(
                    lambda: build_brief(manifest(), cwd=params.get("cwd"), project=params.get("project"),
                                        max_chars=budget),
                    abandon_on_cancel=True,
                )
        except ValueError as exc:
            return PlainTextResponse(f"{exc}\n", status_code=400)
        except Exception as exc:  # noqa: BLE001 — a waiting hook gets an answer, never a hang
            return PlainTextResponse(f"Brief unavailable: {type(exc).__name__}\n", status_code=503)
        if params.get("format") == "json":
            return JSONResponse(brief)
        return PlainTextResponse(brief["text"])

    @register_resource(
        "chronicle://brief/{project}",
        title="Project Memory Brief",
        description="Compact read-only memory brief for a project: open tasks, current facts, recent decisions, warnings.",
    )
    def resource_brief(project: str) -> str:
        return build_brief(manifest(), project=project)["text"]

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
        "chronicle://note/{document_id}",
        title="Indexed Note",
        description="The full indexed (redacted) text of a note that query_memory returned in `notes`.",
    )
    def resource_note(document_id: str) -> dict:
        return read_note(manifest(), document_id) or {"status": "not_found", "document_id": document_id}

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
            "write surface for this session. With project and task_id, task_context holds that "
            "task's checkpoint to resume; without task_id it lists open_tasks instead. "
            "mode='brief' returns only the compact memory brief. "
            "Does not mutate Chronicle unless capture=true."
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
        before: BEFORE_ARG = None,
        mode: STARTUP_MODE_ARG = "bundle",
        ctx: Context | None = None,
    ) -> dict:
        identity = session_identity(ctx, agent)
        if mode == "brief":
            brief = build_brief(manifest(), project=project)
            payload = {"brief": brief["text"], "project": brief["project"], "counts": brief["counts"]}
            payload.update(_public_identity(identity))
            unlock_startup_gate(ctx)
            return payload
        effective_capture = capture if profile == CHRONICLER_PROFILE else False
        payload = build_startup_bundle(
            manifest(),
            domain_id=domain,
            agent=identity["agent"],
            title=title,
            focus=focus,
            capture=effective_capture,
            limit=limit,
            compact=compact,
            project=project,
            task_id=task_id,
            since=since,
            before=before,
        )
        payload.update(_public_identity(identity))
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
        return fetch_recent_events(config, limit=limit, domain=domain)

    @register_tool(
        writes=False,
        name="state_at",
        description=(
            "Timeline archaeology: reconstruct what was true around an ISO timestamp "
            "(nearest snapshots + events in a window). Use for 'what was happening on <date>'; "
            "for topic search use query_memory. mode='as_of' answers 'what did we know then': "
            "only snapshots taken by then, events of the window before it that had been recorded "
            "by then (without their Mem0 sync state), and the facts current then. Snapshots come back digested — their "
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
        mode: TIMELINE_MODE_ARG = "around",
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
            as_of=mode == "as_of",
            visibility="default",  # agents never see quarantined events; the operator's CLI does
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
            "PRIMARY recall — start here for 'what do we know about X'. Fuses full-text and vector "
            "matches over Chronicle events (`results`) and indexed notes (`notes`, the best section "
            "of each; read a whole note at chronicle://note/{document_id}); recency only breaks ties. Works offline on full text "
            "when Ollama is down. `degraded` reports skipped channels, `relaxed` a weaker "
            "word-stem match, and `no_confident_match` that no result is a strong match: treat "
            "those results as leads and verify them. A weak or empty result does not prove the "
            "memory is absent. Independent of the external Mem0/Qdrant stack."
        ),
    )
    def tool_query_memory(
        query: RECALL_QUERY_ARG,
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
                agent=session_identity(ctx, agent)["agent"],
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
            named = agent != UNKNOWN_AGENT
            return record_event(
                manifest(),
                {
                    # The spelling given, canonicalised on write (FR-14); where it
                    # came from decides whether a retry must repeat it (W6).
                    "agent": agent if named else identity.get("agent_raw", identity["agent"]),
                    "agent_source": "explicit" if named else (
                        "session" if identity.get("named") else "client" if "agent_raw" in identity else "default"
                    ),
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
                agent=session_identity(ctx, agent)["agent"],
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
            destructive=True,  # merge retires an entity
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
            open_world=True,
            name="search_mem0_live",
            description=(
                "Live semantic search over the external Mem0 stack through the bridge script the manifest names. "
                "Fail-closed: timeouts, non-zero exits, or unparseable output return status='degraded' with "
                "results=[] instead of raising, so Chronicle stays usable when Mem0 is down."
            ),
        )
        def tool_search_mem0_live(
            query: Annotated[str, Field(description="Semantic query text.")],
            limit: Annotated[int, Field(ge=1, le=100, description="Max results (1–100, default 10).")] = 10,
            collection: Annotated[Literal["personal", "digest", "both"], Field(description="Which Mem0 collection to query.")] = "personal",
            category: Annotated[str | None, Field(description="Optional metadata.category filter.")] = None,
            timeout_s: Annotated[float | None, Field(gt=0, le=120, description="Override bridge timeout (seconds, at most 120).")] = None,
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
        loaded = load_manifest(manifest_path)
        config = config_from_manifest(loaded)
        if not read_only and args.transport != "stdio":  # only such a server syncs notes
            note_settings(loaded)  # a bad [notes] section stops the start, not the sync thread later
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
    if not read_only and args.transport != "stdio":
        _start_notes_sync(manifest_path)
    server.run(transport=args.transport)
    return 0


NOTES_SYNC_FALLBACK_MINUTES = 15  # after a failed or unconfigured round


def _start_notes_sync(manifest_path: Path) -> threading.Thread | None:
    """Keep the note index fresh from a long-running server: sync now, then every [notes] sync_minutes.

    A stdio server lives as long as one client and does not sync; `chronicle
    notes sync` does the same by hand.
    """
    try:
        minutes = note_settings(load_manifest(manifest_path)).sync_minutes
    except (OSError, ValueError) as exc:
        _LOGGER.error("notes sync not started: %s: %s", type(exc).__name__, exc)
        return None
    if not minutes:
        return None

    def loop() -> None:
        minutes = 0
        while True:
            time.sleep(minutes * 60)
            minutes = NOTES_SYNC_FALLBACK_MINUTES
            try:
                loaded = load_manifest_cached(manifest_path)
                minutes = note_settings(loaded).sync_minutes or NOTES_SYNC_FALLBACK_MINUTES
                report = sync_notes(loaded)
                _LOGGER.info("notes sync: %s", {key: report[key] for key in ("indexed", "unchanged", "removed")})
            except Exception:  # noqa: BLE001 - a failed sync must not stop the server or the next sync
                _LOGGER.exception("notes sync failed")

    thread = threading.Thread(target=loop, name="chronicle-notes-sync", daemon=True)
    thread.start()
    return thread


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
