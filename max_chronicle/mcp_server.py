from __future__ import annotations

import argparse
import faulthandler
import functools
import json
import logging
import os
import signal
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
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse

from . import __version__, viewer

from .automation import load_automation_config
from .config import (
    EXIT_CONFIG_ERROR,
    EXIT_SCHEMA_ACTION,
    ChronicleConfigError,
    default_automation_path,
    default_manifest_path,
    feature_enabled,
    mem0_enabled,
)
from .db import MigrationError
from .runtime_context import load_manifest, load_manifest_cached, parse_when
from .brief import BRIEF_DEFAULT_CHARS, build_brief
from .service import (
    DEFAULT_QUERY_MODE,
    ENV_FEATURE_EVENT_EMBEDDINGS,
    add_entity_alias_service,
    build_attach_bundle,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    capture_runtime_snapshot,
    current_state,
    default_mem0_status,
    embed_backfill,
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
from .agent_surface import get_items, index_view
from .liveness import BackgroundEmbedder, LoopMonitor
from .scheduler import Scheduler
from .notes import note_settings, read_note, sync_notes
from .memory import Checkpoint, FactInput
from .memory import task_context as read_task_context

READ_ONLY_PROFILE = "readonly"
CHRONICLER_PROFILE = "chronicler"
# Everything an operator or a curating agent may need: the chronicler's tools
# plus status-document search, source audit, snapshots, entity maintenance and
# the deprecated aliases. Agents' default surface stays small: in a month of
# client logs, 98% of calls were startup_bundle, query_memory and record_event.
ADMIN_PROFILE = "admin"
MCP_PROFILES = {READ_ONLY_PROFILE, CHRONICLER_PROFILE, ADMIN_PROFILE}
WRITE_DESCRIPTION = (
    "No prior call is required. When you continue a task, read its checkpoint with "
    "`startup_bundle(project, task_id)` first."
)
# How long a startup_bundle counts as "this agent has read this project's
# context": writes after it carry no reminder.
STARTUP_NOTICE_TTL_S = 12 * 3600
STARTUP_NOTICE_HINT = (
    "Recorded. This agent has not read this project's memory through this server recently: "
    "call startup_bundle(project=..., task_id=...) to see the current checkpoint, facts and "
    "decisions before you continue."
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
    Literal["brief", "bundle"],
    Field(description="brief (default): the compact memory brief, plus task_context for a task_id; bundle: the full startup bundle."),
]
BRIEF_TIMEOUT_S = 2.0  # a session-start hook waits for this, so it must stay short
VIEW_TIMEOUT_S = 15.0  # a page reads a few queries; recall may wait on the embedding model
# /brief is for processes on this machine. A browser page must not read it: its
# fetch always sends Origin, and DNS rebinding arrives with a foreign Host (O4).
_LOOPBACK_HOST = re.compile(r"(127\.0\.0\.1|localhost|\[::1\])(:\d{1,5})?")
CAPTURE_ARG = Annotated[
    bool,
    Field(description="Capture a fresh runtime snapshot before building the bundle."),
]
LIMIT_ARG = Annotated[int, Field(ge=1, le=100, description="Maximum number of recent items or hits (1–100).")]
STARTUP_LIMIT_ARG = Annotated[int | None, Field(ge=1, le=100, description=(
    "Maximum task changes (brief mode, default 10) or recent events (bundle mode, default 3)."))]
BRIEF_TASK_CHANGES = 10
BUNDLE_RECENT_EVENTS = 3
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
DETAIL_ARG = Annotated[
    Literal["index", "full"],
    Field(description="index (default): one short line per hit; full: every hit's whole record and provenance."),
]
IDS_ARG = Annotated[
    list[str],
    Field(min_length=1, max_length=20, description="Ids from query_memory or a brief: event:<id>, note:<id> or fact:<id> (a bare id is an event, or else a fact)."),
]
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

# What a long-running server sets up in _main and its routes and tools read:
# the loop monitor, the background embedder, the stack-dump signal, and the
# last committed write. A server built without _main (tests, embedding in
# another program) has none of them and behaves as before.
_RUNTIME: dict[str, Any] = {"loop": None, "embedder": None, "dump_signal": None, "last_write": None,
                            "scheduler": None}
STACK_DUMP_SIGNAL = "SIGUSR1"
# A pre-2026 client's session with nothing in flight for this long is closed;
# the client starts a new one on its next request. SDK 1.x never closed them.
SESSION_IDLE_TIMEOUT_S = 1800

# Writers get a single slot (SQLite is single-writer anyway, and queueing here
# beats surfacing `database is locked` to agents); readers get 8 so a slow read
# cannot monopolise the pool. A CapacityLimiter belongs to the event loop that
# created it, so these live in RunVars — one set per async run. A plain module
# global would hand a limiter from a dead loop to a new one (the daemon has a
# single loop, but tests spin up an asyncio.run per case).
# Web pages get threads of their own: a page abandoned at its time limit keeps
# its thread until it ends, and must not take one /health or a tool needs.
_LIMITER_TOKENS = {"read": 8, "write": 1, "ui": 2}
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
    """The object that lasts as long as the client's connection, or None outside a request.

    SDK 2 hands every request a fresh session proxy; the connection behind it
    is what lasts for a pre-2026 client's session. A 2026-07-28 request has no
    session at all, so its identity is per call: the `agent` it names, or the
    client's own name.
    """
    if ctx is None:
        return None
    try:
        session = ctx.session
    except Exception:
        return None
    return getattr(session, "_connection", session)


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
            started = time.monotonic()
            result = await anyio.to_thread.run_sync(functools.partial(fn, **kwargs), limiter=limiter)
            if mutating:
                _RUNTIME["last_write"] = {
                    "tool": tool_name,
                    "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "ms": round((time.monotonic() - started) * 1000),
                }
            return result
        except ToolError:
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
    """The name the MCP client gave: in its initialize request, or in a 2026 request's _meta."""
    session = _gate_session(ctx)
    try:
        name = session.client_params.client_info.name
    except AttributeError:
        return None
    return name if isinstance(name, str) and name.strip() else None


def _public_identity(identity: dict) -> dict:
    return {"session_id": identity["session_id"], "agent": identity["agent"]}


def _prompt_argument(value: str | None) -> str | None:
    """A prompt argument, or None for an empty one or an unfilled placeholder ($1).

    Clients that expose prompts as slash commands sent the literal "$1" when
    the user typed no argument, and every render failed on it.
    """
    if value is None:
        return None
    value = value.strip()
    return None if not value or re.fullmatch(r"\$\d+|\{\w+\}", value) else value


def _audit_summary(audit: dict | None) -> dict | None:
    """Verdict and warnings only — `sources_audit` serves the per-source detail.

    The full audit repeats every source's metadata, and its warnings are
    already spelled out at the end of the activation prompt.
    """
    if not audit:
        return audit
    return {key: audit[key] for key in ("status", "issue_count", "warnings", "issues") if key in audit}


def build_server(manifest_path: Path | None = None, *, profile: str = CHRONICLER_PROFILE) -> MCPServer:
    if profile not in MCP_PROFILES:
        raise ValueError(f"Unsupported MCP profile: {profile}")
    resolved_manifest_path = manifest_path or default_manifest_path()
    writes_allowed = profile in (CHRONICLER_PROFILE, ADMIN_PROFILE)
    admin = profile == ADMIN_PROFILE

    def manifest() -> dict:
        return load_manifest_cached(resolved_manifest_path)

    # Whether Mem0 shows in the tools and instructions is decided when the
    # server is built; the services check the manifest on every call.
    try:
        mem0_on = mem0_enabled(manifest())
    except OSError:
        mem0_on = True

    # Identity per live session object (a WeakKeyDictionary, pruned when a
    # transport drops its session). Writes no longer depend on it: the startup
    # gate that refused writes until startup_bundle was called in the same MCP
    # session is gone. Every reconnect, server restart or resumed session
    # re-locked it (22 refusals in a month, one a second after a successful
    # startup), and the 2026 protocol has no sessions at all. What remains is a
    # reminder, keyed by agent and project instead of by connection.
    gate_lock = threading.Lock()
    identities: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()
    fallback_identity = {"session_id": str(uuid.uuid4()), "agent": UNKNOWN_AGENT}
    started: dict[tuple[str, str], float] = {}

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

    def _startup_key(identity: dict, project: str | None) -> tuple[str, str]:
        return identity["agent"], registry().project(project) or ""

    def note_startup(identity: dict, project: str | None) -> None:
        with gate_lock:
            started[_startup_key(identity, project)] = time.monotonic()
            started[(identity["agent"], "")] = time.monotonic()  # a project read also covers global writes

    def startup_notice(identity: dict, project: str | None) -> dict | None:
        """A reminder for a write by an agent that has not read this project's memory recently."""
        with gate_lock:
            last = started.get(_startup_key(identity, project))
        if last is not None and time.monotonic() - last < STARTUP_NOTICE_TTL_S:
            return None
        return {"notice": "startup_not_read", "hint": STARTUP_NOTICE_HINT}

    server = MCPServer(
        name={CHRONICLER_PROFILE: "Max Chronicle", ADMIN_PROFILE: "Max Chronicle Admin"}.get(
            profile, "Max Chronicle Read Only"),
        instructions=(
            "Chronicle is the shared, durable memory of the agents on this machine: decisions, "
            "findings, task checkpoints and current facts, each with its author and evidence. "
            "Start: `startup_bundle(project, task_id)` returns the project's brief and, for a task, "
            "its checkpoint, current facts and changes. "
            "Recall: `query_memory(query, project)` returns one short line per hit; `get_memory(ids)` "
            "returns the full records you pick. `recent_events` is the latest-N feed, `state_at` what "
            "was known at a moment. "
            "Write: `record_event` for decisions, milestones and findings; put a rule or current value in "
            "`fact` with a slot (replace one with `fact.supersedes`); hand a task over with a `checkpoint` "
            "(completed, verification, open questions, next steps). Writes need no prior call; reuse "
            "`request_id` unchanged on a retry. "
            "A record is an attributed assertion, not proof: check its evidence and kind. A weak or empty "
            "recall does not prove the memory is absent. A failing tool raises with a JSON envelope after "
            "the prefix (parse from the first '{'): follow its `hint`."
            + (" `search_mem0_live` queries the external Mem0 stack." if mem0_on and admin else "")
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
        admin_only: bool = False,
    ):
        """Register a sync tool body wrapped in the thread-offload + envelope layer.

        The annotations tell clients what a call may change: a tool that may
        write is not read-only, one that retires or rewrites records is
        destructive, and one that reaches a service outside Chronicle is open.
        """

        def decorator(fn):
            if admin_only and not admin:
                return fn  # not on this profile's surface
            if writes is True and not writes_allowed:
                return fn
            read_only = writes is False
            annotations = ToolAnnotations(
                read_only_hint=read_only,
                destructive_hint=None if read_only else destructive,
                open_world_hint=open_world,
            )
            tool_kwargs: dict[str, Any] = {"name": name, "description": description, "annotations": annotations}
            if structured_output is not None:
                tool_kwargs["structured_output"] = structured_output
            server.tool(**tool_kwargs)(_offload(fn, tool_name=name, writes=writes))
            return fn

        return decorator

    def register_resource(uri: str, *, title: str, description: str, admin_only: bool = False):
        def decorator(fn):
            if admin_only and not admin:
                return fn
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
        payload.update(_runtime_health())
        return JSONResponse(payload, status_code=200 if db_ok else 503)

    @server.custom_route("/health/live", methods=["GET"])
    async def route_health_live(_: Request) -> JSONResponse:
        # Liveness only: answered by the event loop itself, no database, no
        # thread. A slow disk makes /health degraded; only a stuck loop makes
        # this one silent.
        return JSONResponse({"status": "ok", "pid": os.getpid(),
                             "uptime_s": round(time.time() - _PROCESS_STARTED_AT), **_runtime_health()})

    async def route_ui(request: Request):
        if not viewer.allowed(request.headers):
            return PlainTextResponse("Forbidden\n", status_code=403)
        segments = viewer.segments_of(request.scope.get("raw_path") or request.url.path)
        query = dict(request.query_params)
        health = _runtime_health()
        try:
            with anyio.fail_after(VIEW_TIMEOUT_S):
                status, page = await anyio.to_thread.run_sync(
                    lambda: viewer.render(manifest(), segments, query, lambda: health), abandon_on_cancel=True,
                    limiter=_get_limiter("ui"))
        except ValueError as exc:
            status, page = 400, viewer.error_page(str(exc))
        except Exception as exc:  # noqa: BLE001 — a page that failed says so instead of hanging
            status, page = 503, viewer.error_page(f"Unavailable: {type(exc).__name__}")
        return HTMLResponse(page, status_code=status, headers=viewer.SECURITY_HEADERS)

    server.custom_route("/ui", methods=["GET"])(route_ui)
    server.custom_route("/ui/{rest:path}", methods=["GET"])(route_ui)

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
        admin_only=True,
    )
    def resource_attach_current() -> dict:
        return build_attach_bundle(manifest(), domain_id="global", agent="mcp-resource", capture=False)

    @register_resource(
        "chronicle://attach/domain/{domain}",
        title="Domain Attach Bundle",
        description="Machine-readable agent attach bundle for a domain.",
        admin_only=True,
    )
    def resource_attach_domain(domain: str) -> dict:
        return build_attach_bundle(manifest(), domain_id=domain, agent="mcp-resource", capture=False)

    @register_resource(
        "chronicle://state/current",
        title="Current Chronicle State",
        description="Database summary, latest snapshot, and recent durable events.",
        admin_only=True,
    )
    def resource_state_current() -> dict:
        return current_state(manifest(), domain="global")

    @register_resource(
        "chronicle://state/domain/{domain}",
        title="Domain Chronicle State",
        description="Latest snapshot and recent events for a domain.",
        admin_only=True,
    )
    def resource_state_domain(domain: str) -> dict:
        return current_state(manifest(), domain=domain)

    @register_resource(
        "chronicle://sources/audit",
        title="Chronicle Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for the global domain.",
        admin_only=True,
    )
    def resource_sources_audit() -> dict:
        return build_sources_audit(manifest(), domain_id="global")

    @register_resource(
        "chronicle://sources/audit/{domain}",
        title="Domain Sources Audit",
        description="Machine-readable source catalog, lane coverage, freshness, and trust audit for a domain.",
        admin_only=True,
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
        admin_only=True,
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
        admin_only=True,
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
            "Session entry point: what this project's memory says now. Returns the compact brief "
            "(open tasks, current facts, recent decisions, warnings). With task_id, `task_context` "
            "adds that task's checkpoint, its current facts and the changes since `since` (save "
            "`task_context.cursor` for the next start; page back with `before`). Call it before "
            "continuing a task; writes do not require it. mode='bundle' returns the full startup "
            "bundle instead (domain state, recent events, freshness audit); focus, title, capture "
            "or compact=false ask for the bundle too. Changes nothing unless capture=true."
        ),
    )
    def tool_startup_bundle(
        domain: DOMAIN_ARG = "global",
        agent: AGENT_ARG = "mcp",
        title: OPTIONAL_TITLE_ARG = None,
        focus: OPTIONAL_FOCUS_ARG = None,
        capture: CAPTURE_ARG = False,
        limit: STARTUP_LIMIT_ARG = None,
        compact: COMPACT_ARG = True,
        project: PROJECT_ARG = None,
        task_id: TASK_ARG = None,
        since: CURSOR_ARG = None,
        before: BEFORE_ARG = None,
        mode: STARTUP_MODE_ARG = "brief",
        ctx: Context | None = None,
    ) -> dict:
        identity = session_identity(ctx, agent)
        # A caller that names a bundle-only argument is a caller of the bundle,
        # which was the default before the brief: it gets what it asked for.
        if mode == "brief" and not (focus or title or capture or not compact):
            loaded = manifest()
            brief = build_brief(loaded, project=project)
            payload = {"brief": brief["text"], "project": brief["project"], "counts": brief["counts"]}
            if task_id:
                # The brief alone used to answer a task_id: an agent believed it had
                # resumed the task without ever seeing its checkpoint.
                # The same domain as the bundle's, so a cursor saved from either mode
                # resumes in both.
                payload["task_context"] = read_task_context(
                    loaded, domain=domain, project=brief["project"] or project, task_id=task_id,
                    since=since, before=before, limit=limit or BRIEF_TASK_CHANGES)
            payload.update(_public_identity(identity))
            note_startup(identity, brief["project"] or project)
            return payload
        effective_capture = capture if writes_allowed else False
        payload = build_startup_bundle(
            manifest(),
            domain_id=domain,
            agent=identity["agent"],
            title=title,
            focus=focus,
            capture=effective_capture,
            limit=limit or BUNDLE_RECENT_EVENTS,
            compact=compact,
            project=project,
            task_id=task_id,
            since=since,
            before=before,
        )
        payload.update(_public_identity(identity))
        note_startup(identity, project)
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
        admin_only=True,
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
            "Recall: start here for 'what do we know about X'. Searches recorded events (`results`) "
            "and indexed notes (`notes`) by words and meaning; recency only breaks ties. By default "
            "each hit is one short line (id, when, project, task, agent, a gist of the text, how it "
            "matched); `get_memory(ids)` returns the full records you pick, and detail='full' returns "
            "them all at once. `no_confident_match` means no hit is a strong match: treat the hits as "
            "leads. `relaxed` marks a weaker word-stem match, `degraded` a skipped channel (word "
            "search still works when the embedding model is down). An empty result does not prove "
            "the memory is absent."
        ),
    )
    def tool_query_memory(
        query: RECALL_QUERY_ARG,
        domain: OPTIONAL_DOMAIN_ARG = None,
        limit: LIMIT_ARG = 10,
        project: PROJECT_ARG = None,
        task_id: TASK_ARG = None,
        detail: DETAIL_ARG = "index",
    ) -> dict:
        response = query_memory(manifest(), query=query, domain=domain, limit=limit,
                                project=project, task_id=task_id)
        return index_view(response) if detail == "index" else response

    @register_tool(
        writes=False,
        name="get_memory",
        description=(
            "Full records for ids that query_memory (or a brief) returned: `event:<id>` gives the "
            "event with its why, checkpoint or fact, evidence files and provenance; `note:<id>` "
            "gives the whole indexed note; `fact:<id>` gives a fact with the event that asserted it. "
            "Up to 20 ids; ids that name nothing come back in `missing`."
        ),
    )
    def tool_get_memory(ids: IDS_ARG) -> dict:
        return get_items(manifest(), ids)

    @register_tool(
        writes=False,
        admin_only=True,
        name="sources_audit",
        description="Inspect source coverage, lane enablement, freshness, and trust metadata.",
    )
    def tool_sources_audit(domain: DOMAIN_ARG = "global") -> dict:
        return build_sources_audit(manifest(), domain_id=domain)

    @register_tool(
        writes=True,
        admin_only=True,
        name="activate_agent",
        description=(
            "DEPRECATED alias — prefer `startup_bundle` as the single session entry point. "
            "Kept for transition: captures current state (capture=true by default) and returns "
            "the activation prompt plus a digest of runtime state. Writes need no startup. Call "
            "`startup_bundle` for the brief."
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
        # name. What this tool is actually for is the prompt; the
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
        note_startup(session_identity(ctx, agent), None)
        return payload

    @register_tool(
        writes=True,
        name="record_event",
        description=(
            "Write a durable event: a decision, milestone, finding or handoff, with `why` and "
            "`source_files` as evidence (archived, secrets redacted). `fact` sets or replaces a "
            "current value under a slot; `checkpoint` hands a task over (needs project and "
            "task_id). Unknown but well-formed categories are stored as `note`. "
            f"{WRITE_DESCRIPTION}"
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
        identity = session_identity(ctx, agent)
        named = agent != UNKNOWN_AGENT
        # With a background embedder the vector comes after the commit,
        # outside the write slot every agent shares.
        embedder = _RUNTIME["embedder"]
        stored = record_event(
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
                    mem0=mem0_enabled(manifest()),
                ),
                "mem0_error": None,
                "mem0_raw": None,
            },
            append_compat=True,
            dedupe=True,
            source_kind="chronicle_mcp",
            imported_from="max_chronicle.mcp.record_event",
            defer_embedding=embedder is not None,
        )
        if embedder is not None and stored.get("chronicle_status") == "stored":
            embedder.wake()
        notice = startup_notice(identity, project) if project else None
        if notice:
            stored.update(notice)
        return stored

    @register_tool(
        writes=True,
        admin_only=True,
        name="capture_snapshot",
        description=(
            "Capture live runtime state into Chronicle, archive evidence, and refresh the "
            "markdown projections (status.md and friends — the former render_projections tool "
            "is folded in here)."
        ),
    )
    def tool_capture_snapshot(
        domain: DOMAIN_ARG = "global",
        agent: AGENT_ARG = "mcp",
        title: OPTIONAL_TITLE_ARG = None,
        focus: OPTIONAL_FOCUS_ARG = None,
        ctx: Context | None = None,
    ) -> dict:
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
        admin_only=True,
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
            "tables untouched)."
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

    if mem0_on:  # the live Mem0 search exists only while the installation runs Mem0
        @register_tool(
            writes=False,
            admin_only=True,
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
        name="continue_work",
        description="The memory brief of a project, and a task's checkpoint, to continue from.",
    )
    def prompt_continue_work(project: str | None = None, task_id: str | None = None) -> list[dict]:
        loaded = manifest()
        project, task_id = _prompt_argument(project), _prompt_argument(task_id)
        brief = build_brief(loaded, project=project)
        content = brief["text"]
        if task_id:
            context = read_task_context(loaded, domain=None, project=brief["project"] or project,
                                        task_id=task_id, limit=10)
            content += "\n## Task context\n" + json.dumps(context, ensure_ascii=False, indent=2)
        return [{"role": "user", "content": f"Continue from this Chronicle memory:\n{content}"}]

    @register_prompt(
        name="reconstruct_moment",
        description="Provide the timeline state around a specific timestamp.",
    )
    def prompt_reconstruct_moment(timestamp: str) -> list[dict]:
        loaded = manifest()
        timestamp = _prompt_argument(timestamp)
        if not timestamp:
            raise ValueError("reconstruct_moment needs a timestamp, such as 2026-10-01T12:00")
        target = parse_when(timestamp, loaded)
        content = json.dumps(
            reconstruct_timeline(loaded, timestamp=target, domain="global", window_hours=6, limit=3),
            ensure_ascii=False,
            indent=2,
        )
        return [{"role": "user", "content": f"Reconstruct this moment from Chronicle:\n{content}"}]

    return server


def _runtime_health() -> dict[str, Any]:
    """What /health adds about the process itself: loop, write queue, last write, embedder."""
    health: dict[str, Any] = {}
    if _RUNTIME["dump_signal"]:
        # The watchdog sends this before a kill, so the stacks of a hang reach the log.
        health["stack_dump_signal"] = _RUNTIME["dump_signal"]
    if _RUNTIME["loop"] is not None:
        health["loop"] = _RUNTIME["loop"].snapshot()
    try:
        writes = _get_limiter("write").statistics()
        health["writes"] = {"in_progress": writes.borrowed_tokens, "waiting": writes.tasks_waiting}
    except RuntimeError:  # no event loop: a direct call outside the server
        pass
    if _RUNTIME["last_write"] is not None:
        health["last_write"] = _RUNTIME["last_write"]
    if _RUNTIME["embedder"] is not None:
        health["embedder"] = _RUNTIME["embedder"].snapshot()
    if _RUNTIME["scheduler"] is not None:
        health["scheduler"] = _RUNTIME["scheduler"].snapshot()
    return health


def _configure_logging() -> None:
    """UTC timestamps with a Z on every line, uvicorn's access log included.

    The server logged naive local time, so the switch to winter time made the
    log run backwards for an hour, and the access log had no time at all.
    """
    logging.Formatter.converter = time.gmtime
    utc = "%(asctime)s.%(msecs)03dZ"
    datefmt = "%Y-%m-%dT%H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=f"{utc} %(levelname)s %(name)s: %(message)s",
        datefmt=datefmt,
        stream=sys.stderr,
    )
    try:
        from uvicorn.config import LOGGING_CONFIG
    except ImportError:  # pragma: no cover - uvicorn comes with mcp
        return
    formatters = LOGGING_CONFIG.get("formatters", {})
    if "default" in formatters:
        formatters["default"].update(fmt=f"{utc} %(levelprefix)s %(message)s", datefmt=datefmt)
    if "access" in formatters:
        formatters["access"].update(
            fmt=f'{utc} %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s', datefmt=datefmt)


def _serve(server: MCPServer, transport: str, http_options: dict[str, Any] | None = None) -> None:
    """Run the server; over HTTP with the loop monitor and its stall watcher.

    Streamable HTTP serves both protocol eras on one endpoint: a 2026-07-28
    request is answered on its own, with no session to lose on a restart; an
    older client gets the initialize handshake and a session, closed after
    SESSION_IDLE_TIMEOUT_S without traffic instead of kept forever.
    """
    if transport == "stdio":
        server.run(transport="stdio")
        return
    monitor = LoopMonitor(c_timer=True)
    _RUNTIME["loop"] = monitor
    monitor.start_watcher()
    options = dict(http_options or {})
    if transport == "streamable-http":
        options.setdefault("session_idle_timeout", SESSION_IDLE_TIMEOUT_S)
        run = functools.partial(server.run_streamable_http_async, **options)
    else:
        run = functools.partial(server.run_sse_async, **options)

    async def serve() -> None:
        async with anyio.create_task_group() as group:
            group.start_soon(monitor.run)
            await run()
            group.cancel_scope.cancel()

    anyio.run(serve)


def _install_stack_dump() -> None:
    """Dump every thread's stack on SIGUSR1 (and on a fatal error) to stderr, the server log."""
    try:
        faulthandler.enable(file=sys.stderr, all_threads=True)
        faulthandler.register(getattr(signal, STACK_DUMP_SIGNAL), file=sys.stderr, all_threads=True)
    except (AttributeError, OSError, RuntimeError, ValueError) as exc:  # no usable stderr fd, or no such signal
        _LOGGER.warning("stack dumps unavailable: %s: %s", type(exc).__name__, exc)
        return
    _RUNTIME["dump_signal"] = STACK_DUMP_SIGNAL


def _start_scheduler(manifest_path: Path) -> Scheduler | None:
    """Run the automation jobs from this server when the installation says so."""
    try:
        automation_path = default_automation_path()
        automation = load_automation_config(automation_path)
    except (ChronicleConfigError, OSError, KeyError, ValueError) as exc:
        _LOGGER.info("scheduler not started: no usable automation config (%s: %s)", type(exc).__name__, exc)
        return None
    if not automation.scheduler_enabled:
        return None
    try:
        scheduler = Scheduler.for_installation(manifest_path, automation_path)
        scheduler.start()
    except Exception as exc:  # noqa: BLE001 — the server serves memory even when its jobs cannot run
        _LOGGER.error("scheduler not started: %s: %s", type(exc).__name__, exc)
        return None
    _RUNTIME["scheduler"] = scheduler
    _LOGGER.info("scheduler started: jobs %s", sorted(automation.jobs))
    return scheduler


def _start_embedder(manifest_path: Path) -> BackgroundEmbedder:
    embedder = BackgroundEmbedder(lambda: load_manifest_cached(manifest_path), embed_backfill)
    _RUNTIME["embedder"] = embedder
    embedder.start()
    return embedder


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
    _configure_logging()
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
    http_options: dict[str, Any] = {}
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
        http_options = {
            "host": host,
            "port": port,
            "transport_security": TransportSecuritySettings(allowed_hosts=allowed_hosts),
        }
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
    if args.transport != "stdio":
        _install_stack_dump()
    if not read_only and args.transport != "stdio":
        _start_notes_sync(manifest_path)
        if feature_enabled(ENV_FEATURE_EVENT_EMBEDDINGS, default=True):
            _start_embedder(manifest_path)
        _start_scheduler(manifest_path)
    _serve(server, args.transport, http_options)
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
