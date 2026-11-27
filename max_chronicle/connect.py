"""`chronicle connect`: connect a coding agent to this Chronicle server, with a role.

Four parts per agent, each in the agent's own user-level files:

- **server**: an MCP entry named ``chronicle`` with the address of the role
  (``write``: ``/mcp``, six tools; ``read``: ``/mcp/read``, five, no ``record_event``);
- **permissions**: the role's tools allowed without a prompt;
- **hook**: a session-start hook that hands the session its memory brief
  (``python -m max_chronicle.brief_hook``);
- **block**: a marked block in the agent's instruction file.

What each agent supports, and the evidence, is in
``tests/fixtures/connect/FACTS.md``; a part without evidence is skipped and
reported. Every change is one contiguous piece of text, made by
``connect_edit`` and recorded in the workspace's ledger first, so
``disconnect`` takes back exactly what ``connect`` put. A role is a
guard-rail, not a security boundary: an agent with a shell on this machine
can read the database and other agents' files directly.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import shlex
import sqlite3
import string
import sys
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from http.client import HTTPException

from . import __version__
from .connect_edit import (
    Edit,
    EditRefused,
    JsonDoc,
    Ledger,
    Loaded,
    TextDoc,
    TomlDoc,
    load,
    sha256_file,
    toml_key,
    toml_string,
    write,
)
from .server_address import resolve_server_url

SERVER_NAME = "chronicle"
WRITE_TOOLS = ("startup_bundle", "query_memory", "get_memory", "recent_events", "state_at", "record_event")
READ_TOOLS = tuple(tool for tool in WRITE_TOOLS if tool != "record_event")
ROLES = {"write": ("/mcp", WRITE_TOOLS), "read": ("/mcp/read", READ_TOOLS)}
PARTS = ("server", "permissions", "hook", "block")
MIN_SERVER_VERSION = (0, 33, 0)
HTTP_TIMEOUT_S = 2.0
HOOK_MODULE = "max_chronicle.brief_hook"
HOOK_TIMEOUT_S = 5
HOOK_MATCHER = "startup|resume|clear|compact"
DEFAULT_HOOK_BUDGET = 6000
CODEX_HOOK_BUDGET = 4500  # Codex keeps about 2,500 tokens of a hook's output
LEDGER_NAME = "connect-ledger.json"
BLOCK_BEGIN = "<!-- chronicle:begin"
BLOCK_END = "<!-- chronicle:end -->"
BLOCK_TEMPLATE = Path(__file__).resolve().parent / "docs" / "connect_block.md"
EVENT_CATEGORY = "agent_connection"
OPERATOR = "operator"
MCP_PROTOCOL_VERSION = "2025-06-18"

# Exit codes (documented in `chronicle connect --help`).
EXIT_OK = 0  # done, or nothing to do
EXIT_REFUSED = 1  # refused: an entry in the way, a file not edited, an answer of no
EXIT_PREFLIGHT = 4  # the server is not answering, not Chronicle, too old, or has no read endpoint
EXIT_DRIFT = 5  # doctor --agents: a part is not as connect left it
EXIT_UNSUPPORTED = 6  # an unknown or uninstalled agent, Cursor without --print, Windows

EXIT_CODES_HELP = """exit codes:
  0  done, or nothing to do
  1  refused: an existing entry in the way, a file that is not edited (symbolic link,
     CRLF, byte-order mark, duplicate keys, JSON5, a dotted TOML key), a file that
     changed while connect ran, or the answer was no
  2  the workspace is unclear (as for every chronicle command)
  4  preflight failed: no server address, no answer, not Chronicle, older than 0.33.0,
     or /mcp/read does not answer; nothing was changed
  5  doctor --agents: a part is not as connect left it
  6  not supported: unknown or uninstalled agent, Cursor without --print, Windows
"""


class PreflightFailed(Exception):
    def __init__(self, reason: str, hint: str) -> None:
        super().__init__(reason)
        self.reason, self.hint = reason, hint


# ---------------------------------------------------------------------------
# Addresses
# ---------------------------------------------------------------------------

_LOOPBACK = {"127.0.0.1", "localhost", "::1", "[::1]"}


def _url_parts(url: str) -> tuple[str, str, int | None, str] | None:
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        return None
    host = parts.hostname.lower()
    host = "loopback" if host in _LOOPBACK else host
    return parts.scheme, host, port or (443 if parts.scheme == "https" else 80), parts.path.rstrip("/")


def same_server(url: str | None, base_url: str) -> bool:
    """Whether *url* (an MCP address) belongs to the server at *base_url*: scheme, host, port."""
    if not url:
        return False
    one, other = _url_parts(url), _url_parts(base_url)
    return bool(one and other and one[:3] == other[:3] and one[3] in ("/mcp", "/mcp/read"))


def role_of_url(url: str | None) -> str | None:
    parts = _url_parts(url or "")
    if not parts:
        return None
    return {"/mcp": "write", "/mcp/read": "read"}.get(parts[3])


def _version_tuple(value: str) -> tuple[int, ...]:
    numbers: list[int] = []
    for piece in str(value).split("."):
        digits = "".join(char for char in piece if char.isdigit()) if piece[:1].isdigit() else ""
        if not digits:
            break
        numbers.append(int(digits))
    return tuple(numbers)


def _start_command(manifest: Path) -> str:
    server = Path(sys.executable).with_name("chronicle-mcp")
    program = shlex.quote(str(server)) if server.exists() else f"{shlex.quote(sys.executable)} -m max_chronicle.mcp_server"
    return (f"Start the HTTP server, for example: MCP_PORT=<port> {program} --manifest {shlex.quote(str(manifest))} "
            f"--transport streamable-http --migrate")


def cli_command(manifest: Path) -> str:
    """The absolute command of this installation's CLI, with its workspace."""
    cli = Path(sys.executable).with_name("chronicle")
    program = shlex.quote(str(cli)) if cli.exists() else f"{shlex.quote(sys.executable)} -m max_chronicle.cli"
    return f"{program} --manifest {shlex.quote(str(manifest))}"


def _http_json(url: str, *, data: bytes | None = None, headers: dict[str, str] | None = None,
               method: str | None = None) -> tuple[int, Any, dict[str, str]]:
    request = Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT_S) as response:  # noqa: S310 - a local URL
            status, raw, reply_headers = response.status, response.read(), dict(response.headers)
    except HTTPError as exc:
        status, raw, reply_headers = exc.code, exc.read(), dict(exc.headers or {})
    text = raw.decode("utf-8", errors="replace")
    body: Any = None
    stripped = text.lstrip()
    if stripped.startswith("{"):
        try:
            body = json.loads(stripped)
        except ValueError:
            body = None
    else:
        for line in text.splitlines():
            if line.startswith("data:"):
                try:
                    body = json.loads(line[5:].strip())
                except ValueError:
                    body = None
                break
    return status, body, {key.lower(): value for key, value in reply_headers.items()}


def probe_mcp(url: str) -> bool:
    """Whether an MCP endpoint answers an initialize request (its session is closed again)."""
    message = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": MCP_PROTOCOL_VERSION, "capabilities": {},
        "clientInfo": {"name": "chronicle-connect", "version": __version__}}}
    headers = {"content-type": "application/json", "accept": "application/json, text/event-stream"}
    try:
        status, body, reply = _http_json(url, data=json.dumps(message).encode(), headers=headers, method="POST")
    except (URLError, OSError, HTTPException):
        return False
    ok = status == 200 and isinstance(body, dict) and isinstance(body.get("result"), dict)
    session = reply.get("mcp-session-id")
    if session:
        try:
            _http_json(url, headers={"mcp-session-id": session, "mcp-protocol-version": MCP_PROTOCOL_VERSION},
                       method="DELETE")
        except (URLError, OSError, HTTPException):
            pass
    return ok


def preflight(url_arg: str | None, workspace: Path, manifest: Path) -> dict[str, Any]:
    """The server to connect to, checked before any file is touched."""
    start = _start_command(manifest)
    base_url, source = resolve_server_url(url_arg, workspace)
    if not base_url:
        raise PreflightFailed(
            "no server address: pass --url, set CHRONICLE_SERVER_URL, or start the HTTP server "
            "(it writes its address to the workspace)", start)
    try:
        status, health, _ = _http_json(f"{base_url}/health")
    except (URLError, OSError, HTTPException) as exc:
        reason = getattr(exc, "reason", exc)
        raise PreflightFailed(f"no answer from {base_url} ({type(exc).__name__}: {reason})", start) from exc
    if not isinstance(health, dict) or "status" not in health or "version" not in health:
        raise PreflightFailed(f"something else answers at {base_url}: not a Chronicle server", start)
    version = str(health.get("version"))
    if _version_tuple(version) < MIN_SERVER_VERSION:
        raise PreflightFailed(
            f"Chronicle {version} answers at {base_url}; connect needs "
            f"{'.'.join(map(str, MIN_SERVER_VERSION))} or newer", "Restart the server on the new release. " + start)
    endpoints = {item.get("path"): item.get("role") for item in health.get("mcp_endpoints") or []
                 if isinstance(item, dict)}
    if endpoints.get("/mcp") not in ("write", "admin") or endpoints.get("/mcp/read") != "read":
        raise PreflightFailed(f"the server at {base_url} does not serve /mcp (write) and /mcp/read (read)",
                              "Run a chronicler server over streamable HTTP. " + start)
    if not probe_mcp(f"{base_url}/mcp/read"):
        raise PreflightFailed(f"{base_url}/mcp/read does not answer an MCP request", start)
    notes = []
    served = health.get("manifest_path")
    if served and Path(served).resolve() != manifest.resolve():
        notes.append(f"the server reads {served}; this command's workspace is {manifest}")
    return {"base_url": base_url, "source": source, "version": version, "status": health.get("status"),
            "notes": notes}


# ---------------------------------------------------------------------------
# Plans
# ---------------------------------------------------------------------------


@dataclass
class Context:
    agent: str
    role: str
    base_url: str
    manifest: Path
    force: bool = False
    adopt: bool = False

    @property
    def mcp_url(self) -> str:
        return self.base_url + ROLES[self.role][0]

    @property
    def tools(self) -> tuple[str, ...]:
        return ROLES[self.role][1]


@dataclass
class Part:
    """What one part of a connection needs: edits to make, or why nothing is made."""

    name: str
    status: str = "present"  # install, present, skipped or refused
    detail: str = ""
    manual: str | None = None
    edits: list[Edit] = field(default_factory=list)
    adoptions: list[dict[str, Any]] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def install(self, edit: Edit, detail: str) -> None:
        self.edits.append(edit)
        self.status = "install"
        self.detail = "; ".join(filter(None, [self.detail, detail]))
        if str(edit.path) not in self.files:
            self.files.append(str(edit.path))

    def present(self, path: Path, detail: str, adoption: dict[str, Any] | None = None) -> None:
        if self.status not in ("install",):
            self.status = "present"
        self.detail = "; ".join(filter(None, [self.detail, detail]))
        if str(path) not in self.files:
            self.files.append(str(path))
        if adoption is not None:
            self.adoptions.append({"path": str(path), **adoption})

    def skip(self, detail: str, manual: str | None = None) -> None:
        self.status, self.detail, self.manual = "skipped", detail, manual

    def refuse(self, detail: str, manual: str | None = None) -> None:
        self.status, self.detail, self.manual = "refused", detail, manual
        self.edits.clear()

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"status": self.status, "detail": self.detail, "files": self.files}
        if self.manual:
            payload["manual"] = self.manual
        if self.notes:
            payload["notes"] = self.notes
        return payload


class Files:
    """The files of one run as they will be after the edits planned so far."""

    def __init__(self) -> None:
        self.current: dict[Path, Loaded] = {}
        self.original: dict[Path, Loaded] = {}

    def load(self, path: Path) -> Loaded:
        path = Path(path)
        if path not in self.current:
            loaded = load(path)
            self.current[path] = loaded
            self.original[path] = loaded
        return self.current[path]

    def apply(self, edit: Edit) -> None:
        before = self.current[edit.path]
        if edit.after_text is None:
            self.current[edit.path] = Loaded(edit.path, None, "", None)
        else:
            self.current[edit.path] = Loaded(edit.path, edit.after_bytes, edit.after_text, before.mode)

    def changed_on_disk(self) -> list[str]:
        changed = []
        for path, loaded in self.original.items():
            try:
                raw = path.read_bytes()
            except FileNotFoundError:
                raw = None
            if raw != loaded.raw or path.is_symlink():
                changed.append(str(path))
        return changed


def _edit_into(part: Part, files: Files, edit: Edit, detail: str) -> None:
    files.apply(edit)
    part.install(edit, detail)


def _entry_url(entry: Any, keys: Iterable[str]) -> tuple[str | None, str | None]:
    if not isinstance(entry, dict):
        return None, None
    for key in keys:
        if isinstance(entry.get(key), str):
            return entry[key], key
    return None, None


def plan_json_server(part: Part, files: Files, ctx: Context, path: Path, container: list[str], value: dict[str, Any],
                     *, comments: bool, url_keys: tuple[str, ...] = ("url",)) -> None:
    """The server entry in a JSON file: insert it, take a matching one, switch its role, or refuse."""
    doc = JsonDoc(files.load(path), comments=comments)
    doc.check_path(container)
    entries = doc.get(container)
    if entries is not None and not isinstance(entries, dict):
        part.refuse(f"{path}: '{'.'.join(container)}' is not an object", "Add the entry by hand.")
        return
    entries = entries or {}
    where = f"{path}: {'.'.join(container)}.{SERVER_NAME}"
    if SERVER_NAME in entries:
        url, key = _entry_url(entries[SERVER_NAME], url_keys)
        if url and same_server(url, ctx.base_url):
            if role_of_url(url) == ctx.role:
                part.present(path, f"{where} points at this server",
                             {"text": json.dumps(entries[SERVER_NAME], ensure_ascii=False),
                              "logical": {"format": "json", "action": "adopt", "path": [*container, SERVER_NAME],
                                          "value": entries[SERVER_NAME]}})
                return
            edit = doc.replace_value([*container, SERVER_NAME, key], ctx.mcp_url)
            _edit_into(part, files, edit, f"{where}: address set to the {ctx.role} endpoint")
            return
        if not ctx.force:
            part.refuse(f"{where} points at {url or 'something else'}",
                        "Run connect with --force to replace that entry, or remove it by hand.")
            return
        edit = doc.replace_value([*container, SERVER_NAME], value)
        _edit_into(part, files, edit, f"{where} replaced (--force)")
        return
    others = [name for name, entry in entries.items() if same_server(_entry_url(entry, url_keys)[0], ctx.base_url)]
    if others:
        name = others[0]
        if not ctx.adopt:
            part.refuse(f"{path}: the entry {name!r} points at this server",
                        f"Run connect with --adopt to rename it to {SERVER_NAME!r}, or remove it by hand.")
            return
        edit = doc.rename_key(container, name, SERVER_NAME)
        _edit_into(part, files, edit, f"{path}: {name!r} renamed to {SERVER_NAME!r} (--adopt)")
        url, key = _entry_url(entries[name], url_keys)
        if role_of_url(url) != ctx.role:
            doc = JsonDoc(files.load(path), comments=comments)
            edit = doc.replace_value([*container, SERVER_NAME, key], ctx.mcp_url)
            _edit_into(part, files, edit, f"address set to the {ctx.role} endpoint")
        return
    edit = doc.insert_member(container, SERVER_NAME, value)
    _edit_into(part, files, edit, f"{where} added")


def plan_json_items(part: Part, files: Files, path: Path, array_path: list[str], wanted: list[Any], *,
                    comments: bool, label: str, first: bool = False) -> None:
    """Entries of a JSON array (permission rules): add the ones missing."""
    doc = JsonDoc(files.load(path), comments=comments)
    doc.check_path(array_path[:-1])
    existing = doc.get(array_path)
    if existing is not None and not isinstance(existing, list):
        part.refuse(f"{path}: '{'.'.join(array_path)}' is not an array", "Add the entries by hand.")
        return
    missing = [item for item in wanted if item not in (existing or [])]
    where = f"{path}: {'.'.join(array_path)}"
    if not missing:
        part.present(path, f"{where} has {label}",
                     {"text": json.dumps(wanted, ensure_ascii=False),
                      "logical": {"format": "json", "action": "adopt_items", "path": array_path, "items": wanted}})
        return
    edit = doc.append_items(array_path, missing, first=first)
    _edit_into(part, files, edit, f"{where}: {len(missing)} added")


def plan_json_hook(part: Part, files: Files, path: Path, array_path: list[str], group: dict[str, Any], command: str,
                   *, comments: bool) -> None:
    """A hook group in a JSON array of groups: present, stale (another python or manifest), or new."""
    doc = JsonDoc(files.load(path), comments=comments)
    doc.check_path(array_path[:-1])
    groups = doc.get(array_path)
    where = f"{path}: {'.'.join(array_path)}"
    if groups is not None and not isinstance(groups, list):
        part.refuse(f"{where} is not an array", "Add the hook by hand.")
        return
    for group_index, existing in enumerate(groups or []):
        hooks = existing.get("hooks") if isinstance(existing, dict) else None
        for hook_index, hook in enumerate(hooks if isinstance(hooks, list) else []):
            found = hook.get("command") if isinstance(hook, dict) else None
            if found == command:
                part.present(path, f"{where} has the hook",
                             {"text": command, "logical": {"format": "json", "action": "adopt",
                                                           "path": [*array_path, group_index, "hooks", hook_index,
                                                                    "command"], "value": command}})
                return
            if isinstance(found, str) and HOOK_MODULE in found:
                edit = doc.replace_value([*array_path, group_index, "hooks", hook_index, "command"], command)
                _edit_into(part, files, edit, f"{where}: the hook's command updated")
                return
    edit = doc.append_items(array_path, [group])
    _edit_into(part, files, edit, f"{where}: hook added")


def render_block(ctx: Context, *, hook: bool) -> str:
    """The protocol block for *ctx*'s agent and role, from the package template."""
    template = string.Template(BLOCK_TEMPLATE.read_text(encoding="utf-8"))
    lines = template.substitute(
        role=ctx.role,
        url=ctx.mcp_url,
        start=(
            "- A session starts with a Chronicle memory brief from a session-start hook. If there is none, "
            "call `startup_bundle(project=<project slug>, task_id=<task>)` once."
            if hook else
            "- Start each session with `startup_bundle(project=<project slug>, task_id=<task>)`: it returns "
            "the project's brief and, for a task, its checkpoint."
        ),
        write=(
            "- Record decisions, milestones and findings with `record_event`; a rule or current value goes in "
            "a fact with a slot. Before a handoff, record a checkpoint: completed work, verification, open "
            "questions, next steps."
            if ctx.role == "write" else
            "- This agent is connected read-only: it has no `record_event`. What must be kept is recorded by an "
            "agent with write access or by the operator."
        ),
        connect=f"{cli_command(ctx.manifest)} connect <agent> --role <read|write> --by <your agent id> --yes --json",
    )
    return lines if lines.endswith("\n") else lines + "\n"


def plan_block(part: Part, files: Files, ctx: Context, path: Path, *, hook: bool, create: bool = True,
               create_manual: str | None = None) -> None:
    loaded = files.load(path)
    if not loaded.exists:
        if not path.parent.is_dir():
            part.skip(f"{path.parent} does not exist", f"Create {path} with the block of `connect --print`.")
            return
        if not create:
            part.skip(f"{path} does not exist", create_manual)
            return
    block = render_block(ctx, hook=hook)
    doc = TextDoc(loaded)
    span = doc.find_block(BLOCK_BEGIN, BLOCK_END)
    logical = {"begin": BLOCK_BEGIN, "end": BLOCK_END, "role": ctx.role}
    if span is not None:
        current = doc.text[span[0]:span[1]]
        if current.rstrip("\n") == block.rstrip("\n"):
            part.present(path, f"{path} has the block", {"text": current, "logical": {"format": "text",
                                                                                       "action": "adopt", **logical}})
            return
        edit = doc.replace_block(span, block, logical=logical)
        _edit_into(part, files, edit, f"{path}: block updated")
        return
    edit = doc.append_block(block, logical=logical)
    edit.logical["separator"] = edit.new[: len(edit.new) - len(block)]
    _edit_into(part, files, edit, f"{path}: block added")


# ---------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------


def _home(env: Mapping[str, str]) -> Path:
    return Path(env.get("HOME") or Path.home())


@dataclass(frozen=True)
class Adapter:
    """One agent: where its user-level files are and how each part is planned."""

    id: str
    label: str
    aliases: tuple[str, ...]
    hook_budget: int | None  # None: no confirmed session-start hook
    hook_format: str = "text"
    print_only: bool = False

    # Paths -----------------------------------------------------------------
    def paths(self, env: Mapping[str, str]) -> dict[str, Path]:
        home = _home(env)
        if self.id == "claude":
            config_dir = Path(env["CLAUDE_CONFIG_DIR"]) if env.get("CLAUDE_CONFIG_DIR") else None
            base = config_dir or home / ".claude"
            return {"home": base, "server": (config_dir or home) / ".claude.json",
                    "permissions": base / "settings.json", "hook": base / "settings.json", "block": base / "CLAUDE.md"}
        if self.id == "codex":
            base = Path(env["CODEX_HOME"]) if env.get("CODEX_HOME") else home / ".codex"
            override = base / "AGENTS.override.md"
            block = override if override.is_file() and override.stat().st_size > 0 else base / "AGENTS.md"
            return {"home": base, "server": base / "config.toml", "permissions": base / "config.toml",
                    "hook": base / "hooks.json", "block": block}
        if self.id == "zcode":
            base = home / ".zcode"
            return {"home": base, "server": base / "cli" / "config.json", "permissions": base / "cli" / "config.json",
                    "hook": base / "cli" / "config.json", "block": base / "AGENTS.md"}
        if self.id == "opencode":
            base = (Path(env["XDG_CONFIG_HOME"]) if env.get("XDG_CONFIG_HOME") else home / ".config") / "opencode"
            config = base / "opencode.jsonc"
            if not config.exists() and (base / "opencode.json").exists():
                config = base / "opencode.json"
            return {"home": base, "server": config, "permissions": config, "block": base / "AGENTS.md"}
        if self.id == "gemini":
            base = (Path(env["GEMINI_CLI_HOME"]) if env.get("GEMINI_CLI_HOME") else home) / ".gemini"
            return {"home": base, "server": base / "settings.json", "permissions": base / "settings.json",
                    "hook": base / "settings.json", "block": base / "GEMINI.md"}
        if self.id == "cursor":
            base = home / ".cursor"
            return {"home": base, "server": base / "mcp.json"}
        return {}

    def installed(self, env: Mapping[str, str]) -> bool:
        paths = self.paths(env)
        if self.id == "claude":
            return paths["home"].is_dir() or paths["server"].is_file()
        return bool(paths) and paths["home"].is_dir()

    def hook_command(self, manifest: Path) -> str:
        command = [sys.executable, "-m", HOOK_MODULE, "--manifest", str(manifest), "--agent", self.id,
                   "--budget", str(self.hook_budget)]
        if self.hook_format != "text":
            command += ["--format", self.hook_format]
        return " ".join(shlex.quote(item) for item in command)

    # Parts ------------------------------------------------------------------
    def server_value(self, ctx: Context) -> dict[str, Any]:
        return {
            "claude": {"type": "http", "url": ctx.mcp_url},
            "zcode": {"type": "http", "url": ctx.mcp_url},
            "opencode": {"type": "remote", "url": ctx.mcp_url},
            "gemini": {"url": ctx.mcp_url, "type": "http"},
            "cursor": {"url": ctx.mcp_url},
        }.get(self.id, {"type": "http", "url": ctx.mcp_url})

    def plan(self, files: Files, ctx: Context, env: Mapping[str, str]) -> dict[str, Part]:
        paths = self.paths(env)
        parts = {name: Part(name) for name in PARTS}
        planners: dict[str, Callable[[Part], None]] = {
            "server": lambda part: self._plan_server(part, files, ctx, paths),
            "permissions": lambda part: self._plan_permissions(part, files, ctx, paths),
            "hook": lambda part: self._plan_hook(part, files, ctx, paths),
        }
        for name, planner in planners.items():
            part = parts[name]
            try:
                planner(part)
            except EditRefused as exc:
                part.refuse(f"{exc.path}: {exc.reason}", exc.manual)
        hook_ready = parts["hook"].status in ("install", "present")
        try:
            self._plan_block(parts["block"], files, ctx, paths, hook=hook_ready)
        except EditRefused as exc:
            parts["block"].refuse(f"{exc.path}: {exc.reason}", exc.manual)
        return parts

    def _plan_server(self, part: Part, files: Files, ctx: Context, paths: dict[str, Path]) -> None:
        path = paths["server"]
        if self.id == "codex":
            self._plan_codex_server(part, files, ctx, path)
            return
        container = {"claude": ["mcpServers"], "zcode": ["mcp", "servers"], "opencode": ["mcp", "servers"],
                     "gemini": ["mcpServers"]}[self.id]
        url_keys = ("httpUrl", "url") if self.id == "gemini" else ("url",)
        plan_json_server(part, files, ctx, path, container, self.server_value(ctx),
                         comments=self.id in ("opencode", "gemini"), url_keys=url_keys)

    def _plan_codex_server(self, part: Part, files: Files, ctx: Context, path: Path) -> None:
        doc = TomlDoc(files.load(path))
        servers = doc.get(["mcp_servers"]) or {}
        core = f"[mcp_servers.{SERVER_NAME}]\nurl = {toml_string(ctx.mcp_url)}\n"
        where = f"{path}: [mcp_servers.{SERVER_NAME}]"
        name = ("mcp_servers", SERVER_NAME)
        if isinstance(servers.get(SERVER_NAME), dict):
            entry = servers[SERVER_NAME]
            url = entry.get("url") if isinstance(entry.get("url"), str) else None
            if url and same_server(url, ctx.base_url):
                if role_of_url(url) == ctx.role:
                    doc.check_table(name, own_header=True)
                    part.present(path, f"{where} points at this server",
                                 {"text": core, "logical": {"format": "toml", "action": "adopt",
                                                            "name": list(name), "url": url}})
                    return
                _edit_into(part, files, doc.replace_block(name, core), f"{where}: address set to the {ctx.role} endpoint")
                return
            if not ctx.force:
                part.refuse(f"{where} points at {url or 'something else'}",
                            "Run connect with --force to replace that table, or remove it by hand.")
                return
            _edit_into(part, files, doc.replace_block(name, core), f"{where} replaced (--force)")
            return
        others = [key for key, entry in servers.items()
                  if isinstance(entry, dict) and same_server(entry.get("url") if isinstance(entry.get("url"), str)
                                                            else None, ctx.base_url)]
        if others:
            other = others[0]
            if not ctx.adopt:
                part.refuse(f"{path}: [mcp_servers.{toml_key((other,))}] points at this server",
                            f"Run connect with --adopt to rename it to {SERVER_NAME!r}, or remove it by hand.")
                return
            _edit_into(part, files, doc.rename_table(("mcp_servers", other), name),
                       f"{path}: [mcp_servers.{toml_key((other,))}] renamed (--adopt)")
            if role_of_url(servers[other].get("url")) != ctx.role:
                doc = TomlDoc(files.load(path))
                _edit_into(part, files, doc.replace_block(name, core), f"address set to the {ctx.role} endpoint")
            return
        _edit_into(part, files, doc.append(core), f"{where} added")

    def _plan_permissions(self, part: Part, files: Files, ctx: Context, paths: dict[str, Path]) -> None:
        path = paths["permissions"]
        if self.id == "claude":  # Claude Code writes `allow` before `deny` when it saves the file
            plan_json_items(part, files, path, ["permissions", "allow"],
                            [f"mcp__{SERVER_NAME}__{tool}" for tool in ctx.tools], comments=False,
                            label=f"the {len(ctx.tools)} tools", first=True)
        elif self.id == "zcode":
            plan_json_items(part, files, path, ["permission", "allowedTools"],
                            [f"mcp__{SERVER_NAME}__{tool}" for tool in ctx.tools], comments=False,
                            label=f"the {len(ctx.tools)} tools")
        elif self.id == "opencode":
            plan_json_items(part, files, path, ["permissions"],
                            [{"action": f"{SERVER_NAME}_{tool}", "resource": "*", "effect": "allow"}
                             for tool in ctx.tools], comments=True, label=f"the {len(ctx.tools)} tools")
        elif self.id == "gemini":
            self._plan_gemini_trust(part, files, path)
        elif self.id == "codex":
            self._plan_codex_permissions(part, files, ctx, path)

    def _plan_gemini_trust(self, part: Part, files: Files, path: Path) -> None:
        doc = JsonDoc(files.load(path), comments=True)
        entry = doc.get(["mcpServers", SERVER_NAME])
        if not isinstance(entry, dict):
            part.skip(f"{path}: no {SERVER_NAME!r} entry to trust")
            return
        if entry.get("trust") is True:
            part.present(path, f"{path}: mcpServers.{SERVER_NAME}.trust is true",
                         {"text": "true", "logical": {"format": "json", "action": "adopt",
                                                      "path": ["mcpServers", SERVER_NAME, "trust"], "value": True}})
            return
        if "trust" in entry:
            part.skip(f"{path}: mcpServers.{SERVER_NAME}.trust is {json.dumps(entry['trust'])}",
                      f"Set mcpServers.{SERVER_NAME}.trust to true by hand to allow its tools without a prompt.")
            return
        _edit_into(part, files, doc.insert_member(["mcpServers", SERVER_NAME], "trust", True),
                   f"{path}: mcpServers.{SERVER_NAME}.trust set")

    def _plan_codex_permissions(self, part: Part, files: Files, ctx: Context, path: Path) -> None:
        doc = TomlDoc(files.load(path))
        entry = doc.get(["mcp_servers", SERVER_NAME])
        if not isinstance(entry, dict):
            part.skip(f"{path}: no [mcp_servers.{SERVER_NAME}] table")
            return
        if entry.get("default_tools_approval_mode") == "approve":
            part.present(path, f"{path}: default_tools_approval_mode is approve",
                         {"text": "approve", "logical": {"format": "toml", "action": "adopt_approval",
                                                         "tools": list(ctx.tools), "default": True}})
            return
        tools = entry.get("tools") if isinstance(entry.get("tools"), dict) else {}
        missing = []
        for tool in ctx.tools:
            table = tools.get(tool)
            if isinstance(table, dict) and "approval_mode" in table:
                if table["approval_mode"] != "approve":
                    part.refuse(f"{path}: [mcp_servers.{SERVER_NAME}.tools.{tool}] approval_mode is "
                                f"{table['approval_mode']!r}", f"Set approval_mode = \"approve\" for {tool} by hand.")
                    return
            elif isinstance(table, dict):
                part.refuse(f"{path}: [mcp_servers.{SERVER_NAME}.tools.{tool}] exists without approval_mode",
                            f"Add approval_mode = \"approve\" to that table by hand.")
                return
            else:
                missing.append(tool)
        if not missing:
            part.present(path, f"{path}: the {len(ctx.tools)} tools are approved",
                         {"text": "approve", "logical": {"format": "toml", "action": "adopt_approval",
                                                         "tools": list(ctx.tools)}})
            return
        core = "\n".join(f"[mcp_servers.{SERVER_NAME}.tools.{tool}]\napproval_mode = \"approve\"\n"
                         for tool in missing)
        _edit_into(part, files, doc.append(core), f"{path}: approval_mode = \"approve\" for {len(missing)} tools")

    def _plan_hook(self, part: Part, files: Files, ctx: Context, paths: dict[str, Path]) -> None:
        if self.hook_budget is None:
            part.skip("not confirmed: no session-start hook for this agent (FACTS.md)")
            return
        path = paths["hook"]
        command = self.hook_command(ctx.manifest)
        hook = {"type": "command", "command": command, "timeout": HOOK_TIMEOUT_S}
        if self.id == "claude":
            plan_json_hook(part, files, path, ["hooks", "SessionStart"], {"matcher": HOOK_MATCHER, "hooks": [hook]},
                           command, comments=False)
            if part.status != "refused":
                plan_json_hook(part, files, path, ["hooks", "UserPromptSubmit"], {"hooks": [hook]}, command,
                               comments=False)
        elif self.id == "codex":
            if not path.parent.is_dir():
                part.skip(f"{path.parent} does not exist")
                return
            plan_json_hook(part, files, path, ["hooks", "SessionStart"], {"matcher": HOOK_MATCHER, "hooks": [hook]},
                           command, comments=False)
            if part.status == "install":
                part.notes.append("Codex runs a new or changed hook only after it is trusted in /hooks.")
        elif self.id == "zcode":
            doc = JsonDoc(files.load(path), comments=False)
            hooks = doc.get(["hooks"])
            group = {"matcher": HOOK_MATCHER, "hooks": [hook]}
            if hooks is None:
                edit = doc.insert_member([], "hooks", {"enabled": True, "events": {"SessionStart": [group]}})
                _edit_into(part, files, edit, f"{path}: hooks (enabled) with a SessionStart hook added")
                return
            if not isinstance(hooks, dict) or hooks.get("enabled") is not True:
                part.skip(f"{path}: hooks.enabled is not true",
                          "Set hooks.enabled to true by hand, then run connect again.")
                return
            plan_json_hook(part, files, path, ["hooks", "events", "SessionStart"], group, command, comments=False)
        elif self.id == "gemini":
            doc = JsonDoc(files.load(path), comments=True)
            if doc.get(["hooksConfig", "enabled"]) is False:
                part.skip(f"{path}: hooksConfig.enabled is false", "Enable hooks by hand, then run connect again.")
                return
            gemini_hook = {"name": "chronicle-brief", "type": "command", "command": command,
                           "timeout": HOOK_TIMEOUT_S * 1000}
            plan_json_hook(part, files, path, ["hooks", "SessionStart"], {"matcher": "*", "hooks": [gemini_hook]},
                           command, comments=True)

    def _plan_block(self, part: Part, files: Files, ctx: Context, paths: dict[str, Path], *, hook: bool) -> None:
        path = paths["block"]
        if self.id == "opencode":
            plan_block(part, files, ctx, path, hook=hook, create=False, create_manual=(
                f"Create {path} with the block of `connect --print`: without it opencode reads "
                "~/.claude/CLAUDE.md, so connect does not create it."))
            return
        plan_block(part, files, ctx, path, hook=hook)

    # Printing -----------------------------------------------------------------
    def snippets(self, ctx: Context, env: Mapping[str, str]) -> list[tuple[str, str]]:
        """What to add by hand: (where, text) pairs."""
        paths = self.paths(env)
        tools = ctx.tools
        command = self.hook_command(ctx.manifest) if self.hook_budget else None
        hook = {"type": "command", "command": command, "timeout": HOOK_TIMEOUT_S}
        out: list[tuple[str, str]] = []

        def js(value: Any) -> str:
            return json.dumps(value, indent=2, ensure_ascii=False)

        if self.id == "claude":
            out.append((f"{paths['server']} (mcpServers)", js({SERVER_NAME: self.server_value(ctx)})))
            out.append((f"{paths['permissions']} (permissions.allow)", js([f"mcp__{SERVER_NAME}__{t}" for t in tools])))
            out.append((f"{paths['hook']} (hooks)", js({"SessionStart": [{"matcher": HOOK_MATCHER, "hooks": [hook]}],
                                                        "UserPromptSubmit": [{"hooks": [hook]}]})))
        elif self.id == "codex":
            out.append((str(paths["server"]), f"[mcp_servers.{SERVER_NAME}]\nurl = {toml_string(ctx.mcp_url)}\n\n" +
                        "\n".join(f"[mcp_servers.{SERVER_NAME}.tools.{t}]\napproval_mode = \"approve\"\n" for t in tools)))
            out.append((f"{paths['hook']} (hooks)", js({"SessionStart": [{"matcher": HOOK_MATCHER, "hooks": [hook]}]})))
        elif self.id == "zcode":
            out.append((f"{paths['server']} (mcp.servers)", js({SERVER_NAME: self.server_value(ctx)})))
            out.append((f"{paths['permissions']} (permission.allowedTools)",
                        js([f"mcp__{SERVER_NAME}__{t}" for t in tools])))
            out.append((f"{paths['hook']} (hooks)", js({"enabled": True, "events": {
                "SessionStart": [{"matcher": HOOK_MATCHER, "hooks": [hook]}]}})))
        elif self.id == "opencode":
            out.append((f"{paths['server']} (mcp.servers)", js({SERVER_NAME: self.server_value(ctx)})))
            out.append((f"{paths['permissions']} (permissions)",
                        js([{"action": f"{SERVER_NAME}_{t}", "resource": "*", "effect": "allow"} for t in tools])))
        elif self.id == "gemini":
            out.append((f"{paths['server']} (mcpServers)", js({SERVER_NAME: {**self.server_value(ctx), "trust": True}})))
            out.append((f"{paths['hook']} (hooks)", js({"SessionStart": [{"matcher": "*", "hooks": [
                {"name": "chronicle-brief", **hook, "timeout": HOOK_TIMEOUT_S * 1000}]}]})))
        elif self.id == "cursor":
            out.append((f"{paths['server']} (mcpServers)", js({SERVER_NAME: self.server_value(ctx)})))
        else:
            out.append(("the agent's MCP configuration (a streamable HTTP server)",
                        js({"mcpServers": {SERVER_NAME: self.server_value(ctx)}})))
        return out


ADAPTERS: dict[str, Adapter] = {
    adapter.id: adapter for adapter in (
        Adapter("claude", "Claude Code", ("claude-code",), DEFAULT_HOOK_BUDGET),
        Adapter("codex", "Codex", ("codex-cli",), CODEX_HOOK_BUDGET),
        Adapter("zcode", "ZCode", ("glm",), DEFAULT_HOOK_BUDGET, "zcode"),
        Adapter("opencode", "opencode", (), None),
        Adapter("gemini", "Gemini CLI", ("gemini-cli",), DEFAULT_HOOK_BUDGET, "gemini"),
        Adapter("cursor", "Cursor", (), None, print_only=True),
    )
}
GENERIC = Adapter("other", "an agent without an adapter", (), None, print_only=True)


def find_adapter(name: str) -> Adapter | None:
    folded = name.strip().lower()
    for adapter in ADAPTERS.values():
        if folded == adapter.id or folded in adapter.aliases:
            return adapter
    return None


# ---------------------------------------------------------------------------
# Presence of what the ledger recorded
# ---------------------------------------------------------------------------


def _doc_for(path: Path, logical: dict[str, Any], loaded: Loaded | None = None):
    """The document a recorded change lives in. JSON is read as JSONC: strict JSON reads the same."""
    loaded = loaded if loaded is not None else load(path)
    fmt = logical.get("format")
    if fmt == "toml":
        return TomlDoc(loaded)
    if fmt == "text":
        return TextDoc(loaded)
    return JsonDoc(loaded, comments=True)


def walk_back(changes: list[dict[str, Any]], files: Files) -> list[tuple[dict[str, Any], bool, str, Edit | None]]:
    """Recorded changes, newest first, each tested by taking it back after the newer ones, as disconnect does.

    Each change that can be taken back is applied to *files* (planned, not
    written). An adopted entry is checked against the state left by that.
    """
    results: list[tuple[dict[str, Any], bool, str, Edit | None]] = []
    for change in reversed(changes):
        path = Path(change["file"])
        try:
            loaded = files.load(path)
        except EditRefused as exc:
            results.append((change, False, exc.reason, None))
            continue
        if change["status"] == "adopted":
            present, reason = change_present(change, loaded)
            results.append((change, present, "" if present else reason, None))
            continue
        try:
            edit = _doc_for(path, change["logical"], loaded).undo(change)
        except EditRefused as exc:
            results.append((change, False, exc.reason, None))
            continue
        files.apply(edit)
        results.append((change, True, "", edit))
    return results


def change_present(change: dict[str, Any], loaded: Loaded | None = None) -> tuple[bool, str]:
    """Whether a recorded change is still in its file as written (an adopted entry: still as it was)."""
    path = Path(change["file"])
    logical = change["logical"]
    try:
        doc = _doc_for(path, logical, loaded)
    except EditRefused as exc:
        return False, exc.reason
    if change["status"] == "adopted":
        action = logical.get("action")
        if logical.get("format") == "json":
            if action == "adopt_items":
                existing = doc.get(logical["path"]) or []
                return all(item in existing for item in logical["items"]), "entries removed"
            return doc.get(logical["path"]) == logical["value"], "entry changed"
        if logical.get("format") == "toml":
            if action == "adopt":
                return doc.get([*logical["name"], "url"]) == logical["url"], "table changed"
            entry = doc.get(["mcp_servers", SERVER_NAME]) or {}
            if logical.get("default"):
                return entry.get("default_tools_approval_mode") == "approve", "approval changed"
            tools = entry.get("tools") or {}
            return all((tools.get(tool) or {}).get("approval_mode") == "approve" for tool in logical["tools"]), \
                "approval changed"
        if logical.get("format") == "text":
            span = doc.find_block(BLOCK_BEGIN, BLOCK_END)
            return span is not None and doc.text[span[0]:span[1]] == change["piece"], "block changed"
        return False, "unknown entry"
    try:
        doc.undo(change)
    except EditRefused as exc:
        return False, exc.reason
    return True, ""


# ---------------------------------------------------------------------------
# Running a connection
# ---------------------------------------------------------------------------


def workspace_of(manifest: Path) -> Path:
    return Path(manifest).resolve().parent


def ledger_path(manifest: Path) -> Path:
    return workspace_of(manifest) / LEDGER_NAME


@dataclass
class Outcome:
    """A command's result: the payload for --json, the lines for a person, the diff it showed or would make."""

    payload: dict[str, Any]
    exit_code: int
    lines: list[str] = field(default_factory=list)
    diff: str = ""
    summary: list[str] = field(default_factory=list)
    shown: bool = False  # lines and diff were already printed, before a question


Ask = Callable[[str, list[str], str], bool]


def _plan_connection(adapter: Adapter, ctx: Context, ledger: Ledger, env: Mapping[str, str]):
    """Undo of a connection of another role or address, then the parts of this one, on the planned files."""
    files = Files()
    reverts: list[tuple[Edit, dict[str, Any]]] = []
    lost: list[dict[str, Any]] = []
    released: list[dict[str, Any]] = []
    previous = ledger.connections.get(adapter.id)
    switching = bool(previous) and (previous.get("role"), previous.get("url")) != (ctx.role, ctx.mcp_url)
    # Another role or address: what connect wrote comes out first. The same: it stays, and is only checked.
    walked = walk_back(ledger.active(adapter.id), files if switching else Files())
    for change, present, reason, edit in walked:
        if change["status"] == "adopted":
            if switching or not present:
                released.append(change)
        elif not present:
            lost.append({**change, "lost_reason": reason})
        elif switching and edit is not None:
            reverts.append((edit, change))
    parts = adapter.plan(files, ctx, env)
    return files, parts, reverts, lost, released


def _record_event(manifest: Path, db: Path | None, text: str, connection: dict[str, Any]) -> str | None:
    from .runtime_context import load_manifest
    from .service import record_event

    loaded = load_manifest(manifest)
    if db is not None:
        loaded = {**loaded, "paths": {**loaded.get("paths", {}), "chronicle_db": str(db)}}
    stored = record_event(loaded, {"agent": OPERATOR, "agent_source": "explicit", "category": EVENT_CATEGORY,
                                   "text": text, "connection": connection},
                          dedupe=False, hash_dedupe=False, source_kind="chronicle_cli",
                          imported_from="chronicle.cli.connect")
    return stored.get("id")


def _ask(prompt: str, stream_in=None, stream_out=None) -> bool:
    stream_in = stream_in or sys.stdin
    stream_out = stream_out or sys.stdout
    print(prompt, file=stream_out, flush=True)
    answer = stream_in.readline()
    return answer.strip().lower() in ("y", "yes")


def _display(path: str, env: Mapping[str, str]) -> str:
    home = str(_home(env))
    return "~" + path[len(home):] if path.startswith(home + os.sep) else path


def run_connect(adapter: Adapter, *, role: str, manifest: Path, url: str | None, by: str | None, yes: bool,
                dry_run: bool, force: bool, adopt: bool, env: Mapping[str, str], db: Path | None = None,
                ask: Ask | None = None, server: dict[str, Any] | None = None) -> Outcome:
    payload: dict[str, Any] = {"command": "connect", "agent": adapter.id, "role": role, "status": None}
    if not adapter.installed(env):
        payload.update(status="not_installed", error=f"{adapter.label} is not installed here: "
                       f"{adapter.paths(env).get('home')} does not exist")
        return Outcome(payload, EXIT_UNSUPPORTED, [payload["error"]])
    if server is None:
        try:
            server = preflight(url, workspace_of(manifest), manifest)
        except PreflightFailed as exc:
            payload.update(status="preflight_failed", error=exc.reason, hint=exc.hint)
            return Outcome(payload, EXIT_PREFLIGHT, [f"Preflight failed: {exc.reason}", exc.hint])
    ctx = Context(adapter.id, role, server["base_url"], manifest, force=force, adopt=adopt)
    payload.update(url=ctx.mcp_url, server={"url": server["base_url"], "version": server["version"],
                                             "address_from": server["source"]})
    if server.get("notes"):
        payload["notes"] = list(server["notes"])
    ledger = Ledger(ledger_path(manifest))
    for change in ledger.pending():
        _settle_pending(change, ledger)
    try:
        files, parts, reverts, lost, released = _plan_connection(adapter, ctx, ledger, env)
    except EditRefused as exc:
        payload.update(status="refused", error=f"{exc.path}: {exc.reason}", manual=exc.manual)
        return Outcome(payload, EXIT_REFUSED, [f"Refused: {exc.path}: {exc.reason}", exc.manual or ""])
    payload["parts"] = {name: part.as_dict() for name, part in parts.items()}
    edits = [edit for edit, _ in reverts] + [edit for part in parts.values() for edit in part.edits]
    diff = "".join(edit.diff() for edit in edits)
    payload["diff"] = diff
    refused = [name for name, part in parts.items() if part.status == "refused"]
    lines = [f"Chronicle {server['version']} at {server['base_url']}; {adapter.label}, role {role}: {ctx.mcp_url}"]
    lines += [_part_line(name, part, env) for name, part in parts.items()]
    if refused:
        payload["status"] = "refused"
        return Outcome(payload, EXIT_REFUSED, lines, diff, ["Nothing was changed."])
    retiring = {change["id"] for _, change in reverts} | {change["id"] for change in lost + released}
    adoptions = [(name, item) for name, part in parts.items() for item in part.adoptions
                 if not _tracked(ledger, adapter.id, name, item, retiring)]
    if not edits and not adoptions and not lost and not released and _same_connection(ledger, adapter.id, ctx):
        payload["status"] = "nothing_to_do"
        return Outcome(payload, EXIT_OK, lines, diff, ["Nothing to do: connected as asked."])
    if dry_run:
        payload["status"] = "dry_run"
        return Outcome(payload, EXIT_OK, lines, diff, ["Dry run: nothing was changed."])
    shown = False
    if edits and not yes:
        shown = True
        if ask is None or not ask("Apply these changes? [y/N]", lines, diff):
            payload["status"] = "declined"
            return Outcome(payload, EXIT_REFUSED, lines, diff, ["Not applied."], shown)
    changed = files.changed_on_disk()
    if changed:
        payload.update(status="refused", error=f"changed on disk after it was read: {', '.join(changed)}",
                       manual="Run the command again; it reads the files anew.")
        return Outcome(payload, EXIT_REFUSED, lines, diff,
                       [f"Refused: {payload['error']}. Nothing was changed.", payload["manual"]], shown)
    extra = {"role": role, "url": ctx.mcp_url}
    try:
        for edit, change in reverts:
            ledger.revert(change, write(edit), by="connect")
        for name, part in parts.items():
            for edit in part.edits:
                change = ledger.begin(edit, agent=adapter.id, part=name, extra=extra)
                try:
                    result = write(edit)
                except (EditRefused, OSError) as exc:
                    ledger.finish(change, None, status="failed", error=str(exc))
                    raise
                ledger.finish(change, result)
    except (EditRefused, OSError) as exc:
        reason = exc.reason if isinstance(exc, EditRefused) else f"{type(exc).__name__}: {exc}"
        payload.update(status="refused", error=reason, manual="Some changes were made: run `chronicle disconnect "
                       f"{adapter.id}` to take them back, or connect again.")
        return Outcome(payload, EXIT_REFUSED, lines, diff, [f"Refused while writing: {reason}", payload["manual"]],
                       shown)
    for change in lost:
        _mark(ledger, change["id"], "lost", change.get("lost_reason"))
    for change in released:
        _mark(ledger, change["id"], "released", None)
    for name, item in adoptions:
        ledger.adopt(agent=adapter.id, part=name, path=Path(item["path"]), text=item["text"],
                     logical=item["logical"], extra=extra)
    installed = {name: ("installed" if part.status == "install" else part.status) for name, part in parts.items()}
    for name, status in installed.items():
        payload["parts"][name]["status"] = status
    if not shown:
        lines = lines[:1] + [_part_line(name, part, env, done=True) for name, part in parts.items()]
    ledger.connections[adapter.id] = {"role": role, "url": ctx.mcp_url, "base_url": ctx.base_url,
                                      "at": _now(), "by": by, "parts": installed}
    ledger.save()
    connection = {"action": "connect", "target": adapter.id, "role": role, "url": ctx.mcp_url, "parts": installed,
                  "by": by, "by_self_declared": True}
    text = (f"Connected {adapter.label} to Chronicle with role {role}: "
            + ", ".join(f"{name} {status}" for name, status in installed.items())
            + (f". Run by {by} (self-declared)." if by else "."))
    try:
        payload["event_id"] = _record_event(manifest, db, text, connection)
    except Exception as exc:  # noqa: BLE001 - the files are connected; the report says the event is missing
        payload["event_error"] = f"{type(exc).__name__}: {exc}"
    payload["status"] = "done"
    notes = [note for part in parts.values() for note in part.notes]
    summary = ["Connected."] + notes
    if payload.get("event_id"):
        summary.append(f"Event {payload['event_id']} recorded ({EVENT_CATEGORY}).")
    return Outcome(payload, EXIT_OK, lines, diff, summary, shown)


def _tracked(ledger: Ledger, agent: str, part: str, item: dict[str, Any], retiring: set[str]) -> bool:
    """Whether the ledger already holds this part's entry in this file (not counting what this run retires)."""
    return any(change["file"] == item["path"] and change["id"] not in retiring for change in ledger.active(agent, part))


def _same_connection(ledger: Ledger, agent: str, ctx: Context) -> bool:
    previous = ledger.connections.get(agent)
    return bool(previous) and (previous.get("role"), previous.get("url")) == (ctx.role, ctx.mcp_url)


def _mark(ledger: Ledger, change_id: str, status: str, reason: str | None) -> None:
    for change in ledger.changes:
        if change["id"] == change_id:
            change["status"] = status
            change[f"{status}_at"] = _now()
            if reason:
                change[f"{status}_reason"] = reason
    ledger.save()


def _settle_pending(change: dict[str, Any], ledger: Ledger) -> None:
    """A change left pending by an interrupted run: applied when the file holds its result, else failed."""
    digest = sha256_file(Path(change["file"]))
    status = "applied" if digest == change["sha256_after"] else "failed"
    ledger.finish(change, None, status=status, error=None if status == "applied" else "interrupted")


def _now() -> str:
    from .connect_edit import utc_now

    return utc_now()


def _part_line(name: str, part: Part, env: Mapping[str, str], *, done: bool = False) -> str:
    label = {"install": "installed" if done else "install", "present": "present", "skipped": "skipped",
             "refused": "REFUSED"}[part.status]
    detail = part.detail.replace(str(_home(env)) + os.sep, "~" + os.sep)
    line = f"  {name:<12} {label:<9} {detail}"
    if part.manual and part.status in ("refused", "skipped"):
        line += f"\n  {'':<12} {'':<9} by hand: {part.manual}"
    return line


def run_disconnect(adapter: Adapter, *, manifest: Path, by: str | None, yes: bool, dry_run: bool,
                   env: Mapping[str, str], db: Path | None = None, ask: Ask | None = None) -> Outcome:
    payload: dict[str, Any] = {"command": "disconnect", "agent": adapter.id, "status": None}
    ledger = Ledger(ledger_path(manifest))
    for change in ledger.pending():
        _settle_pending(change, ledger)
    active = ledger.active(adapter.id)
    if not active and adapter.id not in ledger.connections:
        payload["status"] = "nothing_to_do"
        return Outcome(payload, EXIT_OK, [f"{adapter.label} is not connected by chronicle connect: nothing to do."])
    files = Files()
    undo: list[tuple[Edit, dict[str, Any]]] = []
    left: list[dict[str, Any]] = []
    for change, present, reason, edit in walk_back([c for c in active if c["status"] != "adopted"], files):
        if present and edit is not None:
            undo.append((edit, change))
        else:
            left.append({"part": change["part"], "file": change["file"], "reason": reason})
    diff = "".join(edit.diff() for edit, _ in undo)
    payload["diff"] = diff
    payload["removed"] = [{"part": change["part"], "file": change["file"]} for _, change in undo]
    payload["left"] = left
    payload["kept"] = [{"part": change["part"], "file": change["file"]} for change in active
                       if change["status"] == "adopted"]
    lines = [f"{adapter.label}: remove {len(undo)} piece(s) that connect wrote."]
    lines += [f"  {item['part']:<12} remove   {_display(item['file'], env)}" for item in payload["removed"]]
    lines += [f"  {item['part']:<12} kept     {_display(item['file'], env)} (it was there before connect)"
              for item in payload["kept"]]
    lines += [f"  {item['part']:<12} left     {_display(item['file'], env)}: {item['reason']}" for item in left]
    if dry_run:
        payload["status"] = "dry_run"
        return Outcome(payload, EXIT_OK, lines, diff, ["Dry run: nothing was changed."])
    shown = False
    if undo and not yes:
        shown = True
        if ask is None or not ask("Remove these changes? [y/N]", lines, diff):
            payload["status"] = "declined"
            return Outcome(payload, EXIT_REFUSED, lines, diff, ["Not removed."], shown)
    changed = files.changed_on_disk()
    if changed:
        payload.update(status="refused", error=f"changed on disk after it was read: {', '.join(changed)}")
        return Outcome(payload, EXIT_REFUSED, lines, diff, [f"Refused: {payload['error']}. Nothing was changed."],
                       shown)
    for edit, change in undo:
        ledger.revert(change, write(edit), by="disconnect")
    for change in active:
        if change["status"] == "adopted":
            _mark(ledger, change["id"], "released", None)
    for item in left:
        for change in active:
            if change["file"] == item["file"] and change["part"] == item["part"] and change["status"] == "applied":
                _mark(ledger, change["id"], "lost", item["reason"])
    previous = ledger.connections.pop(adapter.id, None) or {}
    ledger.save()
    earliest: dict[str, str | None] = {}
    for _, change in undo:  # newest first: the last one seen for a file is its first change
        earliest[change["file"]] = change["sha256_before"]
    payload["restored"] = {path: sha256_file(Path(path)) == digest for path, digest in earliest.items()}
    connection = {"action": "disconnect", "target": adapter.id, "role": previous.get("role"),
                  "parts": sorted({change["part"] for _, change in undo}), "by": by, "by_self_declared": True}
    text = (f"Disconnected {adapter.label} from Chronicle: removed "
            + (", ".join(connection["parts"]) or "nothing") + (f". Run by {by} (self-declared)." if by else "."))
    try:
        payload["event_id"] = _record_event(manifest, db, text, connection)
    except Exception as exc:  # noqa: BLE001 - the files are restored; the report says the event is missing
        payload["event_error"] = f"{type(exc).__name__}: {exc}"
    payload["status"] = "done"
    summary = ["Disconnected."]
    if payload.get("event_id"):
        summary.append(f"Event {payload['event_id']} recorded ({EVENT_CATEGORY}).")
    return Outcome(payload, EXIT_OK, lines, diff, summary, shown)


# ---------------------------------------------------------------------------
# Status: `chronicle agents` and `chronicle doctor --agents`
# ---------------------------------------------------------------------------


def _last_writes(manifest: Path, db: Path | None) -> Callable[[str], str | None]:
    try:
        from .config import read_toml
        from .identity import Registry

        raw = read_toml(manifest)
        registry = Registry.from_manifest(raw)
        db_path = db or Path(os.path.expanduser(str((raw.get("paths") or {}).get("chronicle_db")
                                                    or workspace_of(manifest) / "chronicle.db")))
    except Exception:  # noqa: BLE001 - a status table without this column is still a status table
        return lambda agent: None
    if not db_path.is_file():
        return lambda agent: None

    def last(agent: str) -> str | None:
        try:
            connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
            try:
                row = connection.execute("SELECT max(recorded_at_utc) FROM events WHERE actor = ?",
                                         (registry.agent(agent),)).fetchone()
            finally:
                connection.close()
        except sqlite3.Error:
            return None
        return row[0] if row else None

    return last


def agent_status(manifest: Path, env: Mapping[str, str], *, db: Path | None = None) -> list[dict[str, Any]]:
    ledger = Ledger(ledger_path(manifest))
    last = _last_writes(manifest, db)
    rows = []
    for adapter in ADAPTERS.values():
        connection = ledger.connections.get(adapter.id) or {}
        parts = connection.get("parts") or {}
        rows.append({
            "agent": adapter.id,
            "label": adapter.label,
            "installed": adapter.installed(env),
            "connected": bool(connection),
            "role": connection.get("role"),
            "url": connection.get("url"),
            "hook": parts.get("hook") if connection else ("print only" if adapter.print_only else None),
            "block": parts.get("block") if connection else None,
            "last_write": last(adapter.id),
        })
    return rows


def doctor_agents(manifest: Path, env: Mapping[str, str], url_arg: str | None) -> dict[str, Any]:
    """Every connected agent's parts checked against the ledger and the server; drift is named."""
    ledger = Ledger(ledger_path(manifest))
    base_url, source = resolve_server_url(url_arg, workspace_of(manifest))
    agents = []
    drift = 0
    for agent, connection in sorted(ledger.connections.items()):
        adapter = ADAPTERS.get(agent)
        issues: list[dict[str, str]] = []
        for change, present, reason, _ in walk_back(ledger.active(agent), Files()):
            if not present:
                issues.append({"part": change["part"], "file": change["file"],
                               "problem": reason or "not as connect left it"})
        for change in ledger.changes:
            if change["agent"] == agent and change["status"] in ("pending", "lost"):
                issues.append({"part": change["part"], "file": change["file"], "problem": change["status"]})
        if base_url and not same_server(connection.get("url"), base_url):
            issues.append({"part": "server", "file": "", "problem":
                           f"the server answers at {base_url}, the agent is connected to {connection.get('url')}"})
        drift += len(issues)
        agents.append({"agent": agent, "role": connection.get("role"), "url": connection.get("url"),
                       "issues": issues, "installed": bool(adapter and adapter.installed(env)),
                       "fix": (f"{cli_command(manifest)} connect {agent} --role {connection.get('role')} --yes"
                               if issues else None)})
    server: dict[str, Any] = {"url": base_url, "address_from": source}
    if base_url:
        try:
            status, health, _ = _http_json(f"{base_url}/health")
            server.update(answering=isinstance(health, dict) and "status" in health,
                          version=(health or {}).get("version") if isinstance(health, dict) else None)
        except (URLError, OSError, HTTPException) as exc:
            server.update(answering=False, error=f"{type(exc).__name__}: {exc}")
    else:
        server["answering"] = False
    if ledger.connections and not server.get("answering"):
        drift += 1
    return {"status": "drift" if drift else "ok", "issue_count": drift, "server": server, "agents": agents}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def add_parsers(sub: argparse._SubParsersAction) -> None:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="Print one JSON object instead of text")
    connect = sub.add_parser(
        "connect", parents=[common], formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Connect an agent to this Chronicle server: MCP entry, tool permissions, brief hook, protocol block",
        description=(
            "Connect a coding agent to the Chronicle HTTP server with a role: write (/mcp, six tools) or read "
            "(/mcp/read, five tools, no record_event). Edits only the agent's user-level files, one piece of text "
            "per change, recorded in the workspace's ledger; disconnect takes them back. Without an agent, lists "
            "the agents. A role is a guard-rail, not a security boundary.\n\n"
            "agents: " + ", ".join(f"{a.id} ({a.label})" for a in ADAPTERS.values()) + "; cursor only with --print."),
        epilog=EXIT_CODES_HELP)
    connect.add_argument("agent", nargs="?", help="claude, codex, zcode, opencode, gemini, cursor")
    connect.add_argument("--role", choices=sorted(ROLES), default="write", help="write (default) or read")
    connect.add_argument("--url", default=None, help="Server address (default: CHRONICLE_SERVER_URL, then the "
                                                     "address file the server writes to the workspace)")
    connect.add_argument("--by", default=None, help="Who runs this, recorded as a self-declared label")
    connect.add_argument("--yes", action="store_true", help="Apply without asking")
    connect.add_argument("--dry-run", action="store_true", help="Show what would change; change nothing")
    connect.add_argument("--adopt", action="store_true",
                         help="Rename an entry of another name that points at this server to 'chronicle'")
    connect.add_argument("--force", action="store_true", help="Replace a 'chronicle' entry that points elsewhere")
    connect.add_argument("--print", dest="print_only", action="store_true",
                         help="Print the snippets and the protocol block to add by hand; change nothing")
    connect.set_defaults(handler=cmd_connect)

    disconnect = sub.add_parser("disconnect", parents=[common], formatter_class=argparse.RawDescriptionHelpFormatter,
                                help="Take back exactly what connect wrote for an agent", epilog=EXIT_CODES_HELP)
    disconnect.add_argument("agent")
    disconnect.add_argument("--by", default=None, help="Who runs this, recorded as a self-declared label")
    disconnect.add_argument("--yes", action="store_true", help="Apply without asking")
    disconnect.add_argument("--dry-run", action="store_true", help="Show what would change; change nothing")
    disconnect.set_defaults(handler=cmd_disconnect)

    agents = sub.add_parser("agents", parents=[common],
                            help="Agents: installed, connected, role, hook, block, last write")
    agents.set_defaults(handler=cmd_agents)


def _emit(outcome: Outcome, as_json: bool, *, show_diff: bool = False) -> int:
    if as_json:
        print(json.dumps({**outcome.payload, "exit_code": outcome.exit_code}, ensure_ascii=False, indent=2))
        return outcome.exit_code
    if not outcome.shown:
        for line in outcome.lines:
            if line:
                print(line)
        if show_diff and outcome.diff:
            print(outcome.diff, end="")
    for line in outcome.summary:
        if line:
            print(line)
    return outcome.exit_code


def _ask_with_diff(prompt: str, lines: list[str], diff: str) -> bool:
    for line in lines:
        if line:
            print(line)
    print(diff, end="")
    return _ask(prompt)


def _windows() -> bool:
    return os.name == "nt" or sys.platform.startswith(("win", "cygwin"))


def _unsupported(message: str, as_json: bool) -> int:
    return _emit(Outcome({"status": "unsupported", "error": message}, EXIT_UNSUPPORTED, [message]), as_json)


def cmd_connect(args: argparse.Namespace) -> int:
    env = os.environ
    manifest = Path(args.manifest)
    if _windows():
        return _unsupported("chronicle connect runs on macOS and Linux; Windows is not supported.", args.json)
    if args.print_only:
        return _print_snippets(args, env)
    if not args.agent:
        return cmd_agents(args)
    adapter = find_adapter(args.agent)
    if adapter is None:
        return _unsupported(f"no adapter for {args.agent!r}: use `connect {args.agent} --print` and add it by hand",
                            args.json)
    if adapter.print_only:
        return _unsupported(f"{adapter.label} is connected by hand: run `chronicle connect {adapter.id} --print`",
                            args.json)
    if args.json and not (args.yes or args.dry_run):
        return _emit(Outcome({"status": "refused", "error": "--json needs --yes or --dry-run"}, EXIT_REFUSED,
                             ["--json needs --yes or --dry-run"]), True)
    result = run_connect(adapter, role=args.role, manifest=manifest, url=args.url, by=args.by, yes=args.yes,
                         dry_run=args.dry_run, force=args.force, adopt=args.adopt, env=env, db=args.db,
                         ask=_ask_with_diff)
    return _emit(result, args.json, show_diff=args.dry_run)


def cmd_disconnect(args: argparse.Namespace) -> int:
    env = os.environ
    if _windows():
        return _unsupported("chronicle disconnect runs on macOS and Linux; Windows is not supported.", args.json)
    adapter = find_adapter(args.agent)
    if adapter is None or adapter.print_only:
        return _unsupported(f"no adapter for {args.agent!r}", args.json)
    if args.json and not (args.yes or args.dry_run):
        return _emit(Outcome({"status": "refused", "error": "--json needs --yes or --dry-run"}, EXIT_REFUSED,
                             ["--json needs --yes or --dry-run"]), True)
    result = run_disconnect(adapter, manifest=Path(args.manifest), by=args.by, yes=args.yes, dry_run=args.dry_run,
                            env=env, db=args.db, ask=_ask_with_diff)
    return _emit(result, args.json, show_diff=args.dry_run)


def cmd_agents(args: argparse.Namespace) -> int:
    rows = agent_status(Path(args.manifest), os.environ, db=getattr(args, "db", None))
    if getattr(args, "json", False):
        print(json.dumps({"agents": rows}, ensure_ascii=False, indent=2))
        return EXIT_OK
    header = ("agent", "installed", "connected", "role", "hook", "block", "last write")
    table = [header] + [(row["agent"], "yes" if row["installed"] else "no", "yes" if row["connected"] else "no",
                         row["role"] or "-", row["hook"] or "-", row["block"] or "-", row["last_write"] or "-")
                        for row in rows]
    widths = [max(len(str(line[index])) for line in table) for index in range(len(header))]
    for line in table:
        print("  ".join(str(cell).ljust(width) for cell, width in zip(line, widths)).rstrip())
    return EXIT_OK


def cmd_doctor_agents(args: argparse.Namespace) -> int:
    report = doctor_agents(Path(args.manifest), os.environ, getattr(args, "server_url", None))
    code = EXIT_DRIFT if report["status"] != "ok" else EXIT_OK
    if not getattr(args, "summary", False):  # JSON by default, as `chronicle doctor`
        print(json.dumps({**report, "exit_code": code}, ensure_ascii=False, indent=2))
        return code
    server = report["server"]
    print(f"Server: {server.get('url') or 'no address'} "
          f"({'answering' if server.get('answering') else 'not answering'})")
    for agent in report["agents"]:
        state = "ok" if not agent["issues"] else f"{len(agent['issues'])} issue(s)"
        print(f"{agent['agent']}: role {agent['role']}, {state}")
        for issue in agent["issues"]:
            print(f"  {issue['part']}: {issue['problem']} {issue['file']}".rstrip())
        if agent["fix"]:
            print(f"  fix: {agent['fix']}")
    return code


def _print_snippets(args: argparse.Namespace, env: Mapping[str, str]) -> int:
    manifest = Path(args.manifest)
    adapter = find_adapter(args.agent) if args.agent else None
    adapter = adapter or GENERIC
    base_url, source = resolve_server_url(args.url, workspace_of(manifest))
    if not base_url:
        message = ("no server address: pass --url, set CHRONICLE_SERVER_URL, or start the HTTP server. "
                   + _start_command(manifest))
        return _emit(Outcome({"status": "preflight_failed", "error": message}, EXIT_PREFLIGHT, [message]), args.json)
    ctx = Context(adapter.id, args.role, base_url, manifest)
    snippets = adapter.snippets(ctx, env)
    hook = adapter.hook_budget is not None
    block = render_block(ctx, hook=hook)
    payload = {"command": "connect --print", "agent": adapter.id if adapter is not GENERIC else args.agent,
               "role": args.role, "url": ctx.mcp_url, "tools": list(ctx.tools),
               "snippets": [{"where": where, "text": text} for where, text in snippets], "block": block,
               "status": "printed"}
    if args.json:
        print(json.dumps({**payload, "exit_code": EXIT_OK}, ensure_ascii=False, indent=2))
        return EXIT_OK
    print(f"{adapter.label}, role {args.role}: {ctx.mcp_url} (tools: {', '.join(ctx.tools)})")
    print(f"Server address from {source}.")
    for where, text in snippets:
        print(f"\n# {_display(where, env)}\n{text}")
    print(f"\n# Protocol block for the agent's instruction file\n{block}", end="")
    return EXIT_OK
