"""The session-start brief hook that `chronicle connect` installs.

Run as ``<absolute python> -m max_chronicle.brief_hook --manifest <path> --agent <id>``
with optional ``--budget <characters>`` and ``--format text|zcode|gemini``. The
agent passes the hook's JSON on stdin (``cwd``, the session id, the event).

The brief comes from the HTTP server's ``/brief``. When the server is silent
(no address, no answer, an error), one line says so and the brief is built
from the database in this process, without the server. The output stays
within the budget (Codex keeps about 2,500 tokens of a hook's output: 4,500
characters). It always exits 0: whatever happens, the session starts.

Nothing heavy is imported before the HTTP request: only the standard library
and ``server_address``. The database brief imports the rest when it is needed.

Claude Code calls it on SessionStart and on UserPromptSubmit; the second gives
the brief only when the first did not (a marker per session in the temporary
directory records that a brief was given).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import select
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from http.client import HTTPException

from .server_address import open_server, resolve_server_url

SERVER_TIMEOUT_S = 2.0  # the server answers /brief within this, or the hook stops waiting
STDIN_WAIT_S = 0.5  # an agent that leaves stdin open is not waited on longer
DEFAULT_BUDGET = 6000
MIN_DATABASE_BUDGET = 1000  # the smallest brief the database builder makes
MAX_BUDGET = 8000
SESSION_EVENTS = ("SessionStart",)
PROMPT_EVENT = "UserPromptSubmit"
MARKER_PREFIX = "chronicle-brief-"
_SAFE = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")

NOTICE_NO_ADDRESS = ("Chronicle's server is not running (no address for it in the workspace). The memory brief "
                     "below was read straight from the database: complete up to its last write.")
NOTICE_SILENT = ("Chronicle's server did not answer at {url} ({why}). The memory brief below was read straight "
                 "from the database: complete up to its last write. Its tools may fail until the server is back.")
NOTICE_UNWELL = ("Chronicle's server answered at {url} without a brief ({why}). The memory brief below was read "
                 "straight from the database.")
NO_BRIEF = ("No Chronicle memory brief could be read, from the server or the database. What earlier sessions "
            "decided is not in front of you: check files and git, and say so when you act without it.")


def _arguments(argv: list[str]) -> dict[str, str]:
    """``--name value`` pairs; this hook takes no other form (argparse is not imported for it)."""
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        name = argv[index]
        if name.startswith("--") and index + 1 < len(argv):
            values[name[2:]] = argv[index + 1]
            index += 2
        else:
            index += 1
    return values


def _stdin_payload() -> dict:
    """The hook's JSON from stdin, or {} when there is none (a terminal, an empty or open pipe)."""
    stream = sys.stdin
    if stream is None or stream.closed:
        return {}
    try:
        if stream.isatty():
            return {}
        chunks: list[str] = []
        deadline = time.monotonic() + STDIN_WAIT_S
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            ready, _, _ = select.select([stream], [], [], remaining)
            if not ready:
                break
            chunk = os.read(stream.fileno(), 65536)
            if not chunk:
                break
            chunks.append(chunk.decode("utf-8", errors="replace"))
        payload = json.loads("".join(chunks) or "{}")
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _project_dir(cwd: str | None) -> str | None:
    """The repository top level holding *cwd* (a ``.git`` entry found upward), else *cwd*."""
    if not cwd:
        return None
    path = Path(cwd)
    for candidate in (path, *path.parents):
        if (candidate / ".git").exists():
            return str(candidate)
    return cwd


def _marker(agent: str, session: str | None) -> Path | None:
    if not session:
        return None
    safe = "".join(char if char in _SAFE else "_" for char in session)[:120]
    return Path(os.environ.get("TMPDIR") or "/tmp") / f"{MARKER_PREFIX}{agent}-{safe}"


def _fit(text: str, budget: int) -> str:
    """*text* within *budget* characters with its final newline, cut at a line end."""
    text = text.rstrip("\n")
    room = budget - 1  # the newline that ends the output
    if len(text) <= room:
        return text
    limit = room - 2  # "\n…" marks the cut
    cut = text.rfind("\n", 0, limit)
    cut = cut if cut > limit // 2 else limit
    return text[:cut].rstrip() + "\n…"


def _server_brief(url: str, budget: int, project_dir: str | None) -> tuple[str | None, str | None, bool]:
    """The server's brief, or why there is none and whether the server answered at all."""
    query = {"budget": str(max(MIN_DATABASE_BUDGET, min(budget, MAX_BUDGET)))}
    if project_dir:
        query["cwd"] = project_dir
    try:
        with open_server(f"{url}/brief?{urlencode(query)}", timeout=SERVER_TIMEOUT_S) as response:
            text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        return None, f"HTTP {exc.code}", True
    except (URLError, OSError, HTTPException) as exc:
        reason = getattr(exc, "reason", exc)
        return None, f"{type(exc).__name__}: {reason}", False
    if not text.strip():
        return None, "an empty brief", True
    return text, None, True


def _database_brief(manifest: Path, budget: int, project_dir: str | None) -> str | None:
    try:
        from .brief import build_brief
        from .runtime_context import load_manifest

        room = max(MIN_DATABASE_BUDGET, min(budget, MAX_BUDGET))
        return build_brief(load_manifest(manifest), cwd=project_dir, max_chars=room)["text"]
    except Exception:  # noqa: BLE001 - the hook never fails a session start
        return None


def brief_text(manifest: Path, budget: int, project_dir: str | None) -> str:
    """The text the session gets: the server's brief, or a notice and the database's brief."""
    workspace = manifest.resolve().parent
    url, _ = resolve_server_url(None, workspace)
    if url:
        text, why, answered = _server_brief(url, budget, project_dir)
        if text is not None:
            return _fit(text, budget)
        notice = (NOTICE_UNWELL if answered else NOTICE_SILENT).format(url=url, why=why)
    else:
        notice = NOTICE_NO_ADDRESS
    from_database = _database_brief(manifest, budget - len(notice) - 2, project_dir)
    if not from_database:
        return _fit(f"{notice}\n{NO_BRIEF}", budget)
    return _fit(f"{notice}\n\n{from_database}", budget)


def run(argv: list[str]) -> int:
    options = _arguments(argv)
    manifest = options.get("manifest")
    agent = options.get("agent", "agent")
    try:
        budget = int(options.get("budget", DEFAULT_BUDGET))
    except ValueError:
        budget = DEFAULT_BUDGET
    output = options.get("format", "text")
    payload = _stdin_payload()
    event = str(payload.get("hook_event_name") or payload.get("hookEventName") or "SessionStart")
    session = payload.get("session_id") or payload.get("sessionId")
    marker = _marker(agent, str(session) if session else None)
    if event == PROMPT_EVENT and (marker is None or marker.exists()):
        return 0  # the session has its brief, or cannot be told apart from one that has
    if not manifest:
        text = NO_BRIEF
    else:
        cwd = payload.get("cwd") if isinstance(payload.get("cwd"), str) else None
        text = brief_text(Path(manifest), budget, _project_dir(cwd or os.getcwd()))
    if output in ("zcode", "gemini", "json"):
        sys.stdout.write(json.dumps({"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}},
                                    ensure_ascii=False) + "\n")
    else:
        sys.stdout.write(text if text.endswith("\n") else text + "\n")
    sys.stdout.flush()
    if marker is not None:
        try:
            marker.touch()
        except OSError:
            pass
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run(sys.argv[1:] if argv is None else argv)
    except BaseException:  # noqa: BLE001 - always exit 0: a hook must never stop a session
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
