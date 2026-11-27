"""The brief hook that `chronicle connect` installs: server first, the database when the server is silent."""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from max_chronicle import server_address
from max_chronicle.brief_hook import NOTICE_NO_ADDRESS, NO_BRIEF
from max_chronicle.service import record_event

REPO = Path(__file__).resolve().parents[1]


class _BriefServer(BaseHTTPRequestHandler):
    status = 200
    body = "Chronicle memory brief from the server.\n- one\n"
    seen: list[dict] = []

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        parts = urlsplit(self.path)
        type(self).seen.append({"path": parts.path, "query": parse_qs(parts.query)})
        raw = self.body.encode()
        self.send_response(self.status)
        self.send_header("content-type", "text/plain; charset=utf-8")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


@pytest.fixture()
def brief_server():
    servers = []

    def start(**attributes) -> str:
        handler = type("Handler", (_BriefServer,), {"seen": [], **attributes})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append((server, handler))
        return f"http://127.0.0.1:{server.server_address[1]}"

    start.handlers = lambda: [handler for _, handler in servers]
    yield start
    for server, _ in servers:
        server.shutdown()
        server.server_close()


def _hook(sandbox, tmp_path, payload: dict | str | None, *args: str, env: dict | None = None) -> subprocess.CompletedProcess:
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("CHRONICLE_") and key != "PYTHONPATH"}
    environment.update({"TMPDIR": str(tmp_path), "OLLAMA_URL": "http://127.0.0.1:1",
                        "PYTHONPATH": str(REPO)})
    environment.update(env or {})
    data = payload if isinstance(payload, str) else json.dumps(payload or {})
    return subprocess.run([sys.executable, "-m", "max_chronicle.brief_hook", "--manifest",
                           str(sandbox.manifest_path), *args], input=data, capture_output=True, text=True,
                          env=environment, timeout=30)


def _closed_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def recorded(loaded_manifest):
    record_event(loaded_manifest, {"agent": "claude", "category": "decision", "project": "demo",
                                   "text": "Decided the harbour opens at nine"}, append_compat=False)
    return loaded_manifest


def test_with_the_server_up_the_brief_comes_from_it(chronicle_sandbox, tmp_path, brief_server) -> None:
    url = brief_server()
    server_address.write_address(chronicle_sandbox.status_root, url, version="0.33.0", pid=1)

    result = _hook(chronicle_sandbox, tmp_path, {"session_id": "a", "hook_event_name": "SessionStart",
                                                 "cwd": str(tmp_path)}, "--agent", "claude")

    assert result.returncode == 0 and result.stdout == "Chronicle memory brief from the server.\n- one\n"
    (handler,) = brief_server.handlers()
    assert handler.seen[0]["path"] == "/brief" and handler.seen[0]["query"]["budget"] == ["6000"]


def test_the_session_directory_is_sent_as_its_repository_top(chronicle_sandbox, tmp_path, brief_server) -> None:
    url = brief_server()
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    (repo / "src" / "deep").mkdir(parents=True)

    _hook(chronicle_sandbox, tmp_path, {"cwd": str(repo / "src" / "deep")}, "--agent", "codex",
          env={"CHRONICLE_SERVER_URL": url})

    (handler,) = brief_server.handlers()
    assert handler.seen[0]["query"]["cwd"] == [str(repo)]


def test_without_a_running_server_the_brief_comes_from_the_database(chronicle_sandbox, tmp_path, recorded) -> None:
    result = _hook(chronicle_sandbox, tmp_path, {"session_id": "b"}, "--agent", "claude")

    assert result.returncode == 0
    assert result.stdout.startswith(NOTICE_NO_ADDRESS)
    assert "harbour opens at nine" in result.stdout


def test_a_silent_server_gets_one_line_and_the_database_brief(chronicle_sandbox, tmp_path, recorded) -> None:
    server_address.write_address(chronicle_sandbox.status_root, f"http://127.0.0.1:{_closed_port()}",
                                 version="0.33.0", pid=1)

    result = _hook(chronicle_sandbox, tmp_path, {"session_id": "c"}, "--agent", "claude")

    first, rest = result.stdout.split("\n", 1)
    assert result.returncode == 0 and first.startswith("Chronicle's server did not answer at http://127.0.0.1:")
    assert "harbour opens at nine" in rest


def test_a_server_that_answers_without_a_brief_is_said_so(chronicle_sandbox, tmp_path, recorded, brief_server) -> None:
    url = brief_server(status=503, body="Brief unavailable: TimeoutError\n")

    result = _hook(chronicle_sandbox, tmp_path, {}, "--agent", "claude", env={"CHRONICLE_SERVER_URL": url})

    assert result.stdout.startswith(f"Chronicle's server answered at {url} without a brief (HTTP 503)")
    assert "harbour opens at nine" in result.stdout


@pytest.mark.parametrize("server_up", [True, False])
def test_the_codex_budget_is_kept(chronicle_sandbox, tmp_path, loaded_manifest, brief_server, server_up) -> None:
    for index in range(60):
        record_event(loaded_manifest, {"agent": "codex", "category": "decision", "project": "demo",
                                       "text": f"Decision {index}: " + "a long line of reasoning " * 12},
                     append_compat=False)
    env = {"CHRONICLE_SERVER_URL": brief_server(body="x" * 7000 + "\n")} if server_up else {}

    result = _hook(chronicle_sandbox, tmp_path, {}, "--agent", "codex", "--budget", "4500", env=env)

    assert result.returncode == 0 and 1000 < len(result.stdout) <= 4500


def test_a_prompt_gets_the_brief_only_when_the_session_start_did_not(chronicle_sandbox, tmp_path, brief_server) -> None:
    env = {"CHRONICLE_SERVER_URL": brief_server()}
    start = {"session_id": "d", "hook_event_name": "SessionStart"}
    prompt = {"session_id": "d", "hook_event_name": "UserPromptSubmit"}

    assert _hook(chronicle_sandbox, tmp_path, start, "--agent", "claude", env=env).stdout
    assert _hook(chronicle_sandbox, tmp_path, prompt, "--agent", "claude", env=env).stdout == ""

    other = {"session_id": "e", "hook_event_name": "UserPromptSubmit"}
    assert _hook(chronicle_sandbox, tmp_path, other, "--agent", "claude", env=env).stdout  # no SessionStart came
    assert _hook(chronicle_sandbox, tmp_path, other, "--agent", "claude", env=env).stdout == ""
    assert _hook(chronicle_sandbox, tmp_path, {"hook_event_name": "UserPromptSubmit"}, "--agent", "claude",
                 env=env).stdout == ""


@pytest.mark.parametrize("output", ["zcode", "gemini"])
def test_json_output_carries_the_brief_as_additional_context(chronicle_sandbox, tmp_path, brief_server, output) -> None:
    env = {"CHRONICLE_SERVER_URL": brief_server()}

    result = _hook(chronicle_sandbox, tmp_path, {"hook_event_name": "SessionStart", "sessionId": "f"},
                   "--agent", output, "--format", output, env=env)

    payload = json.loads(result.stdout)
    assert payload == {"hookSpecificOutput": {"hookEventName": "SessionStart",
                                              "additionalContext": "Chronicle memory brief from the server.\n- one"}}


@pytest.mark.parametrize("stdin", ["not json", "[1, 2]", ""])
def test_any_input_exits_zero(chronicle_sandbox, tmp_path, stdin) -> None:
    result = _hook(chronicle_sandbox, tmp_path, stdin, "--agent", "claude")

    assert result.returncode == 0 and result.stdout.strip()


def test_a_missing_workspace_still_exits_zero_with_a_notice(tmp_path) -> None:
    result = subprocess.run([sys.executable, "-m", "max_chronicle.brief_hook", "--manifest",
                             str(tmp_path / "nowhere" / "SSOT_MANIFEST.toml"), "--agent", "claude"],
                            input="{}", capture_output=True, text=True, timeout=30,
                            env={**os.environ, "TMPDIR": str(tmp_path), "PYTHONPATH": str(REPO)})

    assert result.returncode == 0 and NO_BRIEF in result.stdout


def test_nothing_heavy_is_imported_before_the_request(tmp_path) -> None:
    probe = ("import sys, max_chronicle.brief_hook as hook; "
             "print(sorted(name for name in sys.modules if name.split('.')[0] in "
             "('pydantic', 'mcp', 'sqlite3', 'anyio', 'starlette', 'max_chronicle')))")

    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=30,
                            env={**os.environ, "PYTHONPATH": str(REPO)})

    assert json.loads(result.stdout.replace("'", '"')) == [
        "max_chronicle", "max_chronicle.brief_hook", "max_chronicle.server_address"]


def test_a_proxy_in_the_environment_is_not_used_for_a_loopback_server(chronicle_sandbox, tmp_path,
                                                                      brief_server) -> None:
    url = brief_server()
    server_address.write_address(chronicle_sandbox.status_root, url, version="0.33.0", pid=1)
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("CHRONICLE_") and key.lower() not in ("pythonpath", "no_proxy")}
    environment.update({"TMPDIR": str(tmp_path), "OLLAMA_URL": "http://127.0.0.1:1", "PYTHONPATH": str(REPO)})
    environment.update({name: "http://127.0.0.1:9" for name in ("http_proxy", "HTTP_PROXY", "all_proxy", "ALL_PROXY")})

    result = subprocess.run([sys.executable, "-m", "max_chronicle.brief_hook", "--manifest",
                             str(chronicle_sandbox.manifest_path), "--agent", "claude"],
                            input=json.dumps({"session_id": "p", "hook_event_name": "SessionStart"}),
                            capture_output=True, text=True, env=environment, timeout=30)

    assert result.returncode == 0 and result.stdout == "Chronicle memory brief from the server.\n- one\n"
