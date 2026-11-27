"""End-to-end smoke test of `chronicle connect` against a real HTTP server.

Run from the checkout with its virtual environment active:

    python tests/tools/connect_smoke.py [--keep]

It creates a temporary workspace and a temporary HOME holding copies of the
synthetic agent configs in ``tests/fixtures/connect/home``, starts the HTTP
server on a free port, and runs every check of the acceptance table's row S.
Each check prints one ``PASS <label>`` or ``FAIL <label>: <detail>`` line; the
last line is ``SMOKE OK (<n> checks)`` with exit code 0, or ``SMOKE FAILED``
with exit code 1. Nothing outside the temporary directory is read or written
except the fixtures, which are only copied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.error import HTTPError
from urllib.request import Request, urlopen

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures" / "connect" / "home"
AGENTS = ("claude", "codex", "zcode", "opencode", "gemini")
AGENT_FILES = {
    "claude": [".claude.json", ".claude/settings.json", ".claude/CLAUDE.md"],
    "codex": [".codex/config.toml", ".codex/hooks.json", ".codex/AGENTS.md"],
    "zcode": [".zcode/cli/config.json", ".zcode/AGENTS.md"],
    "opencode": [".config/opencode/opencode.jsonc", ".config/opencode/AGENTS.md"],
    "gemini": [".gemini/settings.json", ".gemini/GEMINI.md"],
}
WRITE_TOOLS = {"startup_bundle", "query_memory", "get_memory", "recent_events", "state_at", "record_event"}
READ_TOOLS = WRITE_TOOLS - {"record_event"}
CONFIRMED = {"server": "installed", "permissions": "installed", "hook": "installed", "block": "installed"}
SERVER_START_TIMEOUT_S = 60.0
POLL_S = 0.1
COMMAND_TIMEOUT_S = 120.0
MCP_VERSION = "2025-06-18"
CODEX_BUDGET = 4500
STRIPPED_ENV = ("CHRONICLE_", "MCP_", "OLLAMA_", "CLAUDE_", "CODEX_", "GEMINI_", "XDG_", "ZCODE_")


class Smoke:
    """The checks, their results, and the temporary installation they run against."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.workspace = root / "ws"
        self.manifest = self.workspace / "SSOT_MANIFEST.toml"
        self.tmp = root / "tmp"
        self.results: list[tuple[str, bool, str]] = []
        self.server: subprocess.Popen | None = None
        self.base_url = ""
        self.homes = 0

    # -- environment -------------------------------------------------------

    def env(self, home: Path | None = None, **extra: str) -> dict[str, str]:
        env = {key: value for key, value in os.environ.items() if not key.startswith(STRIPPED_ENV)}
        env.update({"HOME": str(home or self.root / "home"), "CHRONICLE_ROOT": str(self.workspace),
                    "OLLAMA_URL": "http://127.0.0.1:1", "PYTHONPATH": str(REPO), "TMPDIR": str(self.tmp),
                    "PYTHONUNBUFFERED": "1"})
        env.update(extra)
        return env

    def new_home(self) -> Path:
        self.homes += 1
        home = self.root / f"home-{self.homes}"
        shutil.copytree(FIXTURES, home)
        return home

    def cli(self, *args: str, home: Path | None = None, stdin: str | None = None) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-m", "max_chronicle.cli", "--manifest", str(self.manifest), *args],
                              input=stdin, capture_output=True, text=True, env=self.env(home),
                              timeout=COMMAND_TIMEOUT_S)

    def check(self, label: str, test: Callable[[], tuple[bool, str] | bool]) -> bool:
        try:
            outcome = test()
            passed, detail = outcome if isinstance(outcome, tuple) else (bool(outcome), "")
        except Exception as exc:  # noqa: BLE001 - a crashed check is a failed check, the others still run
            passed, detail = False, f"{type(exc).__name__}: {exc}"
        self.results.append((label, passed, detail))
        print(f"PASS {label}" if passed else f"FAIL {label}: {detail}", flush=True)
        return passed

    # -- installation ----------------------------------------------------------

    def prepare(self) -> None:
        self.tmp.mkdir(parents=True)
        shutil.copytree(FIXTURES, self.root / "home")
        subprocess.run([sys.executable, "-m", "max_chronicle.cli", "init", "--root", str(self.workspace)],
                       env=self.env(), check=True, capture_output=True, timeout=COMMAND_TIMEOUT_S)
        notes = self.workspace / "notes"
        notes.mkdir()
        (notes / "harbour.md").write_text("# Harbour\n\nThe harbour opens at nine.\n", encoding="utf-8")
        with self.manifest.open("a", encoding="utf-8") as stream:
            stream.write(f'\n[notes]\npaths = ["{notes}/*.md"]\nsync_minutes = 60\n')
        automation = self.workspace / "CHRONICLE_AUTOMATION.toml"
        text = automation.read_text(encoding="utf-8")
        # The scheduler runs, with no job due: its thread is counted, nothing runs in the background.
        text = re.sub(r"\n\[jobs\.[a-z_]+\]\n(?:[a-z_]+ = [^\n]*\n)*", "\n", text)
        automation.write_text(text + "\n[scheduler]\nenabled = true\n", encoding="utf-8")

    def start_server(self) -> None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        log = open(self.root / "server.log", "ab")  # noqa: SIM115 - kept open for the server's lifetime
        self.server = subprocess.Popen(
            [sys.executable, "-m", "max_chronicle.mcp_server", "--manifest", str(self.manifest),
             "--transport", "streamable-http", "--migrate"],
            env=self.env(MCP_HOST="127.0.0.1", MCP_PORT=str(port)), stdout=log, stderr=log)
        address = self.workspace / "chronicle-server.json"
        deadline = time.monotonic() + SERVER_START_TIMEOUT_S
        while time.monotonic() < deadline:
            if address.exists():
                self.base_url = json.loads(address.read_text())["url"]
                return
            if self.server.poll() is not None:
                raise RuntimeError(f"the server exited with {self.server.returncode}; see {self.root / 'server.log'}")
            time.sleep(POLL_S)
        raise RuntimeError("the server wrote no address in time")

    def stop_server(self) -> None:
        if self.server is None:
            return
        self.server.send_signal(signal.SIGTERM)
        try:
            self.server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.server.kill()
            self.server.wait()
        self.server = None

    # -- helpers ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        with urlopen(f"{self.base_url}/health", timeout=5) as response:  # noqa: S310 - local
            return json.loads(response.read())

    def mcp(self, path: str) -> "Mcp":
        return Mcp(f"{self.base_url}{path}")

    def counts(self) -> tuple[int, int]:
        connection = sqlite3.connect(f"file:{self.workspace / 'chronicle.db'}?mode=ro", uri=True)
        try:
            return (connection.execute("SELECT COUNT(*) FROM events").fetchone()[0],
                    connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0])
        finally:
            connection.close()

    def ledger(self) -> dict[str, Any]:
        path = self.workspace / "connect-ledger.json"
        return json.loads(path.read_text()) if path.exists() else {"changes": [], "connections": {}}


class Mcp:
    """An MCP client over streamable HTTP: the initialize handshake, then requests."""

    def __init__(self, url: str) -> None:
        self.url, self.next_id = url, 1
        reply, headers = self._post({"method": "initialize", "params": {
            "protocolVersion": MCP_VERSION, "capabilities": {}, "clientInfo": {"name": "smoke", "version": "1"}}})
        if "result" not in reply:
            raise RuntimeError(f"initialize failed: {reply}")
        self.headers = {"mcp-session-id": headers["mcp-session-id"], "mcp-protocol-version": MCP_VERSION}
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"}, self.headers)

    def _send(self, message: dict, headers: dict[str, str] | None = None) -> tuple[str, dict[str, str]]:
        request = Request(self.url, data=json.dumps(message).encode(), method="POST", headers={
            "content-type": "application/json", "accept": "application/json, text/event-stream", **(headers or {})})
        try:
            with urlopen(request, timeout=30) as response:  # noqa: S310 - local
                return response.read().decode(), {key.lower(): value for key, value in response.headers.items()}
        except HTTPError as exc:
            return exc.read().decode(), {}

    def _post(self, message: dict, headers: dict[str, str] | None = None) -> tuple[dict, dict[str, str]]:
        body, reply_headers = self._send({"jsonrpc": "2.0", "id": self.next_id, **message}, headers)
        self.next_id += 1
        for line in body.splitlines():
            if line.startswith("data:"):
                return json.loads(line[5:].strip()), reply_headers
        return (json.loads(body) if body.strip().startswith("{") else {"error": body}), reply_headers

    def request(self, method: str, params: dict | None = None) -> dict:
        return self._post({"method": method, "params": params or {}}, self.headers)[0]

    def tools(self) -> set[str]:
        return {tool["name"] for tool in self.request("tools/list")["result"]["tools"]}

    def call(self, name: str, arguments: dict) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})


def payload_of(reply: dict) -> dict:
    result = reply["result"]
    if result.get("isError"):
        raise RuntimeError(result["content"][0]["text"])
    return json.loads(result["content"][0]["text"])


def sha(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def shas(home: Path, agent: str) -> dict[str, str | None]:
    return {name: sha(home / name) for name in AGENT_FILES[agent]}


def all_bytes(home: Path) -> dict[str, bytes]:
    return {str(path.relative_to(home)): path.read_bytes() for path in sorted(home.rglob("*"))
            if path.is_file() and ".bak-" not in path.name}


def parses(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".toml":
        tomllib.loads(text)
    elif path.suffix in (".json", ".jsonc"):
        from max_chronicle.connect_edit import mask_comments

        json.loads(mask_comments(text)[0])


def json_out(result: subprocess.CompletedProcess) -> dict:
    return json.loads(result.stdout)


class StubHealth(BaseHTTPRequestHandler):
    """A server that says it is Chronicle 0.32.1."""

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        body = json.dumps({"status": "ok", "version": "0.32.1", "mcp_endpoints": [{"path": "/mcp", "role": "write"}]})
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body.encode())


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------


def server_checks(smoke: Smoke) -> None:
    def health_endpoints():
        health = smoke.health()
        endpoints = {item["path"]: item["role"] for item in health["mcp_endpoints"]}
        ok = endpoints == {"/mcp": "write", "/mcp/read": "read"} and health["version"] == "0.33.0"
        return ok, f"{health.get('version')} {endpoints}"

    def read_tools():
        read, write = smoke.mcp("/mcp/read").tools(), smoke.mcp("/mcp").tools()
        return read == READ_TOOLS and write == WRITE_TOOLS, f"read {sorted(read)}, write {sorted(write)}"

    def read_refuses_write():
        before = smoke.counts()
        reader = smoke.mcp("/mcp/read")
        reply = reader.call("record_event", {"text": "Must not be stored by the read endpoint", "agent": "smoke"})
        refused = "error" in reply or bool(reply.get("result", {}).get("isError"))
        payload_of(reader.call("startup_bundle", {"capture": True, "agent": "smoke-reader"}))
        after = smoke.counts()
        return refused and after == before, f"refused={refused}, events/snapshots {before} -> {after}"

    def write_works():
        stored = payload_of(smoke.mcp("/mcp").call("record_event", {
            "text": "Smoke decision: the pelican harbour gate opens at dawn", "agent": "smoke-writer",
            "category": "decision"}))
        found = payload_of(smoke.mcp("/mcp/read").call("query_memory", {"query": "pelican harbour gate"}))
        ids = [hit["id"] for hit in found["results"]]
        return f"event:{stored['id']}" in ids, f"stored {stored['id']}, found {ids}"

    def jobs_once():
        threads = smoke.health()["threads"]
        wanted = {"chronicle-notes-sync": 1, "chronicle-embedder": 1, "chronicle-scheduler": 1}
        return all(threads.get(name) == count for name, count in wanted.items()), str(threads)

    smoke.check("health-endpoints", health_endpoints)
    smoke.check("read-tools", read_tools)
    smoke.check("read-refuses-write", read_refuses_write)
    smoke.check("write-works", write_works)
    smoke.check("jobs-once", jobs_once)


def connect_checks(smoke: Smoke) -> None:
    home = smoke.root / "home"
    before = {agent: {name: (home / name).read_bytes() for name in AGENT_FILES[agent]} for agent in AGENTS}
    reports: dict[str, dict] = {}

    for agent in AGENTS:
        def connected(agent=agent):
            result = smoke.cli("connect", agent, "--yes", "--json", "--by", "smoke-runner", home=home)
            report = json_out(result)
            reports[agent] = report
            statuses = {name: part["status"] for name, part in report["parts"].items()}
            wanted = dict(CONFIRMED, hook="skipped") if agent == "opencode" else CONFIRMED
            for name in AGENT_FILES[agent]:
                parses(home / name)
            return result.returncode == 0 and statuses == wanted, f"exit {result.returncode}, {statuses}"

        smoke.check(f"connect-{agent}", connected)

    changes = smoke.ledger()["changes"]
    for agent in AGENTS:
        def outside(agent=agent):
            for name in AGENT_FILES[agent]:
                text = (home / name).read_text(encoding="utf-8")
                for change in reversed([c for c in changes if c["agent"] == agent and c["file"] == str(home / name)
                                        and c["status"] == "applied"]):
                    start = change["offset"]
                    if text[start:start + len(change["piece"])] != change["piece"]:
                        return False, f"{name}: the piece is not where the ledger says"
                    text = text[:start] + change["original"] + text[start + len(change["piece"]):]
                if text.encode("utf-8") != before[agent][name]:
                    return False, f"{name} differs outside the pieces"
            return True, ""

        smoke.check(f"bytes-outside-{agent}", outside)

    for agent in AGENTS:
        def idempotent(agent=agent):
            files = all_bytes(home)
            result = smoke.cli("connect", agent, "--yes", "--json", home=home)
            return (result.returncode == 0 and all_bytes(home) == files
                    and json_out(result)["status"] == "nothing_to_do"), f"exit {result.returncode}"

        smoke.check(f"idempotent-{agent}", idempotent)

    def role_switch():
        settings, claude_json, block = home / ".claude/settings.json", home / ".claude.json", home / ".claude/CLAUDE.md"
        result = smoke.cli("connect", "claude", "--role", "read", "--yes", "--json", home=home)
        url = json.loads(claude_json.read_text())["mcpServers"]["chronicle"]["url"]
        allow = json.loads(settings.read_text())["permissions"]["allow"]
        text = block.read_text()
        ok = (result.returncode == 0 and url == f"{smoke.base_url}/mcp/read"
              and allow == [f"mcp__chronicle__{tool}" for tool in
                            ("startup_bundle", "query_memory", "get_memory", "recent_events", "state_at")]
              and "role=read" in text and "role=write" not in text)
        return ok, f"exit {result.returncode}, url {url}, {len(allow)} allowed"

    smoke.check("role-switch", role_switch)

    def event_recorded():
        connection = sqlite3.connect(f"file:{smoke.workspace / 'chronicle.db'}?mode=ro", uri=True)
        try:
            rows = connection.execute("SELECT actor, payload_json FROM events WHERE category = 'agent_connection'"
                                      ).fetchall()
        finally:
            connection.close()
        for actor, payload in rows:
            details = json.loads(payload).get("connection") or {}
            if (actor == "operator" and details.get("target") == "codex" and details.get("role") == "write"
                    and details.get("parts", {}).get("server") == "installed" and details.get("by") == "smoke-runner"
                    and details.get("by_self_declared") is True):
                return True, ""
        return False, f"{len(rows)} agent_connection events, none for codex by smoke-runner"

    smoke.check("event-recorded", event_recorded)

    for agent in AGENTS:
        def disconnected(agent=agent):
            result = smoke.cli("disconnect", agent, "--yes", "--json", home=home)
            now = {name: (home / name).read_bytes() for name in AGENT_FILES[agent]}
            differs = [name for name in AGENT_FILES[agent] if now[name] != before[agent][name]]
            return result.returncode == 0 and not differs, f"exit {result.returncode}, differs: {differs}"

        smoke.check(f"disconnect-{agent}", disconnected)


def refusal_checks(smoke: Smoke) -> None:
    def refused(label: str, damage: Callable[[Path], Path], agent: str = "claude") -> None:
        def test():
            home = smoke.new_home()
            target = damage(home)
            files = all_bytes(home)
            result = smoke.cli("connect", agent, "--yes", "--json", home=home)
            report = json_out(result)
            manual = [part.get("manual") for part in report.get("parts", {}).values() if part["status"] == "refused"]
            return (result.returncode != 0 and all_bytes(home) == files and any(manual)), \
                f"exit {result.returncode}, manual {manual}, target {target.name}"
        smoke.check(label, test)

    def symlink(home: Path) -> Path:
        path = home / ".claude/settings.json"
        real = home / "settings-real.json"
        path.rename(real)
        path.symlink_to(real)
        return path

    def crlf(home: Path) -> Path:
        path = home / ".claude/settings.json"
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
        return path

    def bom(home: Path) -> Path:
        path = home / ".gemini/settings.json"
        path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())
        return path

    def duplicate(home: Path) -> Path:
        path = home / ".zcode/cli/config.json"
        path.write_text(path.read_text().replace('  "model": {', '  "mcp": {},\n  "model": {'), encoding="utf-8")
        return path

    refused("refuse-symlink", symlink)
    refused("refuse-crlf", crlf)
    refused("refuse-bom", bom, "gemini")
    refused("refuse-duplicate-key", duplicate, "zcode")

    def changed_under():
        home = smoke.new_home()
        path = home / ".claude/settings.json"
        process = subprocess.Popen(
            [sys.executable, "-m", "max_chronicle.cli", "--manifest", str(smoke.manifest), "connect", "claude"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=smoke.env(home))
        seen = []
        for line in process.stdout:
            seen.append(line)
            if "Apply these changes? [y/N]" in line:
                break
        edited = path.read_text().replace('"effortLevel": "high"', '"effortLevel": "low"')
        path.write_text(edited, encoding="utf-8")
        files = all_bytes(home)
        process.stdin.write("y\n")
        process.stdin.flush()
        rest = process.stdout.read()
        code = process.wait(timeout=COMMAND_TIMEOUT_S)
        output = "".join(seen) + rest
        return (code != 0 and all_bytes(home) == files and "changed on disk" in output
                and "Run the command again" in output), f"exit {code}"

    smoke.check("refuse-changed-under", changed_under)


def existing_checks(smoke: Smoke) -> None:
    def same_url():
        home = smoke.new_home()
        path = home / ".claude.json"
        data = json.loads(path.read_text())
        data["mcpServers"]["chronicle"] = {"type": "http", "url": f"{smoke.base_url}/mcp"}
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        made = path.read_bytes()
        result = smoke.cli("connect", "claude", "--yes", "--json", home=home)
        adopted = [c for c in smoke.ledger()["changes"] if c["agent"] == "claude" and c["part"] == "server"
                   and c["status"] == "adopted" and c["file"] == str(path)]
        status = json_out(result)["parts"]["server"]["status"]
        smoke.cli("disconnect", "claude", "--yes", home=home)
        return (result.returncode == 0 and status == "present" and len(adopted) == 1
                and path.read_bytes() == made), f"exit {result.returncode}, server {status}, {len(adopted)} adopted"

    def other_url():
        home = smoke.new_home()
        path = home / ".codex/config.toml"
        path.write_text(path.read_text() + '\n[mcp_servers.chronicle]\nurl = "http://127.0.0.1:9/mcp"\n',
                        encoding="utf-8")
        files = all_bytes(home)
        result = smoke.cli("connect", "codex", "--yes", "--json", home=home)
        part = json_out(result)["parts"]["server"]
        return (result.returncode != 0 and part["status"] == "refused" and "--force" in part.get("manual", "")
                and all_bytes(home) == files), f"exit {result.returncode}, {part['status']}"

    def other_name():
        home = smoke.new_home()
        path = home / ".zcode/cli/config.json"
        data = json.loads(path.read_text())
        data["mcp"]["servers"]["memory"] = {"type": "http", "url": f"{smoke.base_url}/mcp"}
        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        made = path.read_bytes()
        plain = smoke.cli("connect", "zcode", "--yes", "--json", home=home)
        reported = "'memory'" in json_out(plain)["parts"]["server"]["detail"] and path.read_bytes() == made
        adopted = smoke.cli("connect", "zcode", "--yes", "--json", "--adopt", home=home)
        servers = json.loads(path.read_text())["mcp"]["servers"]
        renamed = adopted.returncode == 0 and "chronicle" in servers and "memory" not in servers
        smoke.cli("disconnect", "zcode", "--yes", home=home)
        restored = path.read_bytes() == made
        return (plain.returncode != 0 and reported and renamed and restored), \
            f"plain exit {plain.returncode} reported {reported}, renamed {renamed}, restored {restored}"

    smoke.check("existing-same-url", same_url)
    smoke.check("existing-other-url", other_url)
    smoke.check("adopt-other-name", other_name)


def doctor_checks(smoke: Smoke) -> None:
    home = smoke.new_home()
    smoke.cli("connect", "zcode", "--yes", home=home)

    def clean():
        result = smoke.cli("doctor", "--agents", home=home)
        return result.returncode == 0 and json_out(result)["status"] == "ok", f"exit {result.returncode}"

    def drift():
        path = home / ".zcode/AGENTS.md"
        path.write_text(path.read_text().replace("<!-- chronicle:end -->", ""), encoding="utf-8")
        result = smoke.cli("doctor", "--agents", home=home)
        issues = [issue["part"] for agent in json_out(result)["agents"] for issue in agent["issues"]]
        return result.returncode != 0 and issues == ["block"], f"exit {result.returncode}, issues {issues}"

    smoke.check("doctor-clean", clean)
    smoke.check("doctor-drift", drift)


def report_checks(smoke: Smoke) -> None:
    def json_report():
        home = smoke.new_home()
        result = smoke.cli("connect", "gemini", "--yes", "--json", home=home)
        report = json.loads(result.stdout)  # the whole output is one object
        smoke.cli("disconnect", "gemini", "--yes", home=home)
        return (result.returncode == 0 and set(report["parts"]) == {"server", "permissions", "hook", "block"}
                and all("status" in part for part in report["parts"].values())), f"exit {result.returncode}"

    def print_cursor():
        home = smoke.new_home()
        files = all_bytes(home)
        result = smoke.cli("connect", "cursor", "--print", home=home)
        return (result.returncode == 0 and f"{smoke.base_url}/mcp" in result.stdout and '"mcpServers"' in result.stdout
                and "<!-- chronicle:begin role=write" in result.stdout and all_bytes(home) == files), \
            f"exit {result.returncode}"

    def cursor_no_edit():
        home = smoke.new_home()
        files = all_bytes(home)
        result = smoke.cli("connect", "cursor", "--yes", home=home)
        return result.returncode != 0 and all_bytes(home) == files, f"exit {result.returncode}"

    smoke.check("json-report", json_report)
    smoke.check("print-cursor", print_cursor)
    smoke.check("cursor-no-edit", cursor_no_edit)


def hook(smoke: Smoke, agent: str, budget: int, session: str) -> subprocess.CompletedProcess:
    payload = json.dumps({"session_id": session, "hook_event_name": "SessionStart", "cwd": str(smoke.workspace)})
    return subprocess.run([sys.executable, "-m", "max_chronicle.brief_hook", "--manifest", str(smoke.manifest),
                           "--agent", agent, "--budget", str(budget)], input=payload, capture_output=True,
                          text=True, env=smoke.env(), timeout=COMMAND_TIMEOUT_S)


def hook_up_checks(smoke: Smoke) -> None:
    def up():
        result = hook(smoke, "claude", 6000, "smoke-up")
        return (result.returncode == 0 and result.stdout.startswith("Chronicle memory brief.")), \
            f"exit {result.returncode}: {result.stdout[:80]!r}"

    def budget():
        writer = smoke.mcp("/mcp")
        for index in range(40):
            payload_of(writer.call("record_event", {
                "text": f"Smoke decision {index}: " + "a long line of reasoning about the harbour " * 8,
                "agent": "smoke-writer", "category": "decision", "project": "smoke"}))
        result = hook(smoke, "codex", CODEX_BUDGET, "smoke-budget")
        return result.returncode == 0 and 0 < len(result.stdout) <= CODEX_BUDGET, f"{len(result.stdout)} characters"

    smoke.check("hook-up", up)
    smoke.check("hook-budget", budget)


def down_checks(smoke: Smoke) -> None:
    """With the server stopped: connect refuses, the hook reads the database."""

    def preflight_down():
        home = smoke.new_home()
        files = all_bytes(home)
        result = smoke.cli("connect", "claude", "--yes", home=home)
        return (result.returncode != 0 and all_bytes(home) == files
                and "--transport streamable-http" in result.stdout), f"exit {result.returncode}"

    def hook_down():
        result = hook(smoke, "claude", 6000, "smoke-down")
        first = result.stdout.split("\n", 1)[0]
        return (result.returncode == 0 and first.startswith("Chronicle's server is not running")
                and "Chronicle memory brief." in result.stdout), f"exit {result.returncode}: {first[:80]!r}"

    smoke.check("preflight-down", preflight_down)
    smoke.check("hook-down", hook_down)


def stub_checks(smoke: Smoke) -> None:
    def preflight_old():
        server = ThreadingHTTPServer(("127.0.0.1", 0), StubHealth)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            home = smoke.new_home()
            files = all_bytes(home)
            result = smoke.cli("connect", "claude", "--yes", "--url", f"http://127.0.0.1:{server.server_address[1]}",
                               home=home)
        finally:
            server.shutdown()
            server.server_close()
        return (result.returncode != 0 and all_bytes(home) == files and "0.32.1" in result.stdout), \
            f"exit {result.returncode}"

    smoke.check("preflight-old", preflight_old)


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end smoke test of chronicle connect.")
    parser.add_argument("--keep", action="store_true", help="Keep the temporary directory and print its path")
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="chronicle-connect-smoke-"))
    smoke = Smoke(root)
    try:
        smoke.prepare()
        smoke.start_server()
        server_checks(smoke)
        connect_checks(smoke)
        refusal_checks(smoke)
        existing_checks(smoke)
        doctor_checks(smoke)
        report_checks(smoke)
        hook_up_checks(smoke)
        smoke.stop_server()
        down_checks(smoke)
        stub_checks(smoke)
    except Exception as exc:  # noqa: BLE001 - the setup failed: say so and fail the smoke
        smoke.results.append(("setup", False, f"{type(exc).__name__}: {exc}"))
        print(f"FAIL setup: {type(exc).__name__}: {exc}", flush=True)
    finally:
        smoke.stop_server()
        if args.keep:
            print(f"kept {root}")
        else:
            shutil.rmtree(root, ignore_errors=True)
    failed = [label for label, passed, _ in smoke.results if not passed]
    if failed:
        print("SMOKE FAILED")
        return 1
    print(f"SMOKE OK ({len(smoke.results)} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
