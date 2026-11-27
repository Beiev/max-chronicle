"""`chronicle connect`, `disconnect`, `agents` and `doctor --agents` on copies of synthetic agent configs."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tomllib

import pytest

from max_chronicle import connect as connect_module
from max_chronicle.connect import (
    ADAPTERS,
    EXIT_DRIFT,
    EXIT_OK,
    EXIT_PREFLIGHT,
    EXIT_REFUSED,
    EXIT_UNSUPPORTED,
    READ_TOOLS,
    WRITE_TOOLS,
    PreflightFailed,
    agent_status,
    doctor_agents,
    find_adapter,
    preflight,
    run_connect,
    run_disconnect,
)
from max_chronicle.connect_edit import Ledger, mask_comments

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "connect" / "home"
AGENTS = ("claude", "codex", "zcode", "opencode", "gemini")
BASE = "http://127.0.0.1:4100"
SERVER = {"base_url": BASE, "version": "0.33.0", "source": "--url", "status": "ok", "notes": []}
AGENT_FILES = {
    "claude": [".claude.json", ".claude/settings.json", ".claude/CLAUDE.md"],
    "codex": [".codex/config.toml", ".codex/hooks.json", ".codex/AGENTS.md"],
    "zcode": [".zcode/cli/config.json", ".zcode/AGENTS.md"],
    "opencode": [".config/opencode/opencode.jsonc", ".config/opencode/AGENTS.md"],
    "gemini": [".gemini/settings.json", ".gemini/GEMINI.md"],
}


@pytest.fixture()
def home(tmp_path) -> Path:
    target = tmp_path / "home"
    shutil.copytree(FIXTURES, target)
    return target


@pytest.fixture()
def env(home) -> dict[str, str]:
    return {"HOME": str(home)}


def _sha(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def _shas(home: Path, agent: str) -> dict[str, str | None]:
    return {name: _sha(home / name) for name in AGENT_FILES[agent]}


def _connect(agent: str, sandbox, env, **options):
    defaults = {"role": "write", "url": None, "by": None, "yes": True, "dry_run": False, "force": False,
                "adopt": False, "server": SERVER}
    defaults.update(options)
    return run_connect(ADAPTERS[agent], manifest=sandbox.manifest_path, env=env, **defaults)


def _disconnect(agent: str, sandbox, env, **options):
    defaults = {"by": None, "yes": True, "dry_run": False}
    defaults.update(options)
    return run_disconnect(ADAPTERS[agent], manifest=sandbox.manifest_path, env=env, **defaults)


def _parses(path: Path) -> None:
    text = path.read_text()
    if path.suffix == ".toml":
        tomllib.loads(text)
    elif path.suffix in (".json", ".jsonc"):
        json.loads(mask_comments(text)[0])


def _events(sandbox) -> list[dict]:
    connection = sqlite3.connect(sandbox.chronicle_db)
    try:
        rows = connection.execute("SELECT actor, category, text, payload_json FROM events "
                                  "WHERE category = 'agent_connection' ORDER BY recorded_at_utc").fetchall()
    finally:
        connection.close()
    return [{"actor": a, "category": c, "text": t, "payload": json.loads(p)} for a, c, t, p in rows]


def test_the_roles_match_the_tools_the_server_registers(chronicle_sandbox) -> None:
    from max_chronicle.mcp_server import build_server

    def names(profile: str) -> set[str]:
        server = build_server(chronicle_sandbox.manifest_path, profile=profile)
        return {tool.name for tool in asyncio.run(server.list_tools())}

    assert set(WRITE_TOOLS) == names("chronicler")
    assert set(READ_TOOLS) == names("readonly")


@pytest.mark.parametrize("agent", AGENTS)
def test_connect_installs_every_confirmed_part_and_the_files_still_parse(agent, chronicle_sandbox, home, env) -> None:
    outcome = _connect(agent, chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_OK, outcome.payload
    statuses = {name: part["status"] for name, part in outcome.payload["parts"].items()}
    expected = {"server": "installed", "permissions": "installed", "hook": "installed", "block": "installed"}
    if agent == "opencode":
        expected["hook"] = "skipped"
    assert statuses == expected
    for name in AGENT_FILES[agent]:
        _parses(home / name)


@pytest.mark.parametrize("agent", AGENTS)
def test_connect_changes_nothing_outside_the_inserted_pieces(agent, chronicle_sandbox, home, env) -> None:
    before = {name: (home / name).read_text() for name in AGENT_FILES[agent]}
    _connect(agent, chronicle_sandbox, env)

    by_file: dict[str, list[dict]] = {}
    for change in Ledger(chronicle_sandbox.status_root / "connect-ledger.json").active(agent):
        by_file.setdefault(change["file"], []).append(change)
    for name in AGENT_FILES[agent]:
        text = (home / name).read_text()
        for change in reversed(by_file.get(str(home / name), [])):
            start = change["offset"]
            assert text[start:start + len(change["piece"])] == change["piece"]
            text = text[:start] + change["original"] + text[start + len(change["piece"]):]
        assert text == before[name], name


@pytest.mark.parametrize("agent", AGENTS)
def test_a_second_connect_changes_no_file(agent, chronicle_sandbox, home, env) -> None:
    _connect(agent, chronicle_sandbox, env)
    before = _shas(home, agent)
    ledger_before = (chronicle_sandbox.status_root / "connect-ledger.json").read_bytes()

    again = _connect(agent, chronicle_sandbox, env)

    assert again.exit_code == EXIT_OK and again.payload["status"] == "nothing_to_do"
    assert _shas(home, agent) == before
    assert (chronicle_sandbox.status_root / "connect-ledger.json").read_bytes() == ledger_before


@pytest.mark.parametrize("agent", AGENTS)
def test_disconnect_restores_every_touched_file(agent, chronicle_sandbox, home, env) -> None:
    before = _shas(home, agent)
    _connect(agent, chronicle_sandbox, env)
    assert _shas(home, agent) != before

    outcome = _disconnect(agent, chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_OK and outcome.payload["status"] == "done"
    assert _shas(home, agent) == before
    assert all(outcome.payload["restored"].values())


def test_a_role_switch_changes_the_address_the_permissions_and_the_block(chronicle_sandbox, home, env) -> None:
    _connect("claude", chronicle_sandbox, env)
    original = (FIXTURES / ".claude" / "settings.json").read_bytes()

    outcome = _connect("claude", chronicle_sandbox, env, role="read")

    assert outcome.exit_code == EXIT_OK and outcome.payload["url"] == f"{BASE}/mcp/read"
    assert json.loads((home / ".claude.json").read_text())["mcpServers"]["chronicle"]["url"] == f"{BASE}/mcp/read"
    allow = json.loads((home / ".claude" / "settings.json").read_text())["permissions"]["allow"]
    assert allow == [f"mcp__chronicle__{tool}" for tool in READ_TOOLS]
    block = (home / ".claude" / "CLAUDE.md").read_text()
    assert "role=read" in block and "role=write" not in block and "record_event`." not in block.split("read-only")[0]
    _disconnect("claude", chronicle_sandbox, env)
    assert (home / ".claude" / "settings.json").read_bytes() == original


def test_codex_read_role_approves_five_tools(chronicle_sandbox, home, env) -> None:
    _connect("codex", chronicle_sandbox, env, role="read")

    config = tomllib.loads((home / ".codex" / "config.toml").read_text())
    entry = config["mcp_servers"]["chronicle"]
    assert entry["url"] == f"{BASE}/mcp/read"
    assert {tool: table["approval_mode"] for tool, table in entry["tools"].items()} == {
        tool: "approve" for tool in READ_TOOLS}


def test_a_hand_made_entry_for_this_server_is_adopted_and_kept(chronicle_sandbox, home, env) -> None:
    path = home / ".claude.json"
    data = json.loads(path.read_text())
    data["mcpServers"]["chronicle"] = {"type": "http", "url": f"{BASE}/mcp"}
    path.write_text(json.dumps(data, indent=2) + "\n")
    made = path.read_bytes()

    outcome = _connect("claude", chronicle_sandbox, env)

    assert outcome.payload["parts"]["server"]["status"] == "present"
    assert path.read_bytes() == made
    adopted = [c for c in Ledger(chronicle_sandbox.status_root / "connect-ledger.json").changes
               if c["part"] == "server" and c["status"] == "adopted"]
    assert len(adopted) == 1 and adopted[0]["file"] == str(path)
    _disconnect("claude", chronicle_sandbox, env)
    assert path.read_bytes() == made


def test_an_entry_pointing_elsewhere_is_refused_without_force(chronicle_sandbox, home, env) -> None:
    path = home / ".codex" / "config.toml"
    path.write_text(path.read_text() + '\n[mcp_servers.chronicle]\nurl = "http://127.0.0.1:4999/mcp"\n')
    before = _shas(home, "codex")

    refused = _connect("codex", chronicle_sandbox, env)

    assert refused.exit_code == EXIT_REFUSED and refused.payload["parts"]["server"]["status"] == "refused"
    assert "--force" in refused.payload["parts"]["server"]["manual"]
    assert _shas(home, "codex") == before
    forced = _connect("codex", chronicle_sandbox, env, force=True)
    assert forced.exit_code == EXIT_OK
    assert tomllib.loads(path.read_text())["mcp_servers"]["chronicle"]["url"] == f"{BASE}/mcp"
    _disconnect("codex", chronicle_sandbox, env)
    assert _shas(home, "codex") == before


def test_an_entry_of_another_name_is_reported_and_renamed_only_with_adopt(chronicle_sandbox, home, env) -> None:
    path = home / ".zcode" / "cli" / "config.json"
    data = json.loads(path.read_text())
    data["mcp"]["servers"]["Память"] = {"type": "http", "url": f"{BASE}/mcp"}
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    made = path.read_bytes()

    refused = _connect("zcode", chronicle_sandbox, env)
    assert refused.exit_code == EXIT_REFUSED and "'Память'" in refused.payload["parts"]["server"]["detail"]
    assert path.read_bytes() == made

    adopted = _connect("zcode", chronicle_sandbox, env, adopt=True)
    servers = json.loads(path.read_text())["mcp"]["servers"]
    assert adopted.exit_code == EXIT_OK and "chronicle" in servers and "Память" not in servers
    _disconnect("zcode", chronicle_sandbox, env)
    assert path.read_bytes() == made


@pytest.mark.parametrize("damage", ["symlink", "crlf", "bom", "duplicate"])
def test_a_file_that_is_not_edited_refuses_the_agent_and_changes_nothing(damage, chronicle_sandbox, home, env) -> None:
    path = home / ".claude" / "settings.json"
    if damage == "symlink":
        real = home / "settings-real.json"
        path.rename(real)
        path.symlink_to(real)
    elif damage == "crlf":
        path.write_bytes(path.read_bytes().replace(b"\n", b"\r\n"))
    elif damage == "bom":
        path.write_bytes(b"\xef\xbb\xbf" + path.read_bytes())
    else:
        path.write_text(path.read_text().replace('"model": "example-model",', '"hooks": {},\n  "model": "x",'))
    before = {name: (home / name).read_bytes() for name in AGENT_FILES["claude"]}

    outcome = _connect("claude", chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_REFUSED
    assert {name: (home / name).read_bytes() for name in AGENT_FILES["claude"]} == before
    refused = [part for part in outcome.payload["parts"].values() if part["status"] == "refused"]
    assert refused and all(part.get("manual") for part in refused)


def test_a_file_changed_while_connect_waits_is_refused(chronicle_sandbox, home, env) -> None:
    path = home / ".claude" / "settings.json"

    def answer(prompt, lines, diff) -> bool:
        path.write_text(path.read_text().replace('"effortLevel": "high"', '"effortLevel": "low"'))
        return True

    changed = path.read_text().replace('"effortLevel": "high"', '"effortLevel": "low"')
    outcome = _connect("claude", chronicle_sandbox, env, yes=False, ask=answer)

    assert outcome.exit_code == EXIT_REFUSED and "changed on disk" in outcome.payload["error"]
    assert path.read_text() == changed
    assert (home / ".claude.json").read_bytes() == (FIXTURES / ".claude.json").read_bytes()


def test_a_dry_run_and_a_no_change_nothing(chronicle_sandbox, home, env) -> None:
    before = _shas(home, "gemini")

    dry = _connect("gemini", chronicle_sandbox, env, dry_run=True)
    declined = _connect("gemini", chronicle_sandbox, env, yes=False, ask=lambda *args: False)

    assert dry.exit_code == EXIT_OK and dry.payload["status"] == "dry_run" and "+++" in dry.diff
    assert declined.exit_code == EXIT_REFUSED and declined.payload["status"] == "declined"
    assert _shas(home, "gemini") == before
    assert not (chronicle_sandbox.status_root / "connect-ledger.json").exists() or not Ledger(
        chronicle_sandbox.status_root / "connect-ledger.json").changes


def test_connect_and_disconnect_record_events_by_the_operator(chronicle_sandbox, home, env) -> None:
    _connect("codex", chronicle_sandbox, env, by="claude", role="read")
    _disconnect("codex", chronicle_sandbox, env, by="claude")

    connected, disconnected = _events(chronicle_sandbox)
    assert connected["actor"] == "operator" and connected["category"] == "agent_connection"
    assert connected["payload"]["connection"] == {
        "action": "connect", "target": "codex", "role": "read", "url": f"{BASE}/mcp/read",
        "parts": {"server": "installed", "permissions": "installed", "hook": "installed", "block": "installed"},
        "by": "claude", "by_self_declared": True}
    assert "self-declared" in connected["text"]
    assert disconnected["payload"]["connection"]["action"] == "disconnect"


def test_codex_hooks_are_reported_as_needing_trust(chronicle_sandbox, env) -> None:
    outcome = _connect("codex", chronicle_sandbox, env)

    assert any("/hooks" in note for note in outcome.payload["parts"]["hook"].get("notes", []))


def test_the_hook_command_has_an_absolute_python_and_the_manifest(chronicle_sandbox, home, env) -> None:
    _connect("codex", chronicle_sandbox, env)

    hook = json.loads((home / ".codex" / "hooks.json").read_text())["hooks"]["SessionStart"][-1]["hooks"][0]
    assert hook["command"].startswith(sys.executable) and " -m max_chronicle.brief_hook " in hook["command"]
    assert str(chronicle_sandbox.manifest_path) in hook["command"] and "--budget 4500" in hook["command"]


def test_a_gemini_server_not_trusted_on_purpose_is_left_so(chronicle_sandbox, home, env) -> None:
    path = home / ".gemini" / "settings.json"
    data = json.loads(path.read_text())
    data["mcpServers"] = {"chronicle": {"url": f"{BASE}/mcp", "type": "http", "trust": False}}
    path.write_text(json.dumps(data, indent=2))

    outcome = _connect("gemini", chronicle_sandbox, env)

    assert outcome.payload["parts"]["permissions"]["status"] == "skipped"
    assert json.loads(path.read_text())["mcpServers"]["chronicle"]["trust"] is False


def test_zcode_hooks_switched_off_leave_the_hook_out_and_the_block_says_start_with_startup_bundle(
        chronicle_sandbox, home, env) -> None:
    path = home / ".zcode" / "cli" / "config.json"
    data = json.loads(path.read_text())
    data["hooks"] = {"enabled": False}
    path.write_text(json.dumps(data, indent=2) + "\n")

    outcome = _connect("zcode", chronicle_sandbox, env)

    assert outcome.payload["parts"]["hook"]["status"] == "skipped"
    assert "Start each session with `startup_bundle" in (home / ".zcode" / "AGENTS.md").read_text()


def test_opencode_gets_no_global_instruction_file_created(chronicle_sandbox, home, env) -> None:
    (home / ".config" / "opencode" / "AGENTS.md").unlink()

    outcome = _connect("opencode", chronicle_sandbox, env)

    assert outcome.payload["parts"]["block"]["status"] == "skipped"
    assert "CLAUDE.md" in outcome.payload["parts"]["block"]["manual"]
    assert not (home / ".config" / "opencode" / "AGENTS.md").exists()


def test_codex_override_instructions_get_the_block(chronicle_sandbox, home, env) -> None:
    (home / ".codex" / "AGENTS.override.md").write_text("# Override\n")

    _connect("codex", chronicle_sandbox, env)

    assert "chronicle:begin" in (home / ".codex" / "AGENTS.override.md").read_text()
    assert "chronicle:begin" not in (home / ".codex" / "AGENTS.md").read_text()


def test_relocated_homes_are_followed(chronicle_sandbox, tmp_path, env) -> None:
    moved = tmp_path / "moved"
    shutil.copytree(FIXTURES / ".claude", moved / "claude")
    shutil.copy(FIXTURES / ".claude.json", moved / "claude" / ".claude.json")
    shutil.copytree(FIXTURES / ".codex", moved / "codex")
    shutil.copytree(FIXTURES / ".config", moved / "xdg")
    shutil.copytree(FIXTURES / ".gemini", moved / "gemini" / ".gemini")
    relocated = {**env, "CLAUDE_CONFIG_DIR": str(moved / "claude"), "CODEX_HOME": str(moved / "codex"),
                 "XDG_CONFIG_HOME": str(moved / "xdg"), "GEMINI_CLI_HOME": str(moved / "gemini")}

    for agent in ("claude", "codex", "opencode", "gemini"):
        assert _connect(agent, chronicle_sandbox, relocated).exit_code == EXIT_OK

    assert "chronicle" in json.loads((moved / "claude" / ".claude.json").read_text())["mcpServers"]
    assert "chronicle" in tomllib.loads((moved / "codex" / "config.toml").read_text())["mcp_servers"]
    assert "chronicle" in (moved / "xdg" / "opencode" / "opencode.jsonc").read_text()
    assert "chronicle" in json.loads((moved / "gemini" / ".gemini" / "settings.json").read_text())["mcpServers"]


def test_disconnect_finds_entries_the_owner_moved(chronicle_sandbox, home, env) -> None:
    _connect("claude", chronicle_sandbox, env)
    path = home / ".claude" / "settings.json"
    data = json.loads(path.read_text())
    data["permissions"] = {"deny": data["permissions"]["deny"], "allow": data["permissions"]["allow"]}
    data["env"] = {"EXAMPLE": "1"}
    path.write_text(json.dumps(data, indent=2) + "\n")  # the owner saves its settings in its own order

    outcome = _disconnect("claude", chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_OK and not outcome.payload["left"]
    after = json.loads(path.read_text())
    assert "allow" not in after["permissions"] and after["env"] == {"EXAMPLE": "1"}
    assert "UserPromptSubmit" not in after["hooks"]


def test_doctor_is_clean_after_connect_and_names_a_part_edited_by_hand(chronicle_sandbox, home, env) -> None:
    _connect("zcode", chronicle_sandbox, env)
    assert doctor_agents(chronicle_sandbox.manifest_path, env, BASE)["agents"][0]["issues"] == []

    path = home / ".zcode" / "cli" / "config.json"
    path.write_text(path.read_text().replace(f'"{BASE}/mcp"', '"http://127.0.0.1:4999/mcp"'))
    report = doctor_agents(chronicle_sandbox.manifest_path, env, BASE)

    issues = report["agents"][0]["issues"]
    assert report["status"] == "drift" and [issue["part"] for issue in issues] == ["server"]
    assert report["agents"][0]["fix"].endswith("connect zcode --role write --yes")


def test_the_agents_table(chronicle_sandbox, env) -> None:
    _connect("gemini", chronicle_sandbox, env, role="read")

    rows = {row["agent"]: row for row in agent_status(chronicle_sandbox.manifest_path, env)}

    assert rows["gemini"]["connected"] and rows["gemini"]["role"] == "read" and rows["gemini"]["hook"] == "installed"
    assert rows["claude"]["installed"] and not rows["claude"]["connected"]
    assert not rows["cursor"]["installed"] and rows["cursor"]["hook"] == "print only"


def test_an_agent_that_is_not_installed_is_reported(chronicle_sandbox, home, env) -> None:
    shutil.rmtree(home / ".gemini")

    outcome = _connect("gemini", chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_UNSUPPORTED and outcome.payload["status"] == "not_installed"


def test_aliases_find_their_adapter() -> None:
    assert find_adapter("Claude-Code").id == "claude"
    assert find_adapter("gemini-cli").id == "gemini"
    assert find_adapter("nothing") is None


# ---------------------------------------------------------------------------
# Preflight against a stand-in server
# ---------------------------------------------------------------------------


class _Stub(BaseHTTPRequestHandler):
    version = "0.33.0"
    endpoints = [{"path": "/mcp", "role": "write"}, {"path": "/mcp/read", "role": "read"}]
    read_answers = True

    def log_message(self, *args) -> None:
        pass

    def _send(self, status: int, body: dict, content_type: str = "application/json") -> None:
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"status": "ok", "version": self.version, "mcp_endpoints": self.endpoints})
        else:
            self._send(404, {})

    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("content-length") or 0))
        if self.path == "/mcp/read" and self.read_answers:
            self._send(200, {"jsonrpc": "2.0", "id": 1, "result": {"serverInfo": {"name": "stub"}}})
        else:
            self._send(404, {})


@pytest.fixture()
def stub():
    servers = []

    def start(**attributes) -> str:
        handler = type("Handler", (_Stub,), attributes)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_address[1]}"

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


def test_preflight_passes_a_current_server(stub, chronicle_sandbox) -> None:
    url = stub()

    found = preflight(url, chronicle_sandbox.status_root, chronicle_sandbox.manifest_path)

    assert found["base_url"] == url and found["version"] == "0.33.0" and found["source"] == "--url"


@pytest.mark.parametrize("attributes, reason", [
    ({"version": "0.32.1"}, "needs 0.33.0 or newer"),
    ({"endpoints": [{"path": "/mcp", "role": "write"}]}, "/mcp/read"),
    ({"read_answers": False}, "does not answer"),
])
def test_preflight_refuses_an_old_or_incomplete_server(stub, chronicle_sandbox, attributes, reason) -> None:
    url = stub(**attributes)

    with pytest.raises(PreflightFailed, match=reason) as failed:
        preflight(url, chronicle_sandbox.status_root, chronicle_sandbox.manifest_path)
    assert "--transport streamable-http" in failed.value.hint


def test_preflight_without_an_address_prints_the_start_command(chronicle_sandbox, monkeypatch) -> None:
    monkeypatch.delenv("CHRONICLE_SERVER_URL", raising=False)

    with pytest.raises(PreflightFailed, match="no server address") as failed:
        preflight(None, chronicle_sandbox.status_root, chronicle_sandbox.manifest_path)
    assert "chronicle-mcp" in failed.value.hint or "max_chronicle.mcp_server" in failed.value.hint


def test_a_failed_preflight_changes_no_file(stub, chronicle_sandbox, home, env) -> None:
    url = stub(version="0.32.1")
    before = _shas(home, "claude")

    outcome = _connect("claude", chronicle_sandbox, env, url=url, server=None)

    assert outcome.exit_code == EXIT_PREFLIGHT and _shas(home, "claude") == before


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def _cli(monkeypatch, capsys, sandbox, home, *argv: str) -> tuple[int, str]:
    from max_chronicle import cli

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(sandbox.manifest_path), *argv])
    code = cli.main()
    return code, capsys.readouterr().out


def test_yes_json_prints_one_object_with_every_part(stub, monkeypatch, capsys, chronicle_sandbox, home) -> None:
    url = stub()

    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", "claude", "--url", url, "--yes",
                     "--json", "--by", "codex")

    report = json.loads(out)
    assert code == 0 and report["status"] == "done" and report["exit_code"] == 0
    assert set(report["parts"]) == {"server", "permissions", "hook", "block"}


@pytest.mark.parametrize("agent", [*AGENTS, "cursor", "some-new-agent"])
def test_print_gives_snippets_and_the_block_and_changes_nothing(agent, stub, monkeypatch, capsys, chronicle_sandbox,
                                                                home) -> None:
    url = stub()
    before = {path: path.read_bytes() for path in home.rglob("*") if path.is_file()}

    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", agent, "--print", "--url", url)

    assert code == 0 and f"{url}/mcp" in out and "<!-- chronicle:begin role=write" in out
    assert {path: path.read_bytes() for path in home.rglob("*") if path.is_file()} == before


def test_cursor_is_only_printed(monkeypatch, capsys, chronicle_sandbox, home) -> None:
    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", "cursor", "--yes", "--url", BASE)

    assert code == EXIT_UNSUPPORTED and "--print" in out


def test_windows_is_refused(monkeypatch, capsys, chronicle_sandbox, home) -> None:
    monkeypatch.setattr(connect_module, "_windows", lambda: True)

    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", "claude", "--yes", "--url", BASE)

    assert code == EXIT_UNSUPPORTED and "Windows" in out


def test_json_without_yes_is_refused(monkeypatch, capsys, chronicle_sandbox, home) -> None:
    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", "claude", "--json", "--url", BASE)

    assert code == EXIT_REFUSED and json.loads(out)["error"] == "--json needs --yes or --dry-run"


def test_doctor_agents_exits_on_drift(monkeypatch, capsys, chronicle_sandbox, home, env, stub) -> None:
    url = stub()
    _connect("gemini", chronicle_sandbox, env, server={**SERVER, "base_url": url})

    clean, _ = _cli(monkeypatch, capsys, chronicle_sandbox, home, "doctor", "--agents", "--url", url)
    (home / ".gemini" / "GEMINI.md").write_text("# Gemini CLI context\n")
    drift, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "doctor", "--agents", "--url", url)

    assert clean == EXIT_OK and drift == EXIT_DRIFT
    assert [issue["part"] for issue in json.loads(out)["agents"][0]["issues"]] == ["block"]


def test_the_interactive_question_shows_the_diff(monkeypatch, capsys, chronicle_sandbox, home, stub) -> None:
    url = stub()
    monkeypatch.setattr(sys, "stdin", io.StringIO("y\n"))

    code, out = _cli(monkeypatch, capsys, chronicle_sandbox, home, "connect", "gemini", "--url", url)

    assert code == 0 and "Apply these changes? [y/N]" in out and "+++ " in out and "Connected." in out


def test_a_pending_change_from_an_interrupted_run_is_settled(chronicle_sandbox, home, env) -> None:
    _connect("gemini", chronicle_sandbox, env)
    ledger = Ledger(chronicle_sandbox.status_root / "connect-ledger.json")
    ledger.changes[-1]["status"] = "pending"
    ledger.save()

    _connect("gemini", chronicle_sandbox, env)

    assert not Ledger(chronicle_sandbox.status_root / "connect-ledger.json").pending()


def test_disconnect_of_an_agent_never_connected_does_nothing(chronicle_sandbox, home, env) -> None:
    before = _shas(home, "opencode")

    outcome = _disconnect("opencode", chronicle_sandbox, env)

    assert outcome.exit_code == EXIT_OK and outcome.payload["status"] == "nothing_to_do"
    assert _shas(home, "opencode") == before
