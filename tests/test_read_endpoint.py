"""The read endpoint: /mcp/read beside /mcp in one HTTP process (stage 1 of `chronicle connect`)."""

from __future__ import annotations

import json
import sys
import threading
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.testclient import TestClient

import max_chronicle.mcp_server as mcp_server
from max_chronicle import __version__
from max_chronicle.mcp_server import (
    MCP_PATH,
    READ_MCP_PATH,
    build_http_app,
    build_server,
    http_endpoints,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import config_from_manifest, open_connection

BASE = "http://127.0.0.1:8000"
HEADERS = {"accept": "application/json, text/event-stream", "content-type": "application/json"}
LEGACY_VERSION = "2025-06-18"
WRITE_TOOLS = {"startup_bundle", "recent_events", "state_at", "query_memory", "get_memory", "record_event"}
READ_TOOLS = WRITE_TOOLS - {"record_event"}


class McpHttp:
    """A minimal MCP client over streamable HTTP: the initialize handshake, then requests."""

    def __init__(self, client: TestClient, path: str) -> None:
        self.client, self.path, self.next_id = client, path, 1
        reply, headers = self._post({"method": "initialize", "params": {
            "protocolVersion": LEGACY_VERSION, "capabilities": {},
            "clientInfo": {"name": "chronicle-test", "version": "1"}}})
        assert "result" in reply, reply
        self.headers = {**HEADERS, "mcp-session-id": headers["mcp-session-id"],
                        "mcp-protocol-version": LEGACY_VERSION}
        self.client.post(self.path, headers=self.headers,
                         json={"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _post(self, message: dict[str, Any], headers: dict[str, str] | None = None) -> tuple[dict, Any]:
        message = {"jsonrpc": "2.0", "id": self.next_id, **message}
        self.next_id += 1
        response = self.client.post(self.path, headers=headers or HEADERS, json=message)
        assert response.status_code == 200, response.text
        return _first_message(response.text), response.headers

    def request(self, method: str, params: dict[str, Any] | None = None) -> dict:
        return self._post({"method": method, "params": params or {}}, self.headers)[0]

    def tools(self) -> set[str]:
        return {tool["name"] for tool in self.request("tools/list")["result"]["tools"]}

    def call(self, name: str, arguments: dict[str, Any]) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})


def _first_message(body: str) -> dict:
    """The JSON-RPC message of a response, sent as JSON or as one server-sent event."""
    if body.lstrip().startswith("{"):
        return json.loads(body)
    for line in body.splitlines():
        if line.startswith("data:"):
            return json.loads(line[len("data:"):].strip())
    raise AssertionError(f"no message in {body[:200]!r}")


def _tool_payload(reply: dict) -> dict:
    result = reply["result"]
    assert not result.get("isError"), result
    return json.loads(result["content"][0]["text"])


def _counts(manifest_path) -> tuple[int, int]:
    config = config_from_manifest(load_manifest(manifest_path))
    with open_connection(config) as connection:
        events = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        snapshots = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    return int(events), int(snapshots)


@pytest.fixture()
def runtime(monkeypatch):
    monkeypatch.setattr(mcp_server, "_RUNTIME", {key: None for key in mcp_server._RUNTIME})
    return mcp_server._RUNTIME


@pytest.fixture()
def app_client(chronicle_sandbox, runtime):
    with TestClient(build_http_app(chronicle_sandbox.manifest_path), base_url=BASE) as client:
        yield client


def test_health_lists_both_endpoints_with_their_roles(app_client) -> None:
    health = app_client.get("/health").json()

    assert health["version"] == __version__
    assert health["profile"] == "chronicler"
    assert health["mcp_endpoints"] == [
        {"path": "/mcp", "role": "write", "profile": "chronicler"},
        {"path": "/mcp/read", "role": "read", "profile": "readonly"},
    ]


def test_the_write_endpoint_lists_six_tools_and_the_read_endpoint_five(app_client) -> None:
    assert McpHttp(app_client, MCP_PATH).tools() == WRITE_TOOLS
    assert McpHttp(app_client, READ_MCP_PATH).tools() == READ_TOOLS


def test_record_event_fails_at_the_read_endpoint(chronicle_sandbox, app_client) -> None:
    before = _counts(chronicle_sandbox.manifest_path)

    reply = McpHttp(app_client, READ_MCP_PATH).call("record_event", {"text": "Must not be stored"})

    failed = "error" in reply or reply["result"].get("isError")
    assert failed, reply
    assert _counts(chronicle_sandbox.manifest_path) == before


def test_a_capture_asked_at_the_read_endpoint_writes_nothing(chronicle_sandbox, app_client, runtime) -> None:
    before = _counts(chronicle_sandbox.manifest_path)

    payload = _tool_payload(McpHttp(app_client, READ_MCP_PATH).call(
        "startup_bundle", {"capture": True, "agent": "reader"}))

    assert payload["session_id"]
    assert _counts(chronicle_sandbox.manifest_path) == before
    assert runtime["last_write"] is None  # a reader's slot, no write recorded


def test_a_write_at_mcp_is_found_through_the_read_endpoint(app_client) -> None:
    stored = _tool_payload(McpHttp(app_client, MCP_PATH).call(
        "record_event", {"text": "Pelican harbour decision recorded through the write endpoint",
                         "agent": "writer", "category": "decision"}))

    found = _tool_payload(McpHttp(app_client, READ_MCP_PATH).call("query_memory", {"query": "pelican harbour"}))

    assert stored["chronicle_status"] == "stored"
    assert any(hit["id"] == f"event:{stored['id']}" for hit in found["results"]), found


def test_the_other_routes_come_from_the_main_server(app_client, loaded_manifest) -> None:
    from max_chronicle import service

    service.record_event(loaded_manifest, {"agent": "agent-a", "text": "A page has something to show"},
                         append_compat=False)
    assert app_client.get("/health/live").json()["status"] == "ok"
    assert app_client.get("/brief").status_code == 200
    assert app_client.get("/ui").status_code == 200
    assert app_client.get("/mcp/read/other").status_code == 404


def test_both_session_managers_run_under_one_lifespan(chronicle_sandbox, runtime) -> None:
    app = build_http_app(chronicle_sandbox.manifest_path)
    with TestClient(app, base_url=BASE) as client:
        assert McpHttp(client, MCP_PATH).tools() and McpHttp(client, READ_MCP_PATH).tools()
    # Each manager runs once; the app's lifespan entered both, so a second start fails.
    with pytest.raises(RuntimeError, match="once"):
        with TestClient(app, base_url=BASE):
            pass


def test_a_read_only_profile_serves_one_endpoint(chronicle_sandbox, runtime) -> None:
    app = build_http_app(chronicle_sandbox.manifest_path, profile="readonly")
    with TestClient(app, base_url=BASE) as client:
        health = client.get("/health").json()
        tools = McpHttp(client, MCP_PATH).tools()
        missing = client.post(READ_MCP_PATH, headers=HEADERS, json={"jsonrpc": "2.0", "id": 1, "method": "ping"})

    assert health["mcp_endpoints"] == [{"path": "/mcp", "role": "read", "profile": "readonly"}]
    assert tools == READ_TOOLS
    assert missing.status_code == 404


def test_a_server_built_alone_lists_its_own_endpoint(chronicle_sandbox, runtime) -> None:
    server = build_server(chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app(), base_url=BASE) as client:
        health = client.get("/health").json()

    assert health["mcp_endpoints"] == [{"path": "/mcp", "role": "write", "profile": "chronicler"}]


def test_http_endpoints_by_profile() -> None:
    assert [item["path"] for item in http_endpoints("chronicler")] == ["/mcp", "/mcp/read"]
    assert [item["role"] for item in http_endpoints("admin")] == ["admin", "read"]
    assert http_endpoints("readonly") == [{"path": "/mcp", "role": "read", "profile": "readonly"}]


def test_health_counts_the_named_background_threads(app_client) -> None:
    stop = threading.Event()
    thread = threading.Thread(target=stop.wait, name="chronicle-test-job", daemon=True)
    thread.start()
    try:
        threads = app_client.get("/health").json()["threads"]
    finally:
        stop.set()
        thread.join()

    assert threads.get("chronicle-test-job") == 1


def test_main_composes_the_app_and_starts_each_job_once(chronicle_sandbox, monkeypatch, runtime) -> None:
    started: list[str] = []
    served: list[Any] = []
    monkeypatch.setattr(mcp_server, "_start_notes_sync", lambda path: started.append("notes"))
    monkeypatch.setattr(mcp_server, "_start_embedder", lambda path: started.append("embedder"))
    monkeypatch.setattr(mcp_server, "_start_scheduler", lambda path: started.append("scheduler"))
    monkeypatch.setattr(mcp_server, "_install_stack_dump", lambda: None)
    monkeypatch.setattr(mcp_server, "_serve_app", lambda app, options: served.append((app, options)))
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "--transport", "streamable-http"])

    assert mcp_server._main() == 0

    assert sorted(started) == ["embedder", "notes", "scheduler"]
    (app, options), = served
    paths = [getattr(route, "path", None) for route in app.routes]
    assert paths.count(MCP_PATH) == 1 and paths.count(READ_MCP_PATH) == 1 and paths.count("/health") == 1
    assert options["port"] == 8000 and options["host"] == "127.0.0.1"


def test_stdio_and_the_read_only_http_server_are_served_as_before(chronicle_sandbox, monkeypatch, runtime) -> None:
    calls: list[str] = []

    async def served(**options) -> None:
        calls.append("http")

    monkeypatch.setattr(mcp_server, "build_server", lambda *args, **kwargs: SimpleNamespace(
        run=lambda transport: calls.append(transport), settings=SimpleNamespace(),
        run_streamable_http_async=served))
    monkeypatch.setattr(mcp_server, "_install_stack_dump", lambda: None)
    monkeypatch.setattr(mcp_server, "_serve_app", lambda app, options: calls.append("composed"))
    with open_connection(config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))):
        pass  # a read-only server needs a database to exist

    for argv in (["--transport", "stdio"], ["--profile", "readonly", "--transport", "streamable-http"]):
        monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path), *argv])
        assert mcp_server._main() == 0

    assert calls == ["stdio", "http"]
