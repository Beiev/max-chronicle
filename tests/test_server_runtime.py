"""The long-running server: writes without embedding in the slot, and health that shows the process."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import pytest

from max_chronicle import mcp_server
from max_chronicle.mcp_server import build_server
from max_chronicle.store import config_from_manifest, open_connection


def _decode(result: Any) -> Any:
    result = getattr(result, "content", result)  # SDK 2: a CallToolResult
    if isinstance(result, tuple):
        return _decode(result[0])
    if isinstance(result, list) and result and hasattr(result[0], "text"):
        return json.loads(result[0].text)
    return result


class _FakeEmbedder:
    def __init__(self) -> None:
        self.wakes = 0

    def wake(self) -> None:
        self.wakes += 1

    def snapshot(self) -> dict:
        return {"runs": 0, "last_run_at": None, "last_report": None}


@pytest.fixture()
def runtime(monkeypatch):
    """A clean _RUNTIME per test; the server's real one is process-wide."""
    state = {"loop": None, "embedder": None, "dump_signal": None, "last_write": None, "scheduler": None}
    monkeypatch.setattr(mcp_server, "_RUNTIME", state)
    return state


def _record(server, text: str, **extra) -> dict:
    async def run() -> dict:
        await server.call_tool("startup_bundle", {"domain": "global", "agent": "pytest", "mode": "brief"})
        return _decode(await server.call_tool("record_event", {"text": text, "agent": "pytest", **extra}))

    return asyncio.run(run())


def test_with_a_background_embedder_the_write_skips_the_embedding_and_wakes_it(
    chronicle_sandbox, runtime, monkeypatch
) -> None:
    calls = []
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda *a, **k: calls.append(a) or [1.0, 0.0])
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "1")
    embedder = _FakeEmbedder()
    runtime["embedder"] = embedder
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    stored = _record(server, "Decided to move the gateway to port 9000", request_id="bg-1")

    assert stored["chronicle_status"] == "stored"
    assert calls == [] and embedder.wakes == 1
    with open_connection(config_from_manifest(_manifest(chronicle_sandbox))) as connection:
        assert connection.execute("SELECT COUNT(*) FROM event_vectors WHERE event_id = ?",
                                  (stored["id"],)).fetchone()[0] == 0
    assert runtime["last_write"]["tool"] == "record_event"


def test_without_one_the_write_embeds_inline_as_before(chronicle_sandbox, runtime, monkeypatch) -> None:
    calls = []
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda *a, **k: calls.append(a) or [1.0, 0.0])
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "1")
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    _record(server, "Decided to keep the gateway on port 9000")

    assert len(calls) == 1


def test_a_retried_request_answers_from_its_receipt_without_new_work(
    chronicle_sandbox, runtime, monkeypatch, tmp_path
) -> None:
    """No embedding and no attachment copy for a request that is already committed."""
    calls = []
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda *a, **k: calls.append(a) or [1.0, 0.0])
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "1")
    attachment = tmp_path / "evidence.txt"
    attachment.write_text("load test numbers", encoding="utf-8")
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    first = _record(server, "Load test passed at 9000", request_id="retry-1", source_files=[str(attachment)])
    staged = []
    monkeypatch.setattr("max_chronicle.service.stage_artifact_from_path", lambda *a, **k: staged.append(a))

    again = _record(server, "Load test passed at 9000", request_id="retry-1", source_files=[str(attachment)])

    assert again["chronicle_status"] == "existing" and again["id"] == first["id"]
    assert again["dedupe_status"] == "request_id_match"
    assert len(calls) == 1 and staged == []


def _manifest(sandbox) -> dict:
    from max_chronicle.runtime_context import load_manifest

    return load_manifest(sandbox.manifest_path)


def test_health_shows_the_process_and_live_answers_without_the_database(chronicle_sandbox, runtime) -> None:
    from starlette.testclient import TestClient

    from max_chronicle.liveness import LoopMonitor

    runtime["loop"] = LoopMonitor()
    runtime["embedder"] = _FakeEmbedder()
    runtime["dump_signal"] = "SIGUSR1"
    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")

    with TestClient(server.streamable_http_app()) as client:
        health = client.get("/health").json()
        live = client.get("/health/live")

    assert health["stack_dump_signal"] == "SIGUSR1"
    assert set(health["loop"]) == {"lag_ms", "max_lag_ms_5m", "last_tick_age_s", "stalls"}
    assert health["writes"] == {"in_progress": 0, "waiting": 0}
    assert health["embedder"]["runs"] == 0
    assert live.status_code == 200 and live.json()["status"] == "ok"


def test_health_advertises_no_dump_signal_it_cannot_honour(chronicle_sandbox, runtime) -> None:
    """The watchdog sends the signal only when told; an unhandled SIGUSR1 kills the process."""
    from starlette.testclient import TestClient

    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app()) as client:
        assert "stack_dump_signal" not in client.get("/health").json()


def test_logs_are_utc_with_a_z(monkeypatch, capsys) -> None:
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(logging.Formatter, "converter", logging.Formatter.converter)

    mcp_server._configure_logging()
    logging.getLogger("max_chronicle.test").warning("hello")

    line = capsys.readouterr().err.strip().splitlines()[-1]
    stamp = line.split(" ", 1)[0]
    assert stamp.endswith("Z") and "T" in stamp and line.endswith("hello")
