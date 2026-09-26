"""state_at as of a moment: only what Chronicle knew then, and the facts current then."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import sys

import pytest

from max_chronicle import cli, service
from max_chronicle.mcp_server import build_server
from max_chronicle.store import config_from_manifest, open_connection

T = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _record(manifest: dict, text: str, occurred: str, received: str, **fields) -> dict:
    """An event that happened at *occurred* and reached the server at *received*."""
    stored = service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text,
                                             "recorded_at": occurred, **fields})
    with open_connection(config_from_manifest(manifest)) as connection, connection:
        connection.execute("UPDATE event_observations SET recorded_at_utc=? WHERE event_id=?", (received, stored["id"]))
        if stored.get("fact_id"):
            connection.execute("UPDATE facts SET recorded_at_utc=? WHERE id=?", (received, stored["fact_id"]))
            connection.execute("UPDATE facts SET expired_at_utc=? WHERE expired_at_utc IS NOT NULL AND id != ?",
                               (received, stored["fact_id"]))
    return stored


def _texts(payload: dict) -> list[str]:
    return [event["text"] for event in payload["events"]]


def test_as_of_leaves_out_what_happened_or_arrived_later(loaded_manifest) -> None:
    _record(loaded_manifest, "Before, known before", "2026-09-10T10:00:00Z", "2026-09-10T10:00:05Z")
    _record(loaded_manifest, "Before, written up later", "2026-09-10T11:00:00Z", "2026-09-10T15:00:00Z")
    _record(loaded_manifest, "After", "2026-09-10T13:00:00Z", "2026-09-10T13:00:05Z")

    around = service.reconstruct_timeline(loaded_manifest, timestamp=T, window_hours=6)
    as_of = service.reconstruct_timeline(loaded_manifest, timestamp=T, window_hours=6, as_of=True)

    assert _texts(around) == ["Before, known before", "Before, written up later", "After"]
    assert (as_of["mode"], _texts(as_of)) == ("as_of", ["Before, known before"])
    assert "facts" not in around


def test_as_of_gives_the_facts_current_then(loaded_manifest) -> None:
    first = _record(loaded_manifest, "Deploys go to staging", "2026-09-09T09:00:00Z", "2026-09-09T09:00:00Z",
                    fact={"slot": "deploy.target", "value": "staging", "kind": "decision"})
    _record(loaded_manifest, "Deploys go to production", "2026-09-11T09:00:00Z", "2026-09-11T09:00:00Z",
            fact={"slot": "deploy.target", "value": "production", "kind": "decision", "supersedes": first["fact_id"]})

    then = service.reconstruct_timeline(loaded_manifest, timestamp=T, as_of=True)["facts"]
    now = service.reconstruct_timeline(loaded_manifest, timestamp=datetime(2026, 9, 12, tzinfo=timezone.utc),
                                       as_of=True)["facts"]

    assert [(fact["value"], fact["retired_at_utc"]) for fact in then] == [("staging", "2026-09-11T09:00:00Z")]
    assert [(fact["value"], fact["retired_at_utc"]) for fact in now] == [("production", None)]


@pytest.mark.parametrize("mode", ["around", "as_of"])
def test_agents_do_not_see_a_quarantined_event_in_the_timeline(chronicle_sandbox, loaded_manifest, mode) -> None:
    _record(loaded_manifest, "Quarantined import", "2026-09-10T11:00:00Z", "2026-09-10T11:00:00Z",
            memory_guard={"visibility": "raw_only"})
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)

    async def state_at() -> dict:
        result = await server.call_tool("state_at", {"timestamp": T.isoformat(), "mode": mode})
        content = result[0] if isinstance(result, tuple) else result
        return json.loads(content[0].text)

    assert _texts(asyncio.run(state_at())) == []
    # The operator's own view keeps it.
    assert _texts(service.reconstruct_timeline(loaded_manifest, timestamp=T, as_of=mode == "as_of")) == [
        "Quarantined import"]


def test_the_cli_prints_the_facts_current_then(chronicle_sandbox, loaded_manifest, monkeypatch, capsys) -> None:
    _record(loaded_manifest, "Deploys go to staging", "2026-09-09T09:00:00Z", "2026-09-09T09:00:00Z",
            fact={"slot": "deploy.target", "value": "staging", "kind": "decision"})
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "timeline", "--at", T.isoformat(), "--as-of"])

    assert cli.main() == 0
    assert "- deploy.target = staging [decision]" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "timeline", "--at", T.isoformat(), "--as-of", "--format", "json"])
    assert cli.main() == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "as_of"
