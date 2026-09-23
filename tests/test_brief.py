"""The startup brief (FR-12): compact, read-only, framed as data, and closed to browser pages."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import sqlite3

import pytest

from max_chronicle import service
from max_chronicle.brief import FRAMING, _read_only, build_brief, resolve_project
from max_chronicle.mcp_server import build_server
from max_chronicle.store import config_from_manifest, open_connection

NOW = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _record(manifest: dict, text: str, *, days_ago: float = 1, **fields) -> dict:
    at = (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return service.record_event(
        manifest, {"agent": "agent-a", "domain": "global", "text": text, "recorded_at": at, **fields}
    )


def _checkpoint(manifest: dict, project: str, task_id: str, goal: str, **fields) -> dict:
    return _record(manifest, f"Checkpoint: {goal}", project=project, task_id=task_id,
                   checkpoint={"goal": goal, "next_steps": ["verify the result."]}, **fields)


def _brief(manifest: dict, **options) -> dict:
    return build_brief(manifest, now=NOW, **options)


# ---------------------------------------------------------------------------
# Content
# ---------------------------------------------------------------------------


def test_the_brief_opens_as_data_and_lists_tasks_facts_and_decisions(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    fact = _record(loaded_manifest, "Deploy target chosen", project="alpha",
                   fact={"slot": "deploy.target", "value": "staging cluster", "kind": "decision"}, category="decision")
    decision = _record(loaded_manifest, "Chose weekly releases", project="alpha", category="decision")

    brief = _brief(loaded_manifest, project="alpha")
    text = brief["text"]

    assert text.startswith(FRAMING)
    assert "alpha/migrate (agent-a, " in text and "Move storage to SQLite" in text and "next: verify the result\n" in text
    assert f"deploy.target = staging cluster (fact {fact['fact_id']})" in text
    assert f"Chose weekly releases (event {decision['id']})" in text
    assert fact["id"] not in text  # the decision that set a fact is shown as that fact
    assert brief["counts"] == {"open_tasks": 1, "facts": 1, "decisions": 1, "warnings": 1}  # no snapshot yet


def test_the_brief_leaves_out_closed_tasks_old_decisions_and_quarantine(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    _record(loaded_manifest, "Migration done", project="alpha", task_id="migrate",
            fact={"slot": "task.status", "value": "completed", "kind": "observed"})
    _record(loaded_manifest, "Chose monthly releases", days_ago=20, category="decision")
    _record(loaded_manifest, "Hidden decision", category="decision", memory_guard={"visibility": "raw_only"})

    text = _brief(loaded_manifest)["text"]

    assert "Move storage to SQLite" not in text
    assert "task.status" not in text  # the open/closed state is what the task list shows
    assert "monthly" not in text and "Hidden" not in text


def test_a_project_brief_keeps_global_items_and_drops_other_projects(loaded_manifest) -> None:
    _record(loaded_manifest, "Global rule", category="decision")
    _record(loaded_manifest, "Alpha rule", project="alpha", category="decision")
    _record(loaded_manifest, "Beta rule", project="beta", category="decision")

    text = _brief(loaded_manifest, project="alpha")["text"]

    assert "Global rule" in text and "Alpha rule" in text and "Beta rule" not in text


def test_warnings_cover_a_stale_capture_and_recent_job_failures_only(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection, connection:
        connection.execute(
            "INSERT INTO snapshots(id, captured_at_utc, captured_at_local, timezone, payload_json) VALUES ('s1', ?, ?, 'UTC', '{}')",
            ("2026-09-21T10:00:00Z", "2026-09-21T10:00:00Z"),
        )
        for job, status, started in (("mem0-dump", "failed", "2026-09-24T05:00:00Z"),
                                     ("retired-job", "failed", "2026-06-04T05:00:00Z"),
                                     ("daybook", "ok", "2026-09-23T21:00:00Z")):
            connection.execute(
                "INSERT INTO automation_runs(id, job_name, started_at_utc, status) VALUES (?, ?, ?, ?)",
                (f"run-{job}", job, started, status),
            )

    warnings = [line for line in _brief(loaded_manifest)["text"].splitlines() if line.startswith("- ") and "job" in line or "capture" in line]

    assert any("The last daily capture is 3d 2h old" in line for line in warnings)
    assert any("mem0-dump: failed" in line for line in warnings)
    assert not any("retired-job" in line or "daybook" in line for line in warnings)


def test_a_long_line_keeps_its_id(loaded_manifest) -> None:
    decision = _record(loaded_manifest, "A very long decision " * 40, category="decision")

    line = next(line for line in _brief(loaded_manifest)["text"].splitlines() if decision["id"] in line)

    assert line.endswith(f"… (event {decision['id']})") and len(line) <= 2 + 220


def test_a_secret_stored_before_the_filter_does_not_reach_the_brief(loaded_manifest) -> None:
    key = "sk-" + "proj-" + "".join(["aB3dE", "fG7hJ", "kL9mN", "pQ2rS", "tU4vW", "xY6zA"]) * 2  # synthetic
    decision = _record(loaded_manifest, "Rotated the API key", category="decision")
    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        connection.execute("UPDATE events SET text = ? WHERE id = ?", (f"Rotated the API key {key}", decision["id"]))

    brief = _brief(loaded_manifest)

    assert key not in brief["text"] and "[REDACTED:" in brief["text"] and brief["redactions"] == 1


def test_a_fact_shows_its_task_and_an_assumption_says_so(loaded_manifest) -> None:
    _record(loaded_manifest, "Live target", project="alpha", task_id="live",
            fact={"slot": "deploy.target", "value": "production", "kind": "observed"})
    _record(loaded_manifest, "Preview target", project="alpha", task_id="preview",
            fact={"slot": "deploy.target", "value": "staging", "kind": "assumption"})

    lines = [line for line in _brief(loaded_manifest, project="alpha")["text"].splitlines() if "deploy.target" in line]

    assert any(line.startswith("- deploy.target = production [task live] (fact ") for line in lines)
    assert any(line.startswith("- deploy.target = staging [task preview, assumption] (fact ") for line in lines)


def test_a_replaced_decision_does_not_come_back(loaded_manifest) -> None:
    first = _record(loaded_manifest, "Deploy to production", project="alpha", category="decision",
                    fact={"slot": "deploy.target", "value": "production", "kind": "decision"})
    _record(loaded_manifest, "Deploy to staging instead", project="alpha", category="decision",
            fact={"slot": "deploy.target", "value": "staging", "kind": "decision", "supersedes": first["fact_id"]})

    text = _brief(loaded_manifest, project="alpha")["text"]

    assert "deploy.target = staging" in text
    assert "production" not in text


def test_a_long_task_id_stays_whole(loaded_manifest) -> None:
    project, task = "p" * 120, "t" * 120
    _checkpoint(loaded_manifest, project, task, "A goal " * 40)

    line = next(line for line in _brief(loaded_manifest)["text"].splitlines() if line.startswith(f"- {project}/{task} "))

    assert line.endswith("…")


# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------


def test_over_budget_the_oldest_decisions_go_first(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    for n in range(8):
        _record(loaded_manifest, f"Decision number {n} " + "with a long explanation " * 6, days_ago=8 - n, category="decision")

    brief = _brief(loaded_manifest, max_chars=1500)

    assert len(brief["text"]) <= 1500 and brief["omitted"] > 0
    assert "Move storage to SQLite" in brief["text"]  # tasks outrank decisions
    assert "Decision number 7" in brief["text"] and "Decision number 0" not in brief["text"]


def test_many_warnings_never_cut_the_protocol(loaded_manifest) -> None:
    from max_chronicle.brief import PROTOCOL

    with open_connection(config_from_manifest(loaded_manifest)) as connection, connection:
        for n in range(10):
            connection.execute(
                "INSERT INTO automation_runs(id, job_name, started_at_utc, status) VALUES (?, ?, ?, 'failed')",
                (f"run-{n}", f"job-{n}-" + "x" * 80, "2026-09-24T05:00:00Z"),
            )

    brief = _brief(loaded_manifest, max_chars=1000)

    assert all(line in brief["text"] for line in PROTOCOL)
    assert brief["omitted"] > 0 and brief["cut"] is False and len(brief["text"]) <= 1000


@pytest.mark.parametrize("max_chars", [999, 8001])
def test_the_budget_stays_within_the_spec(loaded_manifest, max_chars) -> None:
    with pytest.raises(ValueError, match="max_chars"):
        _brief(loaded_manifest, max_chars=max_chars)


# ---------------------------------------------------------------------------
# Project resolution
# ---------------------------------------------------------------------------


def test_the_project_comes_from_a_given_slug_a_root_or_the_directory_name(loaded_manifest, tmp_path) -> None:
    _record(loaded_manifest, "Alpha work", project="alpha")
    _record(loaded_manifest, "Work in the projects folder", project="Projects")
    manifest = {**loaded_manifest, "projects": [
        {"id": "wide", "roots": [str(tmp_path / "work")]},
        {"id": "narrow", "roots": [str(tmp_path / "work" / "repo")]},
    ]}
    with open_connection(config_from_manifest(manifest)) as connection:
        def resolve(cwd=None, project=None):
            return resolve_project(connection, manifest, cwd=cwd, project=project)

        assert resolve(cwd=str(tmp_path / "Projects" / "alpha"), project="given") == ("given", "given")
        assert resolve(cwd=str(tmp_path / "work" / "repo" / "src")) == ("narrow", "root")
        assert resolve(cwd=str(tmp_path / "work" / "other")) == ("wide", "root")
        assert resolve(cwd=str(tmp_path / "Projects" / "ALPHA")) == ("alpha", "directory")
        # A parent's generic name does not claim the directories below it.
        assert resolve(cwd=str(tmp_path / "Projects" / "unknown-repo")) == (None, "none")
        # `..` is resolved before any comparison: this is `other`, not `repo`.
        assert resolve(cwd=str(tmp_path / "work" / "repo" / ".." / "other")) == ("wide", "root")


# ---------------------------------------------------------------------------
# Read-only, HTTP guard, MCP
# ---------------------------------------------------------------------------


def test_the_brief_connection_cannot_write(loaded_manifest) -> None:
    _record(loaded_manifest, "Some decision", category="decision")
    with _read_only(config_from_manifest(loaded_manifest)) as connection:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("DELETE FROM events")


def test_the_brief_never_creates_or_converts_the_database(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    assert not config.db_path.exists()

    fresh = _brief(loaded_manifest)

    assert not config.db_path.exists()
    assert fresh["text"].startswith(FRAMING) and "Nothing has been recorded" in fresh["text"]
    _record(loaded_manifest, "Some decision", category="decision")
    with sqlite3.connect(config.db_path) as connection:
        connection.execute("PRAGMA journal_mode=DELETE")
    _brief(loaded_manifest)
    with sqlite3.connect(config.db_path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({}, 200),
        ({"host": "localhost:8093"}, 200),
        ({"host": "[::1]:8093"}, 200),
        ({"origin": "http://127.0.0.1:8093"}, 403),  # any page, even a local one
        ({"host": "evil.example"}, 403),  # DNS rebinding keeps the attacker's host name
        ({"host": "127.0.0.1.evil.example"}, 403),
    ],
    ids=["loopback", "localhost", "ipv6", "origin", "foreign-host", "suffix-host"],
)
def test_the_brief_route_answers_only_local_non_browser_callers(chronicle_sandbox, headers, status) -> None:
    from starlette.testclient import TestClient

    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app(), base_url="http://127.0.0.1:8093") as client:
        response = client.get("/brief", headers=headers)

    assert response.status_code == status
    if status == 200:
        assert response.text.startswith(FRAMING)


def test_the_brief_route_validates_the_budget_and_serves_json(chronicle_sandbox) -> None:
    from starlette.testclient import TestClient

    server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
    with TestClient(server.streamable_http_app(), base_url="http://127.0.0.1:8093") as client:
        too_small = client.get("/brief", params={"budget": "10"})
        as_json = client.get("/brief", params={"format": "json", "project": "alpha"})

    assert too_small.status_code == 400
    assert as_json.json()["project"] == "alpha" and as_json.json()["text"].startswith(FRAMING)


def test_startup_in_brief_mode_returns_the_brief_and_unlocks_writes(chronicle_sandbox) -> None:
    async def exercise() -> tuple[dict, dict]:
        server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
        started = await server.call_tool("startup_bundle", {"agent": "agent-a", "mode": "brief", "project": "alpha"})
        written = await server.call_tool("record_event", {"agent": "agent-a", "text": "Recorded after a brief start"})
        decode = lambda result: json.loads((result[0] if isinstance(result, tuple) else result)[0].text)
        return decode(started), decode(written)

    started, written = asyncio.run(exercise())

    assert started["brief"].startswith(FRAMING) and started["project"] == "alpha"
    assert "task_context" not in started
    assert written["chronicle_status"] == "stored"


def test_a_brief_start_binds_the_agent_like_a_full_start(chronicle_sandbox) -> None:
    async def exercise() -> tuple[dict, dict]:
        server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
        started = await server.call_tool("startup_bundle", {"agent": "agent-a", "mode": "brief"})
        written = await server.call_tool("record_event", {"text": "Recorded without naming the agent"})
        decode = lambda result: json.loads((result[0] if isinstance(result, tuple) else result)[0].text)
        return decode(started), decode(written)

    started, written = asyncio.run(exercise())

    assert started["agent"] == "agent-a" and started["session_id"]
    assert written["agent"] == "agent-a"
