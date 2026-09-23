"""Handoff context: only a named task is resumed, history pages both ways, and the bundle stays lean."""

from __future__ import annotations

import asyncio
import json

import pytest

from max_chronicle import service
from max_chronicle.mcp_server import build_server
from max_chronicle.memory import task_context


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _record(manifest: dict, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields})


def _checkpoint(manifest: dict, project: str, task_id: str, goal: str, **fields) -> dict:
    return _record(
        manifest,
        f"Checkpoint: {goal}",
        project=project,
        task_id=task_id,
        checkpoint={"goal": goal, "next_steps": [f"continue {task_id}"]},
        **fields,
    )


def _context(manifest: dict, **scope) -> dict:
    scope.setdefault("domain", "global")
    scope.setdefault("project", None)
    scope.setdefault("task_id", None)
    return task_context(manifest, **scope)


# ---------------------------------------------------------------------------
# W4: no foreign checkpoint; open tasks instead
# ---------------------------------------------------------------------------


def test_without_a_task_there_is_no_checkpoint_to_resume_only_open_tasks(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    _checkpoint(loaded_manifest, "beta", "launch", "Ship the landing page")

    context = _context(loaded_manifest)

    assert context["checkpoint"] is None
    assert [(task["project"], task["task_id"], task["goal"]) for task in context["open_tasks"]] == [
        ("beta", "launch", "Ship the landing page"),
        ("alpha", "migrate", "Move storage to SQLite"),
    ]
    assert context["open_tasks"][0]["next_steps"] == ["continue launch"]


def test_open_tasks_show_each_task_once_with_its_latest_checkpoint(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Plan the migration")
    _record(loaded_manifest, "Checkpoint: run it", project="alpha", task_id="migrate",
            checkpoint={"goal": "Run the migration", "next_steps": ["back up", "migrate", "verify", "announce"]})

    tasks = _context(loaded_manifest)["open_tasks"]

    assert [task["goal"] for task in tasks] == ["Run the migration"]
    assert tasks[0]["next_steps"] == ["back up", "migrate", "verify"]  # the task's own scope has all of them


def test_a_completed_or_cancelled_task_is_not_open(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    _checkpoint(loaded_manifest, "beta", "launch", "Ship the landing page")
    _checkpoint(loaded_manifest, "gamma", "survey", "Survey users")
    _record(loaded_manifest, "Migration done", project="alpha", task_id="migrate",
            fact={"slot": "task.status", "value": "completed", "kind": "observed"})
    _record(loaded_manifest, "Survey dropped", project="gamma", task_id="survey",
            fact={"slot": "task.status", "value": "Cancelled", "kind": "decision"})

    assert [task["task_id"] for task in _context(loaded_manifest)["open_tasks"]] == ["launch"]


def test_a_project_scope_lists_only_its_own_open_tasks(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    _checkpoint(loaded_manifest, "beta", "launch", "Ship the landing page")

    context = _context(loaded_manifest, project="alpha")

    assert context["checkpoint"] is None
    assert [task["task_id"] for task in context["open_tasks"]] == ["migrate"]


def test_a_named_task_resumes_its_own_checkpoint(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")
    _checkpoint(loaded_manifest, "beta", "launch", "Ship the landing page")

    context = _context(loaded_manifest, project="alpha", task_id="migrate")

    assert context["checkpoint"]["checkpoint"]["goal"] == "Move storage to SQLite"
    assert "open_tasks" not in context


def test_quarantined_checkpoints_are_not_open_tasks(loaded_manifest) -> None:
    _checkpoint(loaded_manifest, "alpha", "migrate", "Hidden plan", memory_guard={"visibility": "raw_only"})

    assert _context(loaded_manifest)["open_tasks"] == []


# ---------------------------------------------------------------------------
# W3: history pages back with `before`; `since` still pages forward
# ---------------------------------------------------------------------------


def test_older_changes_page_back_until_the_first(loaded_manifest) -> None:
    texts = [f"Step {n}" for n in range(5)]
    for text in texts:
        _record(loaded_manifest, text, project="alpha", task_id="migrate")
    scope = {"project": "alpha", "task_id": "migrate", "limit": 2}

    first = _context(loaded_manifest, **scope)
    second = _context(loaded_manifest, **scope, before=first["before"])
    third = _context(loaded_manifest, **scope, before=second["before"])

    assert [change["text"] for change in first["changes"]] == ["Step 3", "Step 4"]
    assert [change["text"] for change in second["changes"]] == ["Step 1", "Step 2"]
    assert [change["text"] for change in third["changes"]] == ["Step 0"]
    assert (first["has_older"], second["has_older"], third["has_older"]) == (True, True, False)
    assert third["before"] is None


def test_the_forward_cursor_still_returns_only_new_changes(loaded_manifest) -> None:
    _record(loaded_manifest, "Step 0", project="alpha", task_id="migrate")
    first = _context(loaded_manifest, project="alpha", task_id="migrate")
    _record(loaded_manifest, "Step 1", project="alpha", task_id="migrate")

    later = _context(loaded_manifest, project="alpha", task_id="migrate", since=first["cursor"])

    assert [change["text"] for change in later["changes"]] == ["Step 1"]
    assert later["has_older"] is True  # Step 0 is behind this page


def test_cursors_are_bound_to_their_scope_and_direction(loaded_manifest) -> None:
    _record(loaded_manifest, "Step 0", project="alpha", task_id="migrate")
    _record(loaded_manifest, "Step 1", project="alpha", task_id="migrate")
    page = _context(loaded_manifest, project="alpha", task_id="migrate", limit=1)

    with pytest.raises(ValueError, match="another scope"):
        _context(loaded_manifest, project="beta", task_id="launch", before=page["before"])
    with pytest.raises(ValueError, match="exclusive"):
        _context(loaded_manifest, project="alpha", task_id="migrate", since=page["cursor"], before=page["before"])


# ---------------------------------------------------------------------------
# R6 / R7: a lean bundle and no quarantine leak
# ---------------------------------------------------------------------------


def test_the_startup_bundle_drops_sync_bookkeeping_and_the_repeated_checkpoint(loaded_manifest) -> None:
    _record(loaded_manifest, "Step 0", project="alpha", task_id="migrate")
    latest = _checkpoint(loaded_manifest, "alpha", "migrate", "Move storage to SQLite")

    bundle = service.build_startup_bundle(loaded_manifest, project="alpha", task_id="migrate", limit=3, compact=True)

    events = {event["id"]: event for event in bundle["recent_events"]}
    assert bundle["task_context"]["checkpoint"]["event_id"] == latest["id"]
    assert "checkpoint" not in events[latest["id"]]
    assert not any(key.startswith("mem0_") for event in events.values() for key in event)


def test_the_recent_events_tool_honours_quarantine(chronicle_sandbox, loaded_manifest) -> None:
    visible = _record(loaded_manifest, "Visible decision")
    hidden = _record(loaded_manifest, "Quarantined import", memory_guard={"visibility": "raw_only"})

    async def recent() -> list:
        server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile="chronicler")
        result = await server.call_tool("recent_events", {"domain": "global", "limit": 10})
        content = result[0] if isinstance(result, tuple) else result
        items = [json.loads(item.text) for item in content]  # one text block per listed event
        return items[0] if len(items) == 1 and isinstance(items[0], list) else items

    ids = {event["id"] for event in asyncio.run(recent())}

    assert visible["id"] in ids and hidden["id"] not in ids
