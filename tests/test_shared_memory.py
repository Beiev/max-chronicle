"""Cross-agent workflows, using only synthetic local evidence."""

from concurrent.futures import ThreadPoolExecutor

import pytest

from max_chronicle import service
from max_chronicle.store import config_from_manifest, open_connection


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda *args, **kwargs: None)


def record(manifest, text="Use the local ledger", **fields):
    return service.record_event(
        manifest,
        {
            "text": text,
            "agent": "agent-a",
            "category": "decision",
            "domain": "global",
            "project": "demo",
            "task_id": "ship",
            **fields,
        },
        dedupe=True,
        append_compat=False,
    )


def test_independent_confirmation_keeps_new_evidence(loaded_manifest, tmp_path):
    first, second = tmp_path / "first.txt", tmp_path / "second.txt"
    first.write_text("first proof")
    second.write_text("second proof")
    a = record(loaded_manifest, source_files=[str(first)])
    b = record(loaded_manifest, agent="agent-b", source_files=[str(second)])
    assert a["id"] == b["id"]
    assert a["observation_id"] != b["observation_id"]
    assert b["artifacts_written"] == 1
    startup = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship"
    )
    assert {o["agent"] for o in startup["task_context"]["changes"]} == {
        "agent-a",
        "agent-b",
    }


def test_request_retry_is_atomic_and_rejects_changed_content(loaded_manifest):
    def send(_):
        return record(loaded_manifest, request_id="request-1", session_id="session-1")

    with ThreadPoolExecutor(max_workers=2) as workers:
        results = list(workers.map(send, range(2)))
    assert len({r["id"] for r in results}) == 1
    assert len({r["observation_id"] for r in results}) == 1
    reconnected = record(
        loaded_manifest, request_id="request-1", session_id="session-2"
    )
    assert reconnected["observation_id"] == results[0]["observation_id"]
    with pytest.raises(ValueError, match="request_id"):
        record(
            loaded_manifest,
            text="Changed decision",
            request_id="request-1",
            session_id="session-1",
        )


def test_missing_source_is_explicit_in_success_receipt(loaded_manifest, tmp_path):
    result = record(loaded_manifest, source_files=[str(tmp_path / "missing.txt")])
    assert result["chronicle_status"] == "stored"
    assert result["evidence"][0]["status"] == "missing"


def test_agent_b_recovers_checkpoint_without_chat_history(loaded_manifest):
    checkpoint = {
        "goal": "Ship offline recall",
        "completed": ["Implemented lexical search"],
        "verification": ["Offline search test passed"],
        "open_questions": ["Vector threshold?"],
        "next_steps": ["Measure vector precision"],
    }
    record(loaded_manifest, checkpoint=checkpoint, session_id="session-a")
    record(loaded_manifest, text="Other task", task_id="other")
    bundle = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship", focus="Resume recall"
    )
    assert bundle["focus"] == "Resume recall"
    assert bundle["task_context"]["checkpoint"]["checkpoint"] == checkpoint
    assert all(o["task_id"] == "ship" for o in bundle["task_context"]["changes"])


def test_cursor_tracks_new_observation_of_old_event_and_is_scoped(loaded_manifest):
    record(loaded_manifest)
    first = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship"
    )
    record(loaded_manifest, agent="agent-b")
    second = service.build_startup_bundle(
        loaded_manifest,
        project="demo",
        task_id="ship",
        since=first["task_context"]["cursor"],
    )
    assert [o["agent"] for o in second["task_context"]["changes"]] == ["agent-b"]
    with pytest.raises(ValueError, match="cursor"):
        service.build_startup_bundle(
            loaded_manifest,
            project="demo",
            task_id="elsewhere",
            since=first["task_context"]["cursor"],
        )


def test_fact_replacement_preserves_history_and_rejects_stale_writer(loaded_manifest):
    first = record(
        loaded_manifest,
        text="Project active",
        fact={"slot": "status", "value": "active", "kind": "observed"},
    )
    second = record(
        loaded_manifest,
        text="Project paused",
        fact={
            "slot": "status",
            "value": "paused",
            "kind": "decision",
            "supersedes": first["fact_id"],
        },
    )
    with pytest.raises(ValueError, match="current fact"):
        record(
            loaded_manifest,
            text="Project finished",
            fact={
                "slot": "status",
                "value": "finished",
                "kind": "observed",
                "supersedes": first["fact_id"],
            },
        )
    bundle = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship"
    )
    assert [f["id"] for f in bundle["task_context"]["current_facts"]] == [
        second["fact_id"]
    ]
    recall = service.query_memory(
        loaded_manifest, query="Project", project="demo", task_id="ship"
    )
    assert [hit["event_id"] for hit in recall["results"]] == [second["id"]]
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM facts").fetchone()[0] == 2
        assert (
            connection.execute("SELECT COUNT(*) FROM fact_supersessions").fetchone()[0]
            == 1
        )


def test_task_ids_do_not_deduplicate_each_others_work(loaded_manifest):
    first = record(loaded_manifest)
    other = record(loaded_manifest, task_id="other")
    assert first["id"] != other["id"]


def test_fact_values_are_searchable_without_repeating_them_in_prose(loaded_manifest):
    result = record(
        loaded_manifest,
        text="Agreed storage strategy",
        fact={
            "slot": "database",
            "value": "SQLite",
            "kind": "decision",
        },
    )
    record(
        loaded_manifest,
        text="Other project",
        project="other",
        fact={
            "slot": "database",
            "value": "SQLite",
            "kind": "decision",
        },
    )
    hits = service.query_memory(loaded_manifest, query="SQLite", project="demo")[
        "results"
    ]
    assert [hit["event_id"] for hit in hits] == [result["id"]]
    assert hits[0]["provenance"]["facts"][0]["value"] == "SQLite"


def test_same_prose_fact_revision_and_confirmation_do_not_revive_old_knowledge(
    loaded_manifest,
):
    def write(value, **fields):
        return service.record_event(
            loaded_manifest,
            {
                "text": "Storage decision",
                "project": "demo",
                "agent": "agent-a",
                "fact": {
                    "slot": "database",
                    "value": value,
                    "kind": "decision",
                    **fields,
                },
            },
            append_compat=False,
        )

    first = write("SQLite")
    record(
        loaded_manifest,
        text="Confirmed SQLite storage decision",
        task_id=None,
        fact={"slot": "database", "value": "SQLite", "kind": "decision"},
    )
    second = write("PostgreSQL", supersedes=first["fact_id"])
    assert first["id"] != second["id"]
    assert (
        service.query_memory(loaded_manifest, query="SQLite", project="demo")["results"]
        == []
    )
    assert [
        r["event_id"]
        for r in service.query_memory(loaded_manifest, query="storage", project="demo")[
            "results"
        ]
    ] == [second["id"]]


def test_task_startup_inherits_project_facts_without_other_task_facts(loaded_manifest):
    shared = record(
        loaded_manifest,
        task_id=None,
        fact={"slot": "storage", "value": "local", "kind": "decision"},
    )
    local = record(
        loaded_manifest, fact={"slot": "state", "value": "ready", "kind": "observed"}
    )
    record(
        loaded_manifest,
        task_id="other",
        fact={"slot": "state", "value": "paused", "kind": "observed"},
    )
    context = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship"
    )["task_context"]
    assert {f["id"] for f in context["current_facts"]} == {
        shared["fact_id"],
        local["fact_id"],
    }
    assert context["facts_has_more"] is False


def test_unrelated_focus_does_not_receive_global_recent_events(loaded_manifest):
    record(loaded_manifest, text="Garden watering complete")
    bundle = service.build_startup_bundle(
        loaded_manifest, focus="spacecraft reactor", compact=True
    )
    assert bundle["recent_events"] == []
    assert bundle["task_context"]["changes"] == []
    assert bundle["entity_digest"] == []
    assert bundle["runtime"]["repos"] == []
    assert bundle["recall_status"]["degraded"] is True


def test_cursor_paginates_without_losing_changes(loaded_manifest):
    before = service.build_startup_bundle(
        loaded_manifest, project="demo", task_id="ship"
    )
    for i in range(5):
        record(loaded_manifest, text=f"Decision number {i}")
    cursor = before["task_context"]["cursor"]
    seen = []
    for _ in range(3):
        batch = service.build_startup_bundle(
            loaded_manifest, project="demo", task_id="ship", since=cursor, limit=2
        )["task_context"]
        seen.extend(change["text"] for change in batch["changes"])
        cursor = batch["cursor"]
    assert seen == [f"Decision number {i}" for i in range(5)]
    assert batch["has_more"] is False


@pytest.mark.parametrize("ingest_failure", [False, True])
def test_snapshot_receipt_preserves_success_when_derived_outputs_fail(
    loaded_manifest, monkeypatch, ingest_failure
):
    def fail(*args, **kwargs):
        raise OSError("simulated output failure")

    monkeypatch.setattr(service, "append_jsonl", fail)
    monkeypatch.setattr(service, "render_projections", fail)
    if ingest_failure:
        monkeypatch.setattr(service, "start_ingest_run", fail)
    result = service.capture_runtime_snapshot(
        loaded_manifest, domain_id="global", agent="test"
    )
    assert result["chronicle_status"] == "stored"
    expected = {"compat_snapshot", "projections"}
    if ingest_failure:
        expected.add("evidence_ingest")
    assert set(result["side_effect_errors"]) == expected


def test_two_mcp_clients_share_handoff_and_keep_distinct_sessions(chronicle_sandbox):
    import asyncio
    import json
    import os
    from pathlib import Path
    import sys
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    env = {
        **os.environ,
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "CHRONICLE_FEATURE_EVENT_EMBEDDINGS": "0",
        "OLLAMA_URL": "http://127.0.0.1:1",
    }
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "max_chronicle.mcp_server",
            "--manifest",
            str(chronicle_sandbox.manifest_path),
        ],
        env=env,
    )

    def decode(result):
        assert not result.isError, result.content
        return json.loads(result.content[0].text)

    async def run():
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(reader, writer) as a:
                await a.initialize()
                start_a = decode(
                    await a.call_tool(
                        "startup_bundle",
                        {"agent": "agent-a", "project": "demo", "task_id": "ship"},
                    )
                )
                receipt = decode(
                    await a.call_tool(
                        "record_event",
                        {
                            "text": "Offline recall implemented",
                            "project": "demo",
                            "task_id": "ship",
                            "request_id": "mcp-write-1",
                            "checkpoint": {
                                "goal": "Ship recall",
                                "verification": ["Offline test passed"],
                                "next_steps": ["Review search quality"],
                            },
                        },
                    )
                )
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(reader, writer) as b:
                await b.initialize()
                start_b = decode(
                    await b.call_tool(
                        "startup_bundle",
                        {"agent": "agent-b", "project": "demo", "task_id": "ship"},
                    )
                )
                assert start_a["session_id"] != start_b["session_id"]
                checkpoint = start_b["task_context"]["checkpoint"]
                assert checkpoint["observation_id"] == receipt["observation_id"]
                assert checkpoint["agent"] == "agent-a"
                assert checkpoint["checkpoint"]["next_steps"] == [
                    "Review search quality"
                ]
                hits = decode(
                    await b.call_tool(
                        "query_memory",
                        {
                            "query": "Offline recall",
                            "project": "demo",
                            "task_id": "ship",
                        },
                    )
                )
                assert hits["results"][0]["event_id"] == receipt["id"]

    asyncio.run(run())
