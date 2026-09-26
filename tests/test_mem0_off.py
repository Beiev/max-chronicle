"""An installation without Mem0: `[mem0] enabled = false` leaves every Mem0 path quiet."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json

import pytest

from max_chronicle import service
from max_chronicle.brief import build_brief
from max_chronicle.config import mem0_enabled
from max_chronicle.mcp_server import build_server
from max_chronicle.native_automation import (
    doctor_launchd,
    install_launchd,
    run_automation_job,
    sync_mem0_outbox,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import config_from_manifest, count_mem0_outbox, open_connection


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


@pytest.fixture()
def bridge_calls(chronicle_sandbox):
    """A Mem0 bridge that records each call instead of answering."""
    calls = chronicle_sandbox.status_root / "bridge-calls.txt"
    (chronicle_sandbox.status_root / "scripts" / "mem0_bridge.py").write_text(
        "#!/usr/bin/env python3\nimport sys\n"
        f"open({str(calls)!r}, 'a').write(' '.join(sys.argv[1:]) + '\\n')\nraise SystemExit(1)\n",
        encoding="utf-8",
    )
    return calls


@pytest.fixture()
def without_mem0(chronicle_sandbox) -> dict:
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("\n[mem0]\nenabled = false\n")
    return load_manifest(chronicle_sandbox.manifest_path)


def _with_mem0(manifest: dict) -> dict:
    """The same installation while it still ran Mem0."""
    return {**manifest, "mem0": {"enabled": True}}


def _record(manifest: dict, text: str, **fields) -> dict:
    """A decision as an agent writes it through MCP, which queues it for Mem0 while Mem0 runs."""
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "category": "decision",
                                           "text": text, **fields}, source_kind="chronicle_mcp")


def test_the_switch_reads_the_manifest() -> None:
    assert mem0_enabled({"mem0": {"enabled": False}, "paths": {"mem0_bridge": "bridge.py"}}) is False
    assert mem0_enabled({"mem0": {"enabled": True}}) is True
    assert mem0_enabled({"paths": {"mem0_bridge": "bridge.py"}}) is True  # as before the switch
    assert mem0_enabled({"paths": {}}) is False
    with pytest.raises(ValueError, match="true or false"):
        mem0_enabled({"mem0": {"enabled": "no"}})


def test_writes_queue_nothing_for_mem0(without_mem0) -> None:
    before = _record(_with_mem0(without_mem0), "Decided to move the gateway to port 9000 after the load test")
    after = _record(without_mem0, "Decided to keep the gateway on port 9000 for the next release")

    assert (before["mem0_status"], after["mem0_status"]) == ("queued", "skipped")
    assert count_mem0_outbox(config_from_manifest(without_mem0), status="pending") == 1  # the earlier row stays


def test_sync_and_dump_never_call_the_bridge(without_mem0, loaded_automation, bridge_calls) -> None:
    _record(_with_mem0(without_mem0), "Decided to move the gateway to port 9000 after the load test")

    sync = sync_mem0_outbox(without_mem0, loaded_automation, trigger_source="pytest")
    dump = run_automation_job(without_mem0, loaded_automation, job_name="mem0-dump", trigger_source="pytest")

    assert count_mem0_outbox(config_from_manifest(without_mem0), status="pending") == 1  # left as it was
    assert (sync["status"], sync["seen"], dump["status"]) == ("disabled", 0, "skipped")
    assert not bridge_calls.exists()


def test_a_skipped_dump_clears_the_brief_warning(without_mem0, loaded_automation) -> None:
    config = config_from_manifest(without_mem0)
    with open_connection(config) as connection, connection:
        connection.execute("INSERT INTO automation_runs(id, job_name, started_at_utc, status) "
                           "VALUES ('run-old', 'mem0-dump', '2026-09-26T05:00:00Z', 'failed')")
    now = datetime.now(timezone.utc)

    warned = build_brief(without_mem0, now=now)["text"]
    run_automation_job(without_mem0, loaded_automation, job_name="mem0-dump", trigger_source="pytest")

    assert "mem0-dump: failed" in warned and "mem0-dump" not in build_brief(without_mem0, now=now)["text"]


def test_recall_and_snapshots_leave_the_dump_alone(without_mem0, chronicle_sandbox) -> None:
    (chronicle_sandbox.status_root / "mem0-dump.json").write_text(json.dumps({"memories": [
        {"id": "m-1", "memory": "Marmoset gateway runs on port 9000", "metadata": {}}]}), encoding="utf-8")

    context = service.query_context(without_mem0, query="marmoset gateway", limit=5, mode="truth_plus_interpretation")
    snapshot = service.capture_runtime_snapshot(without_mem0, domain_id="global", agent="pytest")
    audit = service.build_freshness_audit(without_mem0, domain_id="global")
    sources = service.build_sources_audit(without_mem0, domain_id="global")

    assert context["mem0_dump_hits"] == []
    assert snapshot["mem0_snapshot_hits"] == {} and snapshot["mem0_dump"] == {"enabled": False}
    assert not [issue for issue in audit["issues"] if issue.get("source_id") == "mem0_dump"]
    assert "mem0_dump" not in json.dumps(sources["coverage"]) and all(
        entry.get("source_id") != "mem0_dump" for entry in sources.get("sources", []))


def test_the_audit_sees_no_backlog(without_mem0, loaded_automation) -> None:
    _record(_with_mem0(without_mem0), "Decided to move the gateway to port 9000 after the load test")
    assert count_mem0_outbox(config_from_manifest(without_mem0), status="pending") == 1

    result = run_automation_job(without_mem0, loaded_automation, job_name="weekly-audit", trigger_source="pytest")

    assert not [issue for issue in result.get("issues", []) if issue["kind"].startswith("mem0")]


def test_launchd_installs_and_checks_no_mem0_job(without_mem0, loaded_automation, chronicle_sandbox) -> None:
    dirs = {"agent_dir": chronicle_sandbox.root / "LaunchAgents",
            "runtime_dir": chronicle_sandbox.status_root / "runtime" / "launchd"}

    install = install_launchd(without_mem0, loaded_automation, log_dir=chronicle_sandbox.status_root / "logs",
                              load_jobs=False, **dirs)
    doctor = doctor_launchd(without_mem0, loaded_automation, **dirs)

    assert len(install["installed"]) == 4 and not any("mem0" in json.dumps(item) for item in install["installed"])
    assert not any("mem0" in json.dumps(job) for job in doctor["jobs"])


def test_the_mcp_server_offers_no_mem0(without_mem0, chronicle_sandbox) -> None:
    server = build_server(manifest_path=chronicle_sandbox.manifest_path)

    async def tools() -> dict:
        return {tool.name: tool for tool in await server.list_tools()}

    assert "search_mem0_live" not in asyncio.run(tools())
    assert "Mem0" not in server.instructions and "mem0" not in server.instructions
    assert service.search_mem0_live_service(without_mem0, query="anything")["status"] == "disabled"
