"""0018: Chronicle no longer mirrors events into Mem0, and the way back reads the pre-upgrade copy."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sys

import pytest

from max_chronicle import service
from max_chronicle.automation import load_automation_config
from max_chronicle.config import MIGRATIONS_DIR
from max_chronicle.db import connect, revert_migration
from max_chronicle.mcp_server import build_server
from max_chronicle.runtime_context import load_manifest
from max_chronicle.scheduler import Scheduler
from max_chronicle.store import config_from_manifest, open_connection, prepare_database, store_snapshot

MEM0_KEYS = ("mem0_status", "mem0_error", "mem0_raw", "mem0_synced_at")


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _upto(config, tmp_path: Path, version: int):
    """*config* on the migrations up to *version*, with their down scripts."""
    early = tmp_path / f"migrations-v{version}"
    (early / "down").mkdir(parents=True, exist_ok=True)
    for path in [*MIGRATIONS_DIR.glob("*.sql"), *(MIGRATIONS_DIR / "down").glob("*.sql")]:
        if int(path.name[:4]) <= version:
            (early / path.relative_to(MIGRATIONS_DIR)).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return replace(config, migrations_dir=early)


# What v0.25 stored while it ran Mem0, as Python wrote it.
SYNCED_PAYLOAD = {"domain": "global", "project": "atlas", "text": "Atlas ships", "mem0_status": "stored",
                  "mem0_raw": '{"results": []}', "mem0_synced_at": "2026-10-20T10:05:00Z", "mem0_error": None}
OBSERVATION_PAYLOAD = {**SYNCED_PAYLOAD, "evidence": []}
OLD_SNAPSHOT = {"domain": "global", "recent_ledger": ["e-synced"], "repos": [],
                "mem0_dump": {"exists": True, "path": "/status/mem0-dump.json", "total_memories": 87}}


def _seed_v17(config) -> None:
    with connect(config.db_path) as connection:
        for event_id, status, payload in (("e-synced", "stored", SYNCED_PAYLOAD),
                                          ("e-plain", "skipped", {"domain": "global", "text": "Plain"})):
            connection.execute(
                """INSERT INTO events(id, occurred_at_utc, timezone, recorded_at_utc, event_type, category, text,
                project, domain, payload_json, written_by, mem0_status, mem0_error)
                VALUES (?, '2026-10-20T10:00:00Z', 'UTC', '2026-10-20T10:00:00Z', 'agent.decision', 'decision',
                        ?, 'atlas', 'global', ?, 'chronicle.record', ?, NULL)""",
                (event_id, payload["text"], json.dumps(payload, sort_keys=True), status))
        connection.execute(
            """INSERT INTO mem0_outbox(id, event_id, collection_name, status, attempts, payload_json, created_at_utc,
            synced_at_utc) VALUES ('o-1', 'e-synced', 'personal', 'synced', 1, '{"text": "Atlas ships"}',
            '2026-10-20T10:00:00Z', '2026-10-20T10:05:00Z')""")
        connection.execute(
            """INSERT INTO event_observations(id, event_id, request_hash, observation_hash, domain, project,
            recorded_at_utc, payload_json) VALUES ('ob-1', 'e-synced', 'h', 'oh', 'global', 'atlas',
            '2026-10-20T10:00:00Z', ?)""", (json.dumps(OBSERVATION_PAYLOAD, sort_keys=True),))
        connection.execute(
            "INSERT INTO snapshots(id, captured_at_utc, captured_at_local, timezone, agent, domain, title, payload_json) "
            "VALUES ('s-old', '2026-10-20T11:00:00Z', '2026-10-20T13:00:00+02:00', 'Europe/Warsaw', 'pytest', "
            "'global', 'Before 0018', ?)", (json.dumps(OLD_SNAPSHOT),))


def _rows(connection, table: str) -> list[dict]:
    return [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]


def test_0018_removes_the_mirror_and_keeps_everything_else(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(_upto(config, tmp_path, 17), allow_upgrade=True)
    _seed_v17(config)

    prepared = prepare_database(config, allow_upgrade=True)

    assert [item.version for item in prepared.applied] == [18] and prepared.backup_path is not None
    assert prepared.compaction == "incremental_vacuum"
    with open_connection(config) as connection:
        assert connection.execute("SELECT name FROM sqlite_master WHERE name LIKE '%mem0%'").fetchall() == []
        assert not {row[1] for row in connection.execute("PRAGMA table_info(events)")} & set(MEM0_KEYS)
        payloads = {row[0]: json.loads(row[1]) for row in connection.execute("SELECT id, payload_json FROM events")}
        observation = json.loads(connection.execute("SELECT payload_json FROM event_observations").fetchone()[0])
        snapshot = json.loads(connection.execute("SELECT payload_json FROM snapshots").fetchone()[0])
        found = connection.execute("SELECT event_id FROM events_fts WHERE events_fts MATCH 'atlas'").fetchall()
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 18
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    assert payloads == {"e-synced": {"domain": "global", "project": "atlas", "text": "Atlas ships"},
                        "e-plain": {"domain": "global", "text": "Plain"}}
    assert observation == {"domain": "global", "project": "atlas", "text": "Atlas ships", "evidence": []}
    assert snapshot == {"domain": "global", "recent_ledger": ["e-synced"], "repos": []}
    assert [tuple(row) for row in found] == [("e-synced",)]
    event = service.fetch_event(config, event_id="e-synced")
    assert not set(MEM0_KEYS) & set(event) and event["text"] == "Atlas ships"


def test_revert_18_brings_the_mirror_back_from_the_archive(chronicle_sandbox, loaded_manifest, tmp_path,
                                                           monkeypatch, capsys) -> None:
    from max_chronicle import cli

    config = config_from_manifest(loaded_manifest)
    v17 = _upto(config, tmp_path, 17)
    prepare_database(v17, allow_upgrade=True)
    _seed_v17(config)
    tables = ("events", "event_observations", "snapshots", "mem0_outbox")
    with connect(config.db_path) as connection:
        before = {table: _rows(connection, table) for table in tables}
    archive = prepare_database(config, allow_upgrade=True).backup_path
    # Written on schema 18, then the way back: v0.25 must read all of it.
    new = service.record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "project": "atlas",
                                                 "category": "decision", "text": "Atlas launches on Friday"})
    store_snapshot(config, {"id": "s-new", "domain": "global", "title": "After 0018", "recent_ledger": [new["id"]]})

    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "migrate", "--revert", "18", "--archive", str(archive)])
    assert cli.main() == 0
    receipt = json.loads(capsys.readouterr().out)

    assert receipt["schema_version"] == 17 and Path(receipt["backup_path"]).is_file()
    with connect(config.db_path) as connection:
        for table, rows in before.items():
            now = _rows(connection, table)
            assert now[:len(rows)] == rows, table
        added = connection.execute("SELECT mem0_status, mem0_error, payload_json FROM events WHERE id = ?",
                                   (new["id"],)).fetchone()
        new_snapshot = json.loads(connection.execute(
            "SELECT payload_json FROM snapshots WHERE id = 's-new'").fetchone()[0])
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert {"idx_mem0_outbox_status"} <= {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'")}

    # Written on 18: no sync state to give back, and a capture that ran without Mem0 says so.
    assert (added["mem0_status"], added["mem0_error"]) == (None, None)
    assert not set(MEM0_KEYS) & set(json.loads(added["payload_json"]))
    assert new_snapshot["mem0_dump"] == {"enabled": False}
    # And up again.
    assert [item.version for item in prepare_database(config, allow_upgrade=True).applied] == [18]


def test_an_old_client_cannot_write_sync_state(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    stored = service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "category": "decision", "text": "Keep the gateway on 9000",
        "mem0_status": "queued", "mem0_error": None, "mem0_raw": None}, source_kind="chronicle_mcp")

    with open_connection(config) as connection:
        payload = json.loads(connection.execute("SELECT payload_json FROM events WHERE id = ?",
                                                (stored["id"],)).fetchone()[0])
        observation = json.loads(connection.execute("SELECT payload_json FROM event_observations WHERE event_id = ?",
                                                    (stored["id"],)).fetchone()[0])
    assert not set(MEM0_KEYS) & (set(payload) | set(observation) | set(stored))


def test_a_legacy_import_leaves_the_sync_state_behind(loaded_manifest) -> None:
    from max_chronicle import bootstrap

    config = config_from_manifest(loaded_manifest)
    config.ledger_path.write_text(json.dumps({"id": "legacy-1", "text": "Imported decision", "domain": "global",
                                              "category": "decision", "mem0_status": "stored"}) + "\n",
                                  encoding="utf-8")
    with open_connection(config) as connection, connection:
        bootstrap.import_legacy_ledger(connection, config)
        payload = json.loads(connection.execute("SELECT payload_json FROM events WHERE id = 'legacy-1'").fetchone()[0])

    assert payload["text"] == "Imported decision" and "mem0_status" not in payload


@pytest.fixture()
def old_settings(chronicle_sandbox) -> dict:
    """A manifest and automation config from the Mem0 days, unchanged."""
    with chronicle_sandbox.manifest_path.open("a", encoding="utf-8") as handle:
        handle.write("\n[mem0]\nenabled = true\n")
    text = chronicle_sandbox.manifest_path.read_text(encoding="utf-8").replace(
        "[paths]\n", f"[paths]\nmem0_dump = \"{chronicle_sandbox.status_root / 'mem0-dump.json'}\"\n"
                     f"mem0_bridge = \"{chronicle_sandbox.status_root / 'scripts' / 'mem0_bridge.py'}\"\n", 1)
    chronicle_sandbox.manifest_path.write_text(text, encoding="utf-8")
    with chronicle_sandbox.automation_path.open("a", encoding="utf-8") as handle:
        handle.write("\n[jobs.mem0_dump]\nhour = 7\nminute = 0\nthrottle_seconds = 900\n")
    return load_manifest(chronicle_sandbox.manifest_path)


def test_old_settings_load_and_run_nothing_for_mem0(old_settings, chronicle_sandbox, monkeypatch, capsys) -> None:
    from max_chronicle import cli, doctor

    automation = load_automation_config(chronicle_sandbox.automation_path)
    scheduler = Scheduler(lambda: old_settings, lambda: automation, lambda job, occurrence: 0)
    jobs = {decision.job for decision in scheduler.decide()}
    prompt = service.build_activation(old_settings, domain_id="global", agent="pytest", capture=True)["prompt"]
    summary = doctor.runtime_component(old_settings, automation, server_url="http://127.0.0.1:9")

    assert "mem0_dump" in automation.jobs and "mem0-dump" not in jobs
    assert jobs == {"daily-capture", "daybook", "weekly-audit", "backup"}
    assert {job["job_name"] for job in summary["jobs"]} == jobs
    assert "Mem0" not in prompt
    for command in (["sync-mem0"], ["repair-mem0-state"], ["backfill-mem0-queue"],
                    ["automation", "run", "--job", "mem0-dump"]):
        monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path), *command])
        with pytest.raises(SystemExit):
            cli.main()
    capsys.readouterr()


def test_the_mcp_server_offers_no_mem0(old_settings, chronicle_sandbox) -> None:
    for profile in ("chronicler", "admin"):
        server = build_server(manifest_path=chronicle_sandbox.manifest_path, profile=profile)

        async def tools() -> list:
            return list(await server.list_tools())

        listed = asyncio.run(tools())
        assert not [tool.name for tool in listed if "mem0" in tool.name.casefold()]
        assert "mem0" not in server.instructions.casefold()
        assert "mem0" not in json.dumps([tool.model_dump(mode="json") for tool in listed]).casefold(), profile


def _upgraded_from_v17(loaded_manifest, tmp_path: Path):
    """A seeded v17 database brought to 18, and the copy taken before."""
    config = config_from_manifest(loaded_manifest)
    prepare_database(_upto(config, tmp_path, 17), allow_upgrade=True)
    _seed_v17(config)
    return config, prepare_database(config, allow_upgrade=True).backup_path


def _revert_18(config, archive: Path) -> None:
    with connect(config.db_path) as connection:
        revert_migration(connection, config, 18, archive=archive)


def test_revert_18_carries_a_later_quarantine_to_the_outbox(loaded_manifest, tmp_path) -> None:
    """v0.25 syncs by the outbox's copy of the guard; one set on 18 must reach it."""
    config, archive = _upgraded_from_v17(loaded_manifest, tmp_path)
    service.guard_event(loaded_manifest, event_id="e-synced", verdict="local_only", reason="test", apply=True)

    _revert_18(config, archive)

    with connect(config.db_path) as connection:
        outbox = json.loads(connection.execute(
            "SELECT payload_json FROM mem0_outbox WHERE event_id = 'e-synced'").fetchone()[0])
    assert outbox["memory_guard"]["verdict"] == "local_only" and outbox["text"] == "Atlas ships"


def test_revert_18_keeps_a_snapshot_stored_again_since(loaded_manifest, tmp_path) -> None:
    config, archive = _upgraded_from_v17(loaded_manifest, tmp_path)
    # Stored again under its id with a ledger as long and everything else as it was.
    with connect(config.db_path) as connection:
        connection.execute("UPDATE snapshots SET payload_json = json_set(payload_json, '$.recent_ledger', "
                           "json_array('e-plain')) WHERE id = 's-old'")

    _revert_18(config, archive)

    with connect(config.db_path) as connection:
        payload = json.loads(connection.execute("SELECT payload_json FROM snapshots WHERE id = 's-old'").fetchone()[0])
    assert payload["recent_ledger"] == ["e-plain"] and payload["mem0_dump"] == OLD_SNAPSHOT["mem0_dump"]


def test_install_takes_down_a_mem0_dump_agent_and_doctor_shows_it(loaded_manifest, loaded_automation,
                                                                  chronicle_sandbox) -> None:
    from max_chronicle.native_automation import RETIRED_JOB_SUFFIXES, doctor_launchd, install_launchd, launchd_labels

    dirs = {"agent_dir": chronicle_sandbox.root / "agents", "runtime_dir": chronicle_sandbox.root / "rt"}
    label = launchd_labels(loaded_automation, RETIRED_JOB_SUFFIXES)["mem0_dump"]
    dirs["agent_dir"].mkdir()
    (dirs["agent_dir"] / f"{label}.plist").write_text("<plist/>", encoding="utf-8")

    before = doctor_launchd(loaded_manifest, loaded_automation, **dirs)
    install = install_launchd(loaded_manifest, loaded_automation, log_dir=chronicle_sandbox.root / "logs",
                              load_jobs=False, **dirs)
    after = doctor_launchd(loaded_manifest, loaded_automation, **dirs)

    assert before["status"] == "issues"
    assert [job["issues"] for job in before["jobs"] if job["label"] == label] == [["retired_job_installed"]]
    assert install["retired"] == [label] and (dirs["runtime_dir"] / "retired-job-agents" / f"{label}.plist").exists()
    assert label not in {job["label"] for job in after["jobs"]}


def test_a_legacy_snapshot_leaves_the_mem0_copies_behind(loaded_manifest) -> None:
    from max_chronicle.bootstrap import bootstrap_legacy

    config = config_from_manifest(loaded_manifest)
    config.snapshot_path.write_text(json.dumps({
        "id": "legacy-snap", "captured_at_utc": "2026-03-01T10:00:00Z", "domain": "global", "recent_ledger": [],
        "mem0_dump": {"exists": True}, "mem0_snapshot_hits": {"q": [{"memory": "y" * 400}]}}) + "\n",
        encoding="utf-8")

    with open_connection(config) as connection, connection:
        bootstrap_legacy(connection, config)
        payload = json.loads(connection.execute(
            "SELECT payload_json FROM snapshots WHERE id = 'legacy-snap'").fetchone()[0])

    assert payload["domain"] == "global" and not {"mem0_dump", "mem0_snapshot_hits"} & set(payload)
