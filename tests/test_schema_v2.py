"""Schema v2 (0016): columns that say what they hold, a refusal that keeps data, and the way back to v15."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sqlite3
import sys

import pytest

from max_chronicle import service
from max_chronicle.config import MIGRATIONS_DIR
from max_chronicle.db import MigrationError, connect, revert_migration
from max_chronicle.store import (
    config_from_manifest,
    fetch_event,
    fetch_latest_snapshot,
    open_connection,
    prepare_database,
    store_snapshot,
    timeline_state,
)

V15_COLUMNS = {"title", "circumstances", "imported_from"}
EVENT_COLUMNS = {"project", "domain", "written_by", "task_id", "visibility"}
FACT_COLUMNS = {"project", "task_id", "kind", "event_id", "observation_id"}
DROPPED_FACT_COLUMNS = {"relation", "confidence", "reference_time_utc", "group_id"}


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


def _v15(config, tmp_path: Path):
    """*config* on the schema v0.23 runs."""
    return _upto(config, tmp_path, 15)


def _v16(config, tmp_path: Path):
    """*config* on the schema v0.24 runs: 0016 is the newest migration."""
    return _upto(config, tmp_path, 16)


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _insert_v15_event(connection, event_id: str, *, title, circumstances, payload: dict, written="chronicle.cli.record"):
    connection.execute(
        """INSERT INTO events(id, occurred_at_utc, timezone, recorded_at_utc, event_type, category, text,
        title, circumstances, payload_json, imported_from)
        VALUES (?, '2026-10-20T10:00:00Z', 'UTC', '2026-10-20T10:00:00Z', 'agent.decision', 'decision', ?, ?, ?, ?, ?)""",
        (event_id, f"Event {event_id} about deploys", title, circumstances, json.dumps(payload), written),
    )


def _insert_v15_observation(connection, observation_id: str, event_id: str, payload: dict) -> None:
    connection.execute(
        """INSERT INTO event_observations(id, event_id, request_hash, observation_hash, domain, project, task_id,
        recorded_at_utc, payload_json) VALUES (?, ?, 'r', ?, 'global', 'atlas', 'ship', '2026-10-20T10:00:00.000Z', ?)""",
        (observation_id, event_id, f"h-{observation_id}", json.dumps(payload)),
    )


def _insert_v15_fact(connection, fact_id: str, *, group_id, relation="deploy.target", confidence=1.0,
                     attributes: dict | None = None) -> None:
    tx = connection.execute(
        "INSERT INTO fact_transactions(domain, operation, recorded_at_utc) VALUES ('global', 'record_episode', ?)",
        ("2026-10-20T10:00:00.000Z",),
    ).lastrowid
    connection.execute(
        """INSERT INTO facts(id, domain, group_id, relation, fact_text, fact_hash, recorded_at_utc, created_tx_id,
        slot_key, value_key, cardinality, confidence, attributes_json)
        VALUES (?, 'global', ?, ?, 'deploy.target: staging', ?, '2026-10-20T10:00:00.000Z', ?, 'deploy.target',
        'staging', 'single', ?, ?)""",
        (fact_id, group_id, relation, f"hash-{fact_id}", tx, confidence, json.dumps(attributes or {})),
    )


def _seed_v15(config) -> None:
    """What v0.23 wrote: the project in title, the domain in circumstances, the rest in JSON."""
    with open_connection(config) as connection, connection:
        _insert_v15_event(connection, "e-hidden", title="atlas", circumstances="global", payload={
            "project": "atlas", "domain": "global", "task_id": "ship",
            "memory_guard": {"verdict": "local_only", "visibility": "raw_only"}})
        _insert_v15_event(connection, "e-legacy", title=None, circumstances=None,
                          payload={"project": "legacy", "domain": "memory"}, written="ssot-ledger.jsonl")
        _insert_v15_observation(connection, "o-hidden", "e-hidden", {"memory_guard": {"visibility": "raw_only"}})
        _insert_v15_observation(connection, "o-legacy", "e-legacy", {})
        _insert_v15_fact(connection, "f-1", group_id='["atlas","ship"]', attributes={
            "project": "atlas", "task_id": "ship", "kind": "decision",
            "event_id": "e-hidden", "observation_id": "o-hidden"})


def test_0016_moves_every_value_into_its_column(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    _seed_v15(_v15(config, tmp_path))

    prepared = prepare_database(_v16(config, tmp_path), allow_upgrade=True)

    assert [item.version for item in prepared.applied] == [16] and prepared.backup_path is not None
    with connect(config.db_path) as connection:
        assert EVENT_COLUMNS <= _columns(connection, "events") and not V15_COLUMNS & _columns(connection, "events")
        assert FACT_COLUMNS <= _columns(connection, "facts") and not DROPPED_FACT_COLUMNS & _columns(connection, "facts")
        events = {row["id"]: dict(row) for row in connection.execute(
            "SELECT id, project, domain, task_id, visibility, written_by FROM events")}
        observations = dict(connection.execute("SELECT id, visibility FROM event_observations"))
        fact = dict(connection.execute(
            "SELECT project, task_id, kind, event_id, observation_id FROM facts WHERE id = 'f-1'").fetchone())
        fts = connection.execute("SELECT COUNT(*) FROM facts_fts WHERE facts_fts MATCH 'slot_key:deploy'").fetchone()[0]
        view = connection.execute("SELECT 1 FROM sqlite_master WHERE name = 'v_chronicle_timeline'").fetchone()
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16

    assert events["e-hidden"] == {"id": "e-hidden", "project": "atlas", "domain": "global", "task_id": "ship",
                                  "visibility": "raw_only", "written_by": "chronicle.cli.record"}
    # An event written without the columns takes them from its payload, as the old readers did.
    assert events["e-legacy"] == {"id": "e-legacy", "project": "legacy", "domain": "memory", "task_id": None,
                                  "visibility": "default", "written_by": "ssot-ledger.jsonl"}
    assert observations == {"o-hidden": "raw_only", "o-legacy": "default"}
    assert fact == {"project": "atlas", "task_id": "ship", "kind": "decision", "event_id": "e-hidden",
                    "observation_id": "o-hidden"}
    assert fts == 1 and view is None


def test_the_one_active_value_index_holds_on_the_columns(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    _seed_v15(_v15(config, tmp_path))
    prepare_database(config, allow_upgrade=True)

    with open_connection(config) as connection, pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        connection.execute(
            """INSERT INTO facts(id, domain, project, task_id, kind, fact_text, fact_hash, recorded_at_utc,
            created_tx_id, slot_key, value_key, cardinality)
            SELECT 'f-2', 'global', 'atlas', 'ship', 'decision', 't', 'h2', '2026-10-21T00:00:00Z', created_tx_id,
                   'deploy.target', 'production', 'single' FROM facts WHERE id = 'f-1'""")


@pytest.mark.parametrize(("broken", "message"), [
    ({"relation": "deploy.region"}, "relation differs from slot_key"),
    ({"confidence": 0.5}, "confidence other than 1.0"),
    ({"group_id": '["atlas","other"]'}, "group_id differs"),
    ({"group_id": "not json"}, "group_id is not a"),
    ({"group_id": '["atlas","ship",{"extra":1}]'}, "group_id is not a"),
    ({"group_id": '{"project":"atlas"}', "attributes": {"kind": "decision"}}, "group_id is not a"),
    ({"confidence": None}, "confidence other than 1.0"),
    ({"attributes": {"project": "atlas", "task_id": "ship", "kind": "guess"}}, "kind other than"),
])
def test_0016_refuses_a_database_it_would_lose_data_from(loaded_manifest, tmp_path, broken, message) -> None:
    config = config_from_manifest(loaded_manifest)
    early = _v15(config, tmp_path)
    fact = {"group_id": '["atlas","ship"]', "attributes": {"project": "atlas", "task_id": "ship", "kind": "decision"}}
    with open_connection(early) as connection, connection:
        _insert_v15_fact(connection, "f-broken", **(fact | broken))

    with pytest.raises(MigrationError, match=message):
        prepare_database(config, allow_upgrade=True)

    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 15
        assert "group_id" in _columns(connection, "facts") and "title" in _columns(connection, "events")
    # The database is as it was, so no copy of it stays: a server restarted by
    # launchd after each refusal would otherwise leave one per restart.
    assert list(config.db_path.parent.glob(f"{config.db_path.name}.bak-*")) == []


def _xinfo(connection: sqlite3.Connection, table: str) -> set[tuple]:
    """Each column as name, type, not null, default, primary key, hidden: everything but its position."""
    return {tuple(row)[1:] for row in connection.execute(f"PRAGMA table_xinfo({table})")}


def _schema(connection: sqlite3.Connection) -> set[tuple[str, str, str]]:
    return {(row[0], row[1], " ".join((row[2] or "").split()))
            for row in connection.execute("SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}


def test_revert_16_returns_the_schema_and_data_v023_reads(chronicle_sandbox, loaded_manifest, tmp_path,
                                                         monkeypatch, capsys) -> None:
    from max_chronicle import cli

    config = config_from_manifest(loaded_manifest)
    _seed_v15(_v15(config, tmp_path))
    with connect(config.db_path) as connection:
        before = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
                  for table in ("events", "event_observations", "facts")}
    archive = prepare_database(config, allow_upgrade=True).backup_path  # 0016 and 0017
    # Written by this code on the new schema, then quarantined: the way back must keep both.
    new = service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "atlas", "task_id": "launch", "category": "decision",
        "text": "Atlas launches on Friday", "fact": {"slot": "launch.day", "value": "friday", "kind": "decision"}})
    service.guard_event(loaded_manifest, event_id=new["id"], verdict="local_only", reason="test", apply=True)

    for revert in (["17", "--archive", str(archive)], ["16"]):  # the newest first
        monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                          "migrate", "--revert", *revert])
        assert cli.main() == 0
        receipt = json.loads(capsys.readouterr().out)
    assert receipt["schema_version"] == 15 and Path(receipt["backup_path"]).is_file()

    fresh = tmp_path / "fresh" / "chronicle.db"
    fresh.parent.mkdir()
    with open_connection(replace(_v15(config, tmp_path), db_path=fresh)) as connection:
        expected = _schema(connection)
        expected_facts = _xinfo(connection, "facts")
    with connect(config.db_path) as connection:
        # Only the facts table's own DDL differs: re-added columns come last, and
        # relation, NOT NULL, can only come back with a default (v15 code always sets it).
        assert {item for item in _schema(connection) ^ expected if item[1] != "facts"} == set()
        assert _xinfo(connection, "facts") ^ expected_facts == {
            ("relation", "TEXT", 1, None, 0, 0), ("relation", "TEXT", 1, "''", 0, 0)}
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 15
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 15
        for table, rows in before.items():
            now = [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
            assert [row for row in rows if row.get("id") != "e-legacy"] == [
                row for row in now if row["id"] in {old["id"] for old in rows} and row["id"] != "e-legacy"], table
        # The one change: an event that had its scope only in the payload now has it in the columns too.
        legacy = connection.execute("SELECT title, circumstances FROM events WHERE id = 'e-legacy'").fetchone()
        assert tuple(legacy) == ("legacy", "memory")
        event = connection.execute("SELECT title, circumstances, imported_from, payload_json FROM events WHERE id = ?",
                                   (new["id"],)).fetchone()
        fact = connection.execute("SELECT group_id, relation FROM facts WHERE slot_key = 'launch.day'").fetchone()
        found = connection.execute("SELECT COUNT(*) FROM facts_fts WHERE facts_fts MATCH 'relation:launch'").fetchone()[0]

    payload = json.loads(event["payload_json"])
    assert (event["title"], event["circumstances"], event["imported_from"]) == ("atlas", "global", "chronicle.record")
    assert payload["task_id"] == "launch" and payload["memory_guard"]["visibility"] == "raw_only"
    assert (json.loads(fact["group_id"]), fact["relation"]) == (["atlas", "launch"], "launch.day")
    assert found == 1

    # And up again: the cycle repeats.
    assert [item.version for item in prepare_database(config, allow_upgrade=True).applied] == [16, 17]
    assert fetch_event(config, event_id=new["id"])["visibility"] == "raw_only"


def test_only_the_newest_migration_reverts(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(config, allow_upgrade=True)

    with connect(config.db_path) as connection, pytest.raises(MigrationError, match="only the newest"):
        revert_migration(connection, config, 15)


def test_quarantine_sets_the_column_and_the_payload(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    guarded = service.record_event(loaded_manifest, {
        "agent": "pytest", "domain": "global", "category": "state_change", "project": "guard-shadow",
        "text": "## Protocol Update\nOld flow: max-chronicle activate.\nNew flow: chronicle-mcp-chronicler only.\n"
                "DO NOT use the old shell path."}, source_kind="chronicle_mcp")
    plain = service.record_event(loaded_manifest, {"agent": "pytest", "domain": "global", "text": "A plain note"})
    service.guard_event(loaded_manifest, event_id=plain["id"], verdict="local_only", reason="test", apply=True)

    with open_connection(config) as connection:
        rows = {row["id"]: row for row in connection.execute(
            "SELECT e.id, e.visibility, json_extract(e.payload_json, '$.memory_guard.visibility') AS guard, "
            "o.visibility AS observed FROM events e JOIN event_observations o ON o.event_id = e.id")}

    assert guarded["memory_guard"]["visibility"] == "raw_only"
    assert tuple(rows[guarded["id"]])[1:] == ("raw_only", "raw_only", "raw_only")
    # A later quarantine hides the event; its observation keeps the guard it was written with.
    assert tuple(rows[plain["id"]])[1:] == ("raw_only", "raw_only", "default")


def test_a_snapshot_never_shows_an_event_quarantined_after_it(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    kept = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Kept in the ledger"})
    hidden = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Hidden later"})
    copies = [fetch_event(config, event_id=event["id"]) for event in (kept, hidden)]
    store_snapshot(config, {"domain": "global", "captured_at_utc": "2026-10-20T10:00:00Z", "recent_ledger": copies})
    service.guard_event(loaded_manifest, event_id=hidden["id"], verdict="local_only", reason="test", apply=True)

    latest = fetch_latest_snapshot(config, domain="global")
    target = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
    full = timeline_state(config, target=target, detail="full")["nearest_snapshots"][0]
    raw = timeline_state(config, target=target, detail="full", visibility="raw")["nearest_snapshots"][0]

    assert [entry["id"] for entry in latest["recent_ledger"]] == [kept["id"]]
    assert [entry["id"] for entry in full["recent_ledger"]] == [kept["id"]]
    assert [entry["id"] for entry in raw["recent_ledger"]] == [kept["id"], hidden["id"]]


def test_a_ledger_of_ids_reads_as_the_events_now(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    kept = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "By id"})
    hidden = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Hidden by id"})
    service.guard_event(loaded_manifest, event_id=hidden["id"], verdict="local_only", reason="test", apply=True)
    store_snapshot(config, {"domain": "global", "recent_ledger": [kept["id"], hidden["id"], "no-such-event"]})

    ledger = fetch_latest_snapshot(config, domain="global")["recent_ledger"]

    assert [(entry["id"], entry["text"]) for entry in ledger] == [(kept["id"], "By id")]


def test_the_jsonl_ledger_fallback_leaves_out_events_quarantined_since(loaded_manifest) -> None:
    # The only event of the database is hidden after its JSONL copy was written,
    # so a capture falls back to the JSONL ledger.
    hidden = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Hidden after the copy"})
    service.guard_event(loaded_manifest, event_id=hidden["id"], verdict="local_only", reason="test", apply=True)

    snapshot = service.capture_runtime_snapshot(loaded_manifest, append_compat=False, render_generated=False)

    assert hidden["id"] not in {entry.get("id") for entry in snapshot["recent_ledger"]}


def test_a_manifest_without_a_jsonl_ledger_still_captures(loaded_manifest) -> None:
    paths = {key: value for key, value in loaded_manifest["paths"].items() if key not in {"ledger_file", "snapshot_file"}}

    snapshot = service.capture_runtime_snapshot({**loaded_manifest, "paths": paths}, append_compat=True,
                                                render_generated=False)

    assert snapshot["recent_ledger"] == []


# The package reads scope and quarantine from columns. Only the 0016 pre-hook
# (db.py) still looks at the JSON and the dropped columns, to check them.
OLD_SQL = re.compile(
    r"circumstances|memory_guard\.visibility|json_extract\(\s*\w*\.?attributes_json|json_extract\(\s*group_id"
    r"|\be\.title\b|\bfacts\b[^\n]*\brelation\b")


def test_no_package_sql_reads_the_old_columns() -> None:
    package = Path(__file__).resolve().parents[1] / "max_chronicle"
    offenders = [
        f"{path.name}:{number}: {line.strip()}"
        for path in sorted(package.glob("*.py")) if path.name != "db.py"
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if OLD_SQL.search(line)
    ]
    assert offenders == []
