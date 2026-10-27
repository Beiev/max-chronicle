"""0017: dead tables and copies go, snapshots keep event ids, and the way back reads the pre-upgrade copy."""

from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pytest

from max_chronicle import db, service
from max_chronicle.config import MIGRATIONS_DIR
from max_chronicle.db import MigrationError, connect, revert_migration
from max_chronicle.store import (
    config_from_manifest,
    fetch_latest_snapshot,
    open_connection,
    prepare_database,
    store_snapshot,
)

DEAD_TABLES = {"llm_calls", "recall_outbox", "reconciliation_runs", "session_messages",
               "forecast_reviews", "scenario_runs", "lens_runs", "situation_models"}
DEAD_OBJECTS = DEAD_TABLES | {"snapshots_fts", "snapshots_ai", "snapshots_au", "snapshots_ad",
                              "idx_outbox_status", "idx_snapshots_domain"}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _v16(config, tmp_path: Path):
    """*config* on the schema v0.24 runs: 0016 is the newest migration."""
    early = tmp_path / "migrations-v16"
    (early / "down").mkdir(parents=True, exist_ok=True)
    for path in [*MIGRATIONS_DIR.glob("*.sql"), *(MIGRATIONS_DIR / "down").glob("*.sql")]:
        if int(path.name[:4]) <= 16:
            (early / path.relative_to(MIGRATIONS_DIR)).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return replace(config, migrations_dir=early)


def _insert_event(connection, event_id: str) -> None:
    connection.execute(
        """INSERT INTO events(id, occurred_at_utc, timezone, recorded_at_utc, event_type, category, text,
        project, domain, payload_json, written_by)
        VALUES (?, '2026-10-20T10:00:00Z', 'UTC', '2026-10-20T10:00:00Z', 'agent.decision', 'decision', ?,
                'atlas', 'global', '{"project": "atlas", "domain": "global"}', 'chronicle.record')""",
        (event_id, f"Event {event_id}"),
    )


def _insert_snapshot(connection, snapshot_id: str, payload: dict, *, hour: int = 11) -> None:
    connection.execute(
        "INSERT INTO snapshots(id, captured_at_utc, captured_at_local, timezone, agent, domain, title, payload_json) "
        "VALUES (?, ?, ?, 'Europe/Warsaw', 'pytest', 'global', ?, ?)",
        (snapshot_id, f"2026-10-20T{hour:02d}:00:00Z", f"2026-10-20T{hour + 2:02d}:00:00+02:00",
         f"Snapshot {snapshot_id}", json.dumps(payload)),
    )


# What v0.24 stored: copies of events in the ledger, Mem0 recall copies, and a
# situation layer nothing reads.
OLD_LEDGER = [
    {"id": "e-1", "text": "Event e-1", "project": "atlas"},
    "e-2",                                          # an id already
    {"id": "e-gone", "text": "Not in this database"},
    {"text": "A copy without an id"},
    7,
]


def _seed_v16(config) -> None:
    with connect(config.db_path) as connection:
        _insert_event(connection, "e-1")
        _insert_event(connection, "e-2")
        _insert_snapshot(connection, "s-old", {
            "domain": "global", "recent_ledger": OLD_LEDGER, "mem0_dump": {"enabled": False},
            "mem0_snapshot_hits": {"deploy": [{"memory": "copy of a Mem0 hit"}]}, "repos": []})
        _insert_snapshot(connection, "s-plain", {"domain": "global", "title": "No ledger"}, hour=9)
        connection.execute(
            "INSERT INTO situation_models(id, domain, valid_at_utc, payload_json, derived_from_json, created_at_utc, "
            "updated_at_utc) VALUES ('sm-1', 'global', '2026-06-04T00:20:00Z', '{}', '[]', '2026-06-04T00:20:00Z', "
            "'2026-06-04T00:20:00Z')")
        connection.execute(
            "INSERT INTO lens_runs(id, situation_id, lens, confidence, findings_json, evidence_refs_json, "
            "created_at_utc, updated_at_utc) VALUES ('lr-1', 'sm-1', 'risk', 0.5, '[]', '[]', "
            "'2026-06-04T00:20:00Z', '2026-06-04T00:20:00Z')")


def _objects(connection) -> set[str]:
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}


def _schema(connection) -> set[tuple[str, str, str]]:
    return {(row[0], row[1], " ".join((row[2] or "").split()))
            for row in connection.execute("SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}


def _stored_payload(connection, snapshot_id: str) -> dict:
    return json.loads(connection.execute("SELECT payload_json FROM snapshots WHERE id = ?", (snapshot_id,)).fetchone()[0])


def _legacy_auto_vacuum(config) -> None:
    """A database from before INCREMENTAL auto-vacuum, like the production one."""
    with sqlite3.connect(config.db_path) as connection:
        connection.execute("PRAGMA auto_vacuum = NONE")
        connection.execute("VACUUM")


def test_0017_drops_what_nothing_reads_and_keeps_ids(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(_v16(config, tmp_path), allow_upgrade=True)
    _seed_v16(config)
    _legacy_auto_vacuum(config)

    prepared = prepare_database(config, allow_upgrade=True)

    assert [item.version for item in prepared.applied] == [17] and prepared.backup_path is not None
    assert prepared.compaction == "vacuum, auto_vacuum=incremental"
    with open_connection(config) as connection:
        assert not DEAD_OBJECTS & _objects(connection)
        assert {"idx_mem0_outbox_status", "idx_snapshots_domain_captured"} <= _objects(connection)
        old = _stored_payload(connection, "s-old")
        plain = connection.execute("SELECT payload_json FROM snapshots WHERE id = 's-plain'").fetchone()[0]
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
        assert connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 2
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    # In order: an event of this database becomes its id; everything else stays as it was.
    assert old["recent_ledger"] == ["e-1", "e-2", {"id": "e-gone", "text": "Not in this database"},
                                    {"text": "A copy without an id"}, 7]
    assert "mem0_snapshot_hits" not in old and old["mem0_dump"] == {"enabled": False}
    assert plain == json.dumps({"domain": "global", "title": "No ledger"})
    ledger = fetch_latest_snapshot(config, domain="global")["recent_ledger"]
    assert [entry.get("id") for entry in ledger] == ["e-1", "e-2", "e-gone", None]
    assert ledger[0]["text"] == "Event e-1" and ledger[0]["visibility"] == "default"


def _checkpoint(config) -> tuple:
    with sqlite3.connect(config.db_path) as connection:
        return tuple(connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone())


def test_a_new_database_checkpoints_and_reclaims_space_incrementally(loaded_manifest, tmp_path) -> None:
    """SQLite 3.53 could not checkpoint a new INCREMENTAL file that created and dropped tables in one go."""
    config = config_from_manifest(loaded_manifest)

    prepared = prepare_database(config, allow_upgrade=True)

    assert prepared.state_before.fresh and prepared.compaction == "vacuum, auto_vacuum=incremental"
    assert _checkpoint(config)[0] == 0
    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 2


def test_an_incremental_database_upgrades_to_0017_and_checkpoints(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(_v16(config, tmp_path), allow_upgrade=True)
    _seed_v16(config)

    prepared = prepare_database(config, allow_upgrade=True)

    assert prepared.compaction == "incremental_vacuum"
    assert _checkpoint(config)[0] == 0
    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA freelist_count").fetchone()[0] == 0


def test_0017_refuses_a_sqlite_without_the_json_arrow(loaded_manifest, tmp_path, monkeypatch) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(_v16(config, tmp_path), allow_upgrade=True)
    monkeypatch.setattr(db.sqlite3, "sqlite_version_info", (3, 37, 2))

    with pytest.raises(MigrationError, match="needs SQLite 3.38.0"):
        prepare_database(config, allow_upgrade=True)

    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert "situation_models" in _objects(connection)
    assert list(config.db_path.parent.glob(f"{config.db_path.name}.bak-*")) == []


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_revert_17_brings_back_what_it_dropped_from_the_archive(chronicle_sandbox, loaded_manifest, tmp_path,
                                                                 monkeypatch, capsys) -> None:
    from max_chronicle import cli

    config = config_from_manifest(loaded_manifest)
    v16 = _v16(config, tmp_path)
    prepare_database(v16, allow_upgrade=True)
    _seed_v16(config)
    with connect(config.db_path) as connection:
        tables = sorted(DEAD_TABLES | {"snapshots", "events", "facts", "event_vectors"})
        before = {table: [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                  for table in tables}
    archive = prepare_database(config, allow_upgrade=True).backup_path
    archive_sha = _sha256(archive)
    # Written on schema 17, then the way back: v0.24 must read all of it.
    new = service.record_event(loaded_manifest, {
        "agent": "agent-a", "domain": "global", "project": "atlas", "category": "decision",
        "text": "Atlas launches on Friday"})
    store_snapshot(config, {"id": "s-new", "domain": "global", "title": "After 0017",
                            "recent_ledger": [service.fetch_event(config, event_id=new["id"])]})
    # Stored again under its id since the upgrade: what it holds now is newer than the copy.
    store_snapshot(config, {"id": "s-plain", "captured_at_utc": "2026-10-20T09:00:00Z", "domain": "global",
                            "title": "Stored again", "recent_ledger": []})

    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "migrate", "--revert", "17", "--archive", str(archive)])
    assert cli.main() == 0
    receipt = json.loads(capsys.readouterr().out)

    assert receipt["schema_version"] == 16 and Path(receipt["backup_path"]).is_file()
    assert _sha256(archive) == archive_sha
    fresh = tmp_path / "fresh" / "chronicle.db"
    fresh.parent.mkdir()
    with open_connection(replace(v16, db_path=fresh)) as connection:
        expected = _schema(connection)
    with connect(config.db_path) as connection:
        assert _schema(connection) == expected
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 16
        assert connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] == 16
        assert [row[1] for row in connection.execute("PRAGMA database_list")] == ["main"]
        for table, rows in before.items():
            now = [dict(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]
            if table == "snapshots":
                now, rows = ([row for row in found if row["id"] != "s-plain"] for found in (now, rows))
            assert now[:len(rows)] == rows, table
        new_ledger = _stored_payload(connection, "s-new")["recent_ledger"]
        restored_again = _stored_payload(connection, "s-plain")
        indexed = connection.execute(
            "SELECT snapshot_id FROM snapshots_fts WHERE snapshots_fts MATCH 'title:After' ").fetchall()
        assert connection.execute("SELECT COUNT(*) FROM snapshots_fts").fetchone()[0] == 3
        connection.execute("INSERT INTO snapshots_fts(snapshots_fts) VALUES ('integrity-check')")
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    # A snapshot captured on 17 keeps its ids, which v0.24 reads.
    assert new_ledger == [new["id"]] and [tuple(row) for row in indexed] == [("s-new",)]
    assert restored_again["title"] == "Stored again"
    # And up again.
    assert [item.version for item in prepare_database(config, allow_upgrade=True).applied] == [17]


def test_revert_17_refuses_without_the_right_archive(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(config, allow_upgrade=True)
    stranger = tmp_path / "other.db"
    sqlite3.connect(stranger).execute("CREATE TABLE notes(text TEXT)").connection.close()
    current = tmp_path / "current.db"
    with connect(config.db_path) as source, sqlite3.connect(current) as target:
        source.backup(target)

    for archive, message in ((None, "pass --archive"), (tmp_path / "missing.db", "no archive"),
                             (stranger, "not a Chronicle database"), (current, "already has migration 0017")):
        with connect(config.db_path) as connection, pytest.raises(MigrationError, match=message):
            revert_migration(connection, config, 17, archive=archive)

    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
        assert [row[1] for row in connection.execute("PRAGMA database_list")] == ["main"]
    assert not (tmp_path / "missing.db").exists()


def test_revert_17_refuses_the_copy_of_another_database(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    prepare_database(_v16(config, tmp_path), allow_upgrade=True)
    _seed_v16(config)
    prepare_database(config, allow_upgrade=True)
    empty = replace(_v16(config, tmp_path), db_path=tmp_path / "empty" / "chronicle.db")
    other = replace(empty, db_path=tmp_path / "other" / "chronicle.db")
    for copy in (empty, other):
        copy.db_path.parent.mkdir()
        prepare_database(copy, allow_upgrade=True)
    with connect(other.db_path) as connection:
        _insert_event(connection, "e-elsewhere")

    for archive, message in ((empty.db_path, "events this database recorded before the migration"),
                             (other.db_path, "events in the copy that this database lacks")):
        with connect(config.db_path) as connection, pytest.raises(MigrationError, match=message):
            revert_migration(connection, config, 17, archive=archive)

    with connect(config.db_path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 17
        assert [row[1] for row in connection.execute("PRAGMA database_list")] == ["main"]


def test_a_ledger_entry_with_an_odd_id_is_kept_as_it_is(loaded_manifest) -> None:
    from max_chronicle.store import timeline_state
    from datetime import datetime, timezone

    config = config_from_manifest(loaded_manifest)
    event = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Known"})
    odd = [{"id": ["not", "an", "id"]}, {"id": {"x": 1}}, "no-such-event", 7, None]
    store_snapshot(config, {"id": "s-odd", "domain": "global", "captured_at_utc": "2026-10-20T10:00:00Z",
                            "recent_ledger": [event["id"], *odd]})
    target = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)

    default = timeline_state(config, target=target, detail="full")["nearest_snapshots"][0]["recent_ledger"]
    raw = timeline_state(config, target=target, detail="full", visibility="raw")["nearest_snapshots"][0]["recent_ledger"]

    assert [entry.get("id") for entry in default] == [event["id"], ["not", "an", "id"], {"x": 1}]
    assert raw[0]["text"] == "Known" and raw[1:] == odd


def test_embed_prune_works_on_the_database_given_with_db(chronicle_sandbox, loaded_manifest, tmp_path,
                                                        monkeypatch, capsys) -> None:
    from max_chronicle import cli

    config = config_from_manifest(loaded_manifest)
    event = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Embedded"})
    with open_connection(config) as connection, connection:
        _insert_vector(connection, event["id"], "nomic-embed-text")
    copy = tmp_path / "copy.db"
    with connect(config.db_path) as source, sqlite3.connect(copy) as target:
        source.backup(target)

    monkeypatch.setattr(sys, "argv", ["chronicle", "--manifest", str(chronicle_sandbox.manifest_path),
                                      "--db", str(copy), "embed-prune", "--model", "nomic-embed-text", "--apply"])
    assert cli.main() == 0
    capsys.readouterr()

    with connect(copy) as connection:
        assert connection.execute("SELECT COUNT(*) FROM event_vectors").fetchone()[0] == 0
    with connect(config.db_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM event_vectors").fetchone()[0] == 1


def test_a_snapshot_stores_event_ids_and_reads_the_events_back(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    first = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "First"})
    copy = service.fetch_event(config, event_id=first["id"])
    ledger = [copy, {"id": "gone", "text": "Not here"}, {"text": "No id"}]

    returned = store_snapshot(config, {"id": "s-1", "domain": "global", "recent_ledger": ledger})

    with open_connection(config) as connection:
        stored = _stored_payload(connection, "s-1")
    read = fetch_latest_snapshot(config, domain="global")["recent_ledger"]
    assert returned["recent_ledger"] == ledger  # the caller keeps what it passed
    assert stored["recent_ledger"] == [first["id"], {"id": "gone", "text": "Not here"}, {"text": "No id"}]
    assert [entry.get("text") for entry in read] == ["First", "Not here", "No id"]


def test_a_capture_stores_the_ids_of_its_recent_events(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    event = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Captured by id"})

    snapshot = service.capture_runtime_snapshot(loaded_manifest, append_compat=False, render_generated=False)

    with open_connection(config) as connection:
        stored = _stored_payload(connection, snapshot["id"])
    assert stored["recent_ledger"] == [event["id"]]
    assert snapshot["recent_ledger"][0]["text"] == "Captured by id"


def test_legacy_import_stores_snapshot_ledgers_as_ids(loaded_manifest) -> None:
    from max_chronicle.bootstrap import bootstrap_legacy

    config = config_from_manifest(loaded_manifest)
    config.ledger_path.write_text(json.dumps({"id": "legacy-1", "text": "Imported decision", "domain": "global",
                                              "category": "decision"}) + "\n", encoding="utf-8")
    config.snapshot_path.write_text(json.dumps({
        "id": "legacy-snap", "captured_at_utc": "2026-03-01T10:00:00Z", "domain": "global",
        "recent_ledger": [{"id": "legacy-1", "text": "Imported decision"}, {"text": "No id"}]}) + "\n",
        encoding="utf-8")

    with open_connection(config) as connection, connection:
        result = bootstrap_legacy(connection, config)
        stored = _stored_payload(connection, "legacy-snap")

    assert result["snapshots"]["imported"] == 1
    assert stored["recent_ledger"] == ["legacy-1", {"text": "No id"}]


def _insert_vector(connection, event_id: str, model_key: str) -> None:
    connection.execute(
        "INSERT INTO event_vectors(event_id, model_key, dim, vector, created_at_utc) "
        "VALUES (?, ?, 2, ?, '2026-10-27T00:00:00Z')", (event_id, model_key, bytes(8)))


def test_embed_prune_counts_then_deletes_another_models_vectors(loaded_manifest) -> None:
    from max_chronicle.embeddings import active_profile

    config = config_from_manifest(loaded_manifest)
    event = service.record_event(loaded_manifest, {"agent": "a", "domain": "global", "text": "Embedded twice"})
    with open_connection(config) as connection, connection:
        _insert_vector(connection, event["id"], "nomic-embed-text")
        _insert_vector(connection, event["id"], active_profile().key)

    counted = service.embed_prune(loaded_manifest, model_key="nomic-embed-text")
    with open_connection(config) as connection:
        after_count = connection.execute("SELECT COUNT(*) FROM event_vectors").fetchone()[0]
    pruned = service.embed_prune(loaded_manifest, model_key="nomic-embed-text", apply=True)
    with open_connection(config) as connection:
        left = [row[0] for row in connection.execute("SELECT model_key FROM event_vectors")]

    assert counted["vectors"] == {"event_vectors": 1, "chunk_vectors": 0} and not counted["deleted"]
    assert after_count == 2
    assert pruned["deleted"] and left == [active_profile().key]
    with pytest.raises(ValueError, match="active embedding model"):
        service.embed_prune(loaded_manifest, model_key=active_profile().key, apply=True)
