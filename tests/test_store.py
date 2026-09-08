from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import threading

import pytest

import sqlite3

from max_chronicle.store import (
    _atomic_write_text,
    config_from_manifest,
    open_connection,
    start_automation_run,
    start_curation_run,
    store_artifact_from_path,
    store_artifact_text,
)
from max_chronicle.service import record_event


def _plan_details(connection, sql: str, params: tuple[object, ...] = ()) -> str:
    rows = connection.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    return " | ".join(str(row[3]) for row in rows)


def _utc_stamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_atomic_write_text_preserves_existing_file_when_fsync_fails(tmp_path, monkeypatch) -> None:
    target = tmp_path / "durable.md"
    target.write_text("prior\n", encoding="utf-8")

    def fail_fsync(fd: int) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("max_chronicle.store.os.fsync", fail_fsync)

    with pytest.raises(OSError, match="disk full"):
        _atomic_write_text(target, "partial\n")

    assert target.read_text(encoding="utf-8") == "prior\n"
    assert list(tmp_path.glob(".durable.md.*.tmp")) == []


def test_store_artifact_from_path_uses_pointer_only_for_disallowed_extension(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    source_path = chronicle_sandbox.status_root / "notes.bin"
    source_path.write_bytes(b"chronicle")

    artifact = store_artifact_from_path(
        config,
        source_path=source_path,
        artifact_type="event-source",
    )

    assert artifact is not None
    assert artifact["storage_path"].startswith("pointer://")

    with open_connection(config) as connection:
        row = connection.execute("SELECT storage_path, metadata_json FROM artifacts WHERE id = ?", (artifact["id"],)).fetchone()
    metadata = json.loads(row["metadata_json"])
    assert row["storage_path"].startswith("pointer://")
    assert metadata["storage_mode"] == "pointer_only"
    assert metadata["pointer_reason"] == "extension_not_allowed"


def test_store_artifact_from_path_uses_pointer_only_for_symlinks(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    target_path = chronicle_sandbox.status_root / "allowed.json"
    target_path.write_text('{"ok":true}\n', encoding="utf-8")
    link_path = chronicle_sandbox.status_root / "linked.json"
    os.symlink(target_path, link_path)

    artifact = store_artifact_from_path(
        config,
        source_path=link_path,
        artifact_type="event-source",
    )

    assert artifact is not None
    assert artifact["storage_path"].startswith("pointer://")

    with open_connection(config) as connection:
        row = connection.execute("SELECT metadata_json FROM artifacts WHERE id = ?", (artifact["id"],)).fetchone()
    metadata = json.loads(row["metadata_json"])
    assert metadata["pointer_reason"] == "symlink_rejected"


def test_store_artifact_text_rejects_generated_payload_above_max_size(loaded_manifest) -> None:
    config = replace(config_from_manifest(loaded_manifest), artifact_max_copy_bytes=8)
    with pytest.raises(ValueError, match="exceeds max copy size"):
        store_artifact_text(
            config,
            artifact_type="backup-manifest",
            content="x" * 32,
            filename="oversized.json",
        )


def test_store_artifact_from_path_is_race_safe_under_concurrent_writes(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    source_path = chronicle_sandbox.status_root / "race.json"
    source_path.write_text('{"race":"artifact"}\n', encoding="utf-8")
    with open_connection(config):
        pass
    barrier = threading.Barrier(4)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait()
            artifact = store_artifact_from_path(
                config,
                source_path=source_path,
                artifact_type="event-source",
            )
            assert artifact is not None
            results.append(artifact)
        except BaseException as exc:  # pragma: no cover - test helper
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(results) == 4
    assert len({item["id"] for item in results}) == 1

    with open_connection(config) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM artifacts WHERE artifact_type = 'event-source' AND source_path = ?",
            (str(source_path),),
        ).fetchone()[0]
    assert count == 1


def test_store_artifact_from_path_recopies_corrupt_existing_destination(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    source_path = chronicle_sandbox.status_root / "source.json"
    payload = b'{"safe":"copy"}\n'
    source_path.write_bytes(payload)
    expected_sha = hashlib.sha256(payload).hexdigest()
    storage_path = (
        config.artifact_dir
        / "event-source"
        / expected_sha[:2]
        / expected_sha[2:4]
        / f"{expected_sha}-{source_path.name}"
    )
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_bytes(b"corrupt partial bytes\n")

    artifact = store_artifact_from_path(
        config,
        source_path=source_path,
        artifact_type="event-source",
    )

    assert artifact is not None
    assert artifact["sha256"] == expected_sha
    assert storage_path.read_bytes() == payload
    assert hashlib.sha256(storage_path.read_bytes()).hexdigest() == expected_sha

    with open_connection(config) as connection:
        row = connection.execute("SELECT sha256, storage_path FROM artifacts WHERE id = ?", (artifact["id"],)).fetchone()
    assert row["sha256"] == expected_sha
    assert row["storage_path"] == str(storage_path)


def test_start_run_recovers_stale_running_rows_but_blocks_fresh_rows(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    now = datetime.now(timezone.utc)
    stale_started = _utc_stamp(now - timedelta(hours=7))
    fresh_started = _utc_stamp(now - timedelta(hours=1))

    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, status, details_json)
            VALUES ('stale-auto', 'mem0-dump', 'stale-key', 'pytest', ?, 'running', ?)
            """,
            (stale_started, json.dumps({"before": "auto"})),
        )
        connection.execute(
            """
            INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, status)
            VALUES ('fresh-auto', 'mem0-dump', 'fresh-key', 'pytest', ?, 'running')
            """,
            (fresh_started,),
        )
        connection.execute(
            """
            INSERT INTO curation_runs(id, curation_type, run_key, started_at_utc, status, payload_json)
            VALUES ('stale-curation', 'daybook', 'stale-day', ?, 'running', ?)
            """,
            (stale_started, json.dumps({"before": "curation"})),
        )
        connection.execute(
            """
            INSERT INTO curation_runs(id, curation_type, run_key, started_at_utc, status)
            VALUES ('fresh-curation', 'daybook', 'fresh-day', ?, 'running')
            """,
            (fresh_started,),
        )

    new_auto_id, auto_created = start_automation_run(
        config,
        job_name="mem0-dump",
        run_key="stale-key",
        trigger_source="pytest",
        stale_ttl_hours=6,
    )
    blocked_auto_id, blocked_auto_created = start_automation_run(
        config,
        job_name="mem0-dump",
        run_key="fresh-key",
        trigger_source="pytest",
        stale_ttl_hours=6,
    )
    new_curation_id, curation_created = start_curation_run(
        config,
        curation_type="daybook",
        run_key="stale-day",
        payload={"trigger_source": "pytest"},
        stale_ttl_hours=6,
    )
    blocked_curation_id, blocked_curation_created = start_curation_run(
        config,
        curation_type="daybook",
        run_key="fresh-day",
        stale_ttl_hours=6,
    )

    assert auto_created is True
    assert new_auto_id != "stale-auto"
    assert blocked_auto_created is False
    assert blocked_auto_id == "fresh-auto"
    assert curation_created is True
    assert new_curation_id != "stale-curation"
    assert blocked_curation_created is False
    assert blocked_curation_id == "fresh-curation"

    with open_connection(config) as connection:
        stale_auto = connection.execute(
            "SELECT status, run_key, details_json FROM automation_runs WHERE id = 'stale-auto'"
        ).fetchone()
        new_auto = connection.execute(
            "SELECT status, run_key FROM automation_runs WHERE id = ?",
            (new_auto_id,),
        ).fetchone()
        fresh_auto = connection.execute(
            "SELECT status FROM automation_runs WHERE id = 'fresh-auto'"
        ).fetchone()
        stale_curation = connection.execute(
            "SELECT status, run_key, payload_json, notes FROM curation_runs WHERE id = 'stale-curation'"
        ).fetchone()
        new_curation = connection.execute(
            "SELECT status, run_key FROM curation_runs WHERE id = ?",
            (new_curation_id,),
        ).fetchone()
        fresh_curation = connection.execute(
            "SELECT status FROM curation_runs WHERE id = 'fresh-curation'"
        ).fetchone()

    assert stale_auto["status"] == "stale_failed"
    assert stale_auto["run_key"].startswith("stale-key:stale:")
    auto_details = json.loads(stale_auto["details_json"])
    assert auto_details["stale_run_recovery"]["reason"] == "stale running row exceeded TTL"
    assert new_auto["status"] == "running"
    assert new_auto["run_key"] == "stale-key"
    assert fresh_auto["status"] == "running"

    assert stale_curation["status"] == "stale_failed"
    assert stale_curation["run_key"].startswith("stale-day:stale:")
    assert stale_curation["notes"] == "stale running row exceeded TTL"
    curation_payload = json.loads(stale_curation["payload_json"])
    assert curation_payload["stale_run_recovery"]["reason"] == "stale running row exceeded TTL"
    assert new_curation["status"] == "running"
    assert new_curation["run_key"] == "stale-day"
    assert fresh_curation["status"] == "running"


def test_record_event_dedupe_is_race_safe_under_concurrency(loaded_manifest) -> None:
    entry = {
        "agent": "pytest",
        "domain": "global",
        "category": "decision",
        "project": "status",
        "text": "Concurrent dedupe fact",
        "why": "Concurrent identical writes should collapse to one durable event.",
        "source_files": [],
        "mem0_status": "off",
        "mem0_error": None,
        "mem0_raw": None,
        "recorded_at": "2026-03-29T12:00:00Z",
    }
    config = config_from_manifest(loaded_manifest)
    with open_connection(config):
        pass
    barrier = threading.Barrier(4)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait()
            results.append(
                record_event(
                    loaded_manifest,
                    entry,
                    append_compat=False,
                    dedupe=True,
                    source_kind="pytest",
                    imported_from="tests.test_store.concurrent_dedupe",
                )
            )
        except BaseException as exc:  # pragma: no cover - test helper
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(results) == 4
    assert len({item["id"] for item in results}) == 1
    assert {item["chronicle_status"] for item in results} == {"stored", "existing"}

    with open_connection(config) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM events WHERE text = ?",
            ("Concurrent dedupe fact",),
        ).fetchone()[0]
    assert count == 1


def test_query_plan_smoke_uses_stability_indexes(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        native_domain = _plan_details(
            connection,
            "SELECT id FROM events WHERE circumstances = ? ORDER BY occurred_at_utc DESC LIMIT 5",
            ("global",),
        )
        legacy_domain = _plan_details(
            connection,
            "SELECT id FROM events WHERE circumstances IS NULL AND json_extract(payload_json, '$.domain') = ? ORDER BY occurred_at_utc DESC LIMIT 5",
            ("global",),
        )
        title_project = _plan_details(
            connection,
            "SELECT id FROM events WHERE title = ? ORDER BY occurred_at_utc DESC LIMIT 5",
            ("status",),
        )
        legacy_project = _plan_details(
            connection,
            "SELECT id FROM events WHERE entity_id IS NULL AND title IS NULL AND json_extract(payload_json, '$.project') = ? ORDER BY occurred_at_utc DESC LIMIT 5",
            ("status",),
        )

    assert "idx_events_domain_recent" in native_domain
    assert "idx_events_payload_domain_recent" in legacy_domain
    assert "idx_events_title_recent" in title_project
    assert "idx_events_payload_project_recent" in legacy_project


def test_open_connection_closes_on_exit(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        captured = connection
        assert connection.execute("SELECT 1").fetchone()[0] == 1
    with pytest.raises(sqlite3.ProgrammingError):
        captured.execute("SELECT 1")


def test_open_connection_closes_on_exception(loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    captured: sqlite3.Connection | None = None
    with pytest.raises(RuntimeError):
        with open_connection(config) as connection:
            captured = connection
            raise RuntimeError("boom")
    assert captured is not None
    with pytest.raises(sqlite3.ProgrammingError):
        captured.execute("SELECT 1")


# ===== Phase-4 write-path hardening =====


def test_write_transaction_commits_and_rolls_back(loaded_manifest) -> None:
    from max_chronicle.store import write_transaction

    config = config_from_manifest(loaded_manifest)
    with write_transaction(config) as connection:
        connection.execute(
            "INSERT INTO entities(id, entity_type, slug, name, created_at_utc, updated_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
            ("test:committed", "project", "committed", "committed", "2026-08-13T00:00:00Z", "2026-08-13T00:00:00Z"),
        )

    with pytest.raises(RuntimeError):
        with write_transaction(config) as connection:
            connection.execute(
                "INSERT INTO entities(id, entity_type, slug, name, created_at_utc, updated_at_utc) VALUES (?, ?, ?, ?, ?, ?)",
                ("test:rolled-back", "project", "rolled-back", "rolled-back", "2026-08-13T00:00:00Z", "2026-08-13T00:00:00Z"),
            )
            raise RuntimeError("boom")

    with open_connection(config) as connection:
        ids = {
            row["id"]
            for row in connection.execute("SELECT id FROM entities WHERE id LIKE 'test:%'").fetchall()
        }
    assert "test:committed" in ids
    assert "test:rolled-back" not in ids


def test_record_event_is_atomic_across_event_artifact_and_embedding(loaded_manifest, tmp_path, monkeypatch) -> None:
    """A failure while linking artifacts must not leave a half-recorded event."""
    import max_chronicle.service as service_module

    config = config_from_manifest(loaded_manifest)
    source_file = tmp_path / "evidence.txt"
    source_file.write_text("evidence body", encoding="utf-8")

    def exploding_link(*args, **kwargs):
        raise RuntimeError("link failed mid-write")

    monkeypatch.setattr(service_module, "link_artifact", exploding_link)

    with pytest.raises(RuntimeError, match="link failed mid-write"):
        record_event(
            loaded_manifest,
            {
                "agent": "pytest",
                "domain": "global",
                "category": "decision",
                "project": "status",
                "text": "Atomicity probe event",
                "why": "Artifact linking blows up after the event insert.",
                "source_files": [str(source_file)],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
                "_embed_fn": lambda _text: [0.5] * 768,
            },
        )

    with open_connection(config) as connection:
        events = connection.execute(
            "SELECT COUNT(*) FROM events WHERE text = ?", ("Atomicity probe event",)
        ).fetchone()[0]
        artifacts = connection.execute(
            "SELECT COUNT(*) FROM artifacts WHERE source_path = ?", (str(source_file),)
        ).fetchone()[0]
        embeddings = connection.execute("SELECT COUNT(*) FROM event_embeddings").fetchone()[0]

    assert events == 0, "event survived a failed artifact link — transaction was not atomic"
    assert artifacts == 0
    assert embeddings == 0


def test_record_event_commits_event_artifact_and_embedding_together(loaded_manifest, tmp_path) -> None:
    config = config_from_manifest(loaded_manifest)
    source_file = tmp_path / "evidence-ok.txt"
    source_file.write_text("evidence body", encoding="utf-8")

    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Atomic happy path",
            "why": "Event, artifact link and embedding land in one transaction.",
            "source_files": [str(source_file)],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
            "_embed_fn": lambda _text: [0.25] * 768,
        },
    )

    assert stored["chronicle_status"] == "stored"
    assert stored["artifacts_written"] == 1
    with open_connection(config) as connection:
        links = connection.execute(
            "SELECT COUNT(*) FROM artifact_links WHERE target_id = ?", (stored["id"],)
        ).fetchone()[0]
        embeddings = connection.execute(
            "SELECT COUNT(*) FROM event_embeddings WHERE event_id = ?", (stored["id"],)
        ).fetchone()[0]
    assert links == 1
    assert embeddings == 1


def test_load_manifest_cached_reparses_only_after_change(chronicle_sandbox, monkeypatch) -> None:
    import max_chronicle.runtime_context as runtime_context

    runtime_context._manifest_cache.clear()
    calls = {"n": 0}
    original_read_toml = runtime_context.read_toml

    def counting_read_toml(path):
        calls["n"] += 1
        return original_read_toml(path)

    monkeypatch.setattr(runtime_context, "read_toml", counting_read_toml)

    first = runtime_context.load_manifest_cached(chronicle_sandbox.manifest_path)
    second = runtime_context.load_manifest_cached(chronicle_sandbox.manifest_path)
    assert calls["n"] == 1
    assert first["paths"] == second["paths"]
    # Callers get their own top-level dict — the copy-then-assign idiom in cli.py
    # must not leak into the cached body.
    second["paths"] = {"tampered": True}
    assert runtime_context.load_manifest_cached(chronicle_sandbox.manifest_path)["paths"] != {"tampered": True}

    manifest_text = chronicle_sandbox.manifest_path.read_text(encoding="utf-8")
    chronicle_sandbox.manifest_path.write_text(manifest_text + "\n# touched\n", encoding="utf-8")
    runtime_context.load_manifest_cached(chronicle_sandbox.manifest_path)
    assert calls["n"] == 2
    runtime_context._manifest_cache.clear()
