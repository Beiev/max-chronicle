from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import threading

import pytest

from max_chronicle.store import (
    config_from_manifest,
    open_connection,
    store_artifact_from_path,
    store_artifact_text,
)
from max_chronicle.service import record_event


def _plan_details(connection, sql: str, params: tuple[object, ...] = ()) -> str:
    rows = connection.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
    return " | ".join(str(row[3]) for row in rows)


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
