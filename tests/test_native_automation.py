from __future__ import annotations

import argparse
from dataclasses import replace
import importlib.util
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any

import pytest
from zoneinfo import ZoneInfo

import max_chronicle.native_automation as native_automation_module
from max_chronicle.config import MIGRATIONS_DIR
from max_chronicle.db import database_summary
from max_chronicle.native_automation import (
    doctor_launchd,
    launchd_labels,
    install_git_hooks,
    install_launchd,
    run_audit,
    run_automation_job,
    run_backup,
    run_daybook,
    run_git_commit_hook,
    sync_mem0_outbox,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.service import capture_runtime_snapshot, record_event
from max_chronicle.store import config_from_manifest, open_connection


PROJECT_STATUS_ROOT = Path(__file__).resolve().parents[1]
MEM0_BRIDGE = PROJECT_STATUS_ROOT / "scripts" / "mem0_bridge.py"


def _cli(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_STATUS_ROOT)
    return subprocess.run(
        [sys.executable, "-m", "max_chronicle.cli", *args],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )


# The Mem0 bridge is operator tooling that ships outside the package, so a
# clean checkout (and CI) will not have it. Those tests skip instead of failing.
# doctor_launchd inspects real launchd state through macOS-only tooling; the
# code degrades gracefully elsewhere, but these assertions only mean something
# on darwin.
requires_darwin = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="launchd inspection is macOS-only",
)

requires_mem0_bridge = pytest.mark.skipif(
    not MEM0_BRIDGE.exists(),
    reason="scripts/mem0_bridge.py is operator tooling, not part of the package",
)


def _load_mem0_bridge_module():
    spec = importlib.util.spec_from_file_location("mem0_bridge_test_module", MEM0_BRIDGE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _local_date(timezone_name: str) -> str:
    return datetime.now(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")


def _utc_stamp(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_migrate_is_idempotent(chronicle_sandbox) -> None:
    first = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "migrate",
    )
    second = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "migrate",
    )
    first_payload = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    latest = max(int(path.name[:4]) for path in MIGRATIONS_DIR.glob("*.sql"))
    assert len(first_payload["applied"]) == latest
    assert second_payload["applied"] == []
    assert first_payload["summary"]["user_version"] == latest
    assert second_payload["summary"]["user_version"] == latest


def test_repair_stale_runs_cli_reports_and_marks_both_run_tables(chronicle_sandbox, loaded_manifest) -> None:
    config = config_from_manifest(loaded_manifest)
    now = datetime.now(timezone.utc)
    stale_started = _utc_stamp(now - timedelta(hours=8))
    fresh_started = _utc_stamp(now - timedelta(hours=1))

    with open_connection(config) as connection, connection:
        connection.execute(
            """
            INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, status, details_json)
            VALUES ('repair-auto-stale', 'backup', '2026-04-22', 'pytest', ?, 'running', ?)
            """,
            (stale_started, json.dumps({"before": "automation"})),
        )
        connection.execute(
            """
            INSERT INTO automation_runs(id, job_name, run_key, trigger_source, started_at_utc, status)
            VALUES ('repair-auto-fresh', 'backup', 'fresh', 'pytest', ?, 'running')
            """,
            (fresh_started,),
        )
        connection.execute(
            """
            INSERT INTO curation_runs(id, curation_type, run_key, started_at_utc, status, payload_json)
            VALUES ('repair-curation-stale', 'daybook', '2026-04-22', ?, 'running', ?)
            """,
            (stale_started, json.dumps({"before": "curation"})),
        )
        connection.execute(
            """
            INSERT INTO curation_runs(id, curation_type, run_key, started_at_utc, status)
            VALUES ('repair-curation-fresh', 'daybook', 'fresh', ?, 'running')
            """,
            (fresh_started,),
        )

    dry_run = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "repair-stale-runs",
        "--dry-run",
        "--ttl-hours",
        "6",
    )
    dry_payload = json.loads(dry_run.stdout)

    assert dry_payload["status"] == "dry_run"
    assert dry_payload["stale_count"] == 2
    assert dry_payload["updated_count"] == 0
    assert dry_payload["automation_runs"]["stale_count"] == 1
    assert dry_payload["curation_runs"]["stale_count"] == 1

    with open_connection(config) as connection:
        dry_statuses = {
            row["id"]: row["status"]
            for row in connection.execute(
                """
                SELECT id, status FROM automation_runs
                WHERE id IN ('repair-auto-stale', 'repair-auto-fresh')
                UNION ALL
                SELECT id, status FROM curation_runs
                WHERE id IN ('repair-curation-stale', 'repair-curation-fresh')
                """
            ).fetchall()
        }
    assert set(dry_statuses.values()) == {"running"}

    apply_run = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "repair-stale-runs",
        "--ttl-hours",
        "6",
    )
    apply_payload = json.loads(apply_run.stdout)

    assert apply_payload["status"] == "ok"
    assert apply_payload["stale_count"] == 2
    assert apply_payload["updated_count"] == 2

    with open_connection(config) as connection:
        stale_auto = connection.execute(
            "SELECT status, details_json FROM automation_runs WHERE id = 'repair-auto-stale'"
        ).fetchone()
        fresh_auto = connection.execute(
            "SELECT status FROM automation_runs WHERE id = 'repair-auto-fresh'"
        ).fetchone()
        stale_curation = connection.execute(
            "SELECT status, payload_json, notes FROM curation_runs WHERE id = 'repair-curation-stale'"
        ).fetchone()
        fresh_curation = connection.execute(
            "SELECT status FROM curation_runs WHERE id = 'repair-curation-fresh'"
        ).fetchone()

    assert stale_auto["status"] == "stale_failed"
    assert fresh_auto["status"] == "running"
    auto_details = json.loads(stale_auto["details_json"])
    assert auto_details["stale_run_recovery"]["reason"] == "repaired stale running row"
    assert stale_curation["status"] == "stale_failed"
    assert stale_curation["notes"] == "repaired stale running row"
    curation_payload = json.loads(stale_curation["payload_json"])
    assert curation_payload["stale_run_recovery"]["reason"] == "repaired stale running row"
    assert fresh_curation["status"] == "running"


def test_git_commit_hook_dedupes_by_commit(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    repo = chronicle_sandbox.portfolio_repo
    new_file = repo / "src" / "new.ts"
    new_file.parent.mkdir(parents=True, exist_ok=True)
    new_file.write_text("export const value = 1;\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "add tracked value"], check=True, capture_output=True, text=True)
    commit_sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    first = run_git_commit_hook(
        loaded_manifest,
        loaded_automation,
        repo_slug="demo-portfolio",
        commit_sha=commit_sha,
        repo_root=repo,
        trigger_source="pytest",
    )
    second = run_git_commit_hook(
        loaded_manifest,
        loaded_automation,
        repo_slug="demo-portfolio",
        commit_sha=commit_sha,
        repo_root=repo,
        trigger_source="pytest",
    )
    assert first["status"] == "stored"
    assert second["status"] == "existing"

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        refs = connection.execute("SELECT COUNT(*) FROM event_external_refs WHERE ref_type = 'git_commit'").fetchone()[0]
        relations = connection.execute("SELECT COUNT(*) FROM relations WHERE relation_type = 'touches_repo'").fetchone()[0]
    assert refs == 1
    assert relations >= 1


def test_daybook_is_deterministic_ok_by_default(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    # The daybook always uses the deterministic skeleton — there is no LLM path any more
    # (MiniMax was cut 2026-05-19). The run must succeed with status "ok".
    # Seed at least one event so the daybook has a durable delta to render.
    record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "note",
            "project": "status",
            "text": "Daybook determinism seed event.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=False,
        source_kind="pytest",
    )
    local_date = _local_date(loaded_manifest["settings"]["timezone"])
    result = run_daybook(loaded_manifest, loaded_automation, target_date=local_date, trigger_source="pytest")
    assert result["status"] == "ok"
    daybook_path = chronicle_sandbox.status_root / "daybooks" / local_date[:4] / f"{local_date}.md"
    assert daybook_path.exists()
    assert "## Key Events" in daybook_path.read_text(encoding="utf-8")


def test_capture_runtime_renders_projections(loaded_manifest) -> None:
    snapshot = capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Projection test",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    status_path = Path(loaded_manifest["paths"]["status_root"]) / "status.md"
    job_path = Path(loaded_manifest["paths"]["status_root"]) / "job-search-status.md"
    assert snapshot["id"]
    assert "Chronicle Generated View" in status_path.read_text(encoding="utf-8")
    assert "Generated by Chronicle" in job_path.read_text(encoding="utf-8")


def test_backup_skips_when_target_is_unmounted(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest")
    assert result["status"] == "skipped"
    assert result["reason"] == "target_unmounted"
    assert result["backup_policy"] == "opportunistic_external"


def test_backup_skips_when_volume_root_is_not_mounted(monkeypatch, loaded_manifest, loaded_automation) -> None:
    automation = replace(loaded_automation, backup_root=Path("/Volumes/BackupDrive/Chronicle-Backups"))
    checked: list[Path] = []

    def fake_is_mount(path: Path) -> bool:
        checked.append(path)
        return False

    monkeypatch.setattr(Path, "is_mount", fake_is_mount)

    result = run_backup(loaded_manifest, automation, trigger_source="pytest", force=True)

    assert result["status"] == "skipped"
    assert result["reason"] == "backup_target_not_mounted"
    assert result["volume_root"] == "/Volumes/BackupDrive"
    assert checked == [Path("/Volumes/BackupDrive")]


def test_backup_skips_when_target_free_space_is_insufficient(
    monkeypatch,
    chronicle_sandbox,
    loaded_manifest,
    loaded_automation,
) -> None:
    config = config_from_manifest(loaded_manifest)
    with open_connection(config):
        pass
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)

    class TinyStatvfs:
        f_bavail = 1
        f_frsize = 1

    monkeypatch.setattr(native_automation_module.os, "statvfs", lambda path: TinyStatvfs())

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    assert result["status"] == "skipped"
    assert result["reason"] == "insufficient_space"
    assert result["available_bytes"] == 1
    assert result["required_bytes"] > result["db_size_bytes"]
    assert list(chronicle_sandbox.backup_root.iterdir()) == []


def test_backup_copies_db_and_restores(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Backup seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    assert result["status"] == "ok"
    assert Path(result["manifest_path"]).exists()
    assert result["restore_check"]["ok"] is True
    assert result["restore_check"]["quick_check"] == "ok"
    assert result["restore_check"]["backup_runs_running"] == 0
    assert result["restore_check"]["backup_manifest_required"] is False
    assert result["restore_check"]["artifact_tree_complete"] is True
    assert result["backup_policy"] == "opportunistic_external"
    assert result["maintenance"]["reason"] in {"nothing_to_prune", "pruned_retained_rows"}
    assert result["maintenance"]["wal_checkpoint"]["reason"] == "post_backup"

    backup_db = Path(result["backup_path"]) / "chronicle.db"
    manifest_payload = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest_payload["restore_check"]["ok"] is True
    assert manifest_payload["db_sha256"] == hashlib.sha256(backup_db.read_bytes()).hexdigest()
    with sqlite3.connect(backup_db) as connection:
        connection.row_factory = sqlite3.Row
        run_row = connection.execute(
            "SELECT status, manifest_path, restore_ok FROM backup_runs WHERE id = ?",
            (result["run_id"],),
        ).fetchone()
    assert run_row["status"] == "ok"
    assert run_row["manifest_path"] == result["manifest_path"]
    assert run_row["restore_ok"] == 1


def test_backup_manifest_hash_matches_post_maintenance_db_hash(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Backup manifest hash seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    backup_db = Path(result["backup_path"]) / "chronicle.db"
    manifest_payload = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert result["status"] == "ok"
    assert manifest_payload["db_sha256"] == hashlib.sha256(backup_db.read_bytes()).hexdigest()
    assert manifest_payload["restore_check"] == result["restore_check"]


def test_backup_artifacts_are_independent_of_source(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    artifact_source = chronicle_sandbox.status_root / "chronicle-artifacts" / "snapshots" / "shared.json"
    artifact_source.parent.mkdir(parents=True, exist_ok=True)
    artifact_source.write_text('{"hello":"world"}\n', encoding="utf-8")

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    backup_artifact = Path(result["backup_path"]) / "chronicle-artifacts" / "snapshots" / "shared.json"

    assert result["status"] == "ok"
    assert backup_artifact.exists()
    assert backup_artifact.stat().st_ino != artifact_source.stat().st_ino
    artifact_source.write_text("source changed", encoding="utf-8")
    assert backup_artifact.read_text() == '{"hello":"world"}\n'
    assert result["artifact_files_copied"] >= 1


def test_restore_verifies_contents_after_relocation(chronicle_sandbox, loaded_manifest, loaded_automation, tmp_path) -> None:
    import shutil
    from max_chronicle.native_automation import _restore_check
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    source = chronicle_sandbox.status_root / "chronicle-artifacts" / "proof.txt"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("original proof")
    result = run_backup(loaded_manifest, loaded_automation, force=True)
    relocated = tmp_path / "relocated"
    shutil.copytree(result["backup_path"], relocated)
    source.unlink()
    assert _restore_check(relocated / "chronicle.db", backup_path=relocated)["ok"]
    (relocated / "chronicle-artifacts" / "proof.txt").write_text("corrupt! proof")
    check = _restore_check(relocated / "chronicle.db", backup_path=relocated)
    assert not check["ok"]
    assert not check["artifact_hashes_ok"]


def test_backup_falls_back_to_copy_when_hardlink_unavailable(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    artifact_source = chronicle_sandbox.status_root / "chronicle-artifacts" / "snapshots" / "copied.json"
    artifact_source.parent.mkdir(parents=True, exist_ok=True)
    artifact_source.write_text('{"fallback":"copy"}\n', encoding="utf-8")

    def fail_link(*args: object, **kwargs: object) -> None:
        raise OSError("cross-device link")

    monkeypatch.setattr(native_automation_module.os, "link", fail_link)

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    backup_artifact = Path(result["backup_path"]) / "chronicle-artifacts" / "snapshots" / "copied.json"

    assert result["status"] == "ok"
    assert backup_artifact.exists()
    assert backup_artifact.read_text(encoding="utf-8") == artifact_source.read_text(encoding="utf-8")
    assert backup_artifact.stat().st_ino != artifact_source.stat().st_ino
    assert result["artifact_files_copied"] >= 1
    assert result["artifact_copy_strategy"]["copied"] >= 1


def test_backup_prunes_stale_synced_mem0_rows_and_records_wal_maintenance(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Prunable synced outbox row",
            "why": None,
            "source_files": [],
            "mem0_status": "queued",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )
    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection, connection:
        connection.execute(
            "UPDATE mem0_outbox SET status = 'synced', synced_at_utc = '2025-01-01T00:00:00Z' WHERE event_id = ?",
            (stored["id"],),
        )

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    assert result["status"] == "ok"
    assert result["maintenance"]["reason"] == "pruned_retained_rows"
    assert result["maintenance"]["pruned"] >= 1
    assert result["maintenance"]["pruned_by_status"]["synced"] >= 1
    assert result["maintenance"]["wal_checkpoint"] is not None
    assert result["maintenance"]["wal_checkpoint"]["reason"] == "post_backup"

    with open_connection(config) as connection:
        remaining = connection.execute(
            "SELECT COUNT(*) FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()[0]
    assert remaining == 0


def test_backup_skips_when_target_is_unwritable(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.parent.mkdir(parents=True, exist_ok=True)
    chronicle_sandbox.backup_root.write_text("not a directory\n", encoding="utf-8")
    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    assert result["status"] == "skipped"
    assert result["reason"] == "target_unwritable"
    assert result["backup_policy"] == "opportunistic_external"


def test_audit_does_not_warn_about_backup_when_target_is_unmounted(loaded_manifest, loaded_automation) -> None:
    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Audit seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    result = run_audit(loaded_manifest, loaded_automation, trigger_source="pytest")
    issue_kinds = {issue["kind"] for issue in result["issues"]}
    assert "backup_missing" not in issue_kinds
    assert "backup_stale" not in issue_kinds


def test_mem0_dump_automation_refreshes_dump_and_archives_artifact(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    bridge_path = chronicle_sandbox.status_root / "scripts" / "mem0_bridge.py"
    bridge_path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "args = sys.argv[1:]\n"
        "if args and args[0] == 'dump':\n"
        "    output = args[args.index('--output') + 1] if '--output' in args else 'mem0-dump.json'\n"
        "    payload = {\n"
        "        'snapshot_type': 'unified',\n"
        "        'total_memories': 2,\n"
        "        'collection_totals': {'personal': 1, 'digest': 1},\n"
        "        'collections': {\n"
        "            'personal': {'collection_name': 'chronicle_personal', 'total_memories': 1},\n"
        "            'digest': {'collection_name': 'chronicle_digest', 'total_memories': 1},\n"
        "        },\n"
        "        'memories': [\n"
        "            {'id': 'personal-memory', 'memory': 'Chronicle sandbox memory', 'metadata': {'project': 'status'}, 'source_collection': 'personal', 'source_collection_name': 'chronicle_personal'},\n"
        "            {'id': 'digest-memory', 'memory': 'Digest sandbox memory', 'metadata': {'category': 'news_digest'}, 'source_collection': 'digest', 'source_collection_name': 'chronicle_digest'},\n"
        "        ],\n"
        "    }\n"
        "    with open(output, 'w', encoding='utf-8') as handle:\n"
        "        json.dump(payload, handle, ensure_ascii=False, indent=2)\n"
        "        handle.write('\\n')\n"
        "    print(json.dumps({'status': 'ok', 'output': output}, ensure_ascii=False))\n"
        "    raise SystemExit(0)\n"
        "raise SystemExit(1)\n",
        encoding="utf-8",
    )

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="mem0-dump", trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["artifact_id"] is not None
    dump_path = chronicle_sandbox.status_root / "mem0-dump.json"
    payload = json.loads(dump_path.read_text(encoding="utf-8"))
    assert payload["snapshot_type"] == "unified"
    assert payload["total_memories"] == 2
    assert payload["collection_totals"] == {"personal": 1, "digest": 1}
    assert payload["collections"]["personal"]["collection_name"] == "chronicle_personal"
    assert payload["collections"]["digest"]["collection_name"] == "chronicle_digest"
    assert {item["source_collection"] for item in payload["memories"]} == {"personal", "digest"}

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        run_row = connection.execute(
            "SELECT status FROM automation_runs WHERE job_name = 'mem0-dump'"
        ).fetchone()
        artifact_row = connection.execute(
            "SELECT artifact_type FROM artifacts WHERE id = ?",
            (result["artifact_id"],),
        ).fetchone()
    assert run_row["status"] == "ok"
    assert artifact_row["artifact_type"] == "mem0-dump"


def test_mem0_dump_automation_retries_failed_run_same_day(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    bridge_path = chronicle_sandbox.status_root / "scripts" / "mem0_bridge.py"
    bridge_path.write_text(
        "#!/usr/bin/env python3\n"
        "raise SystemExit(1)\n",
        encoding="utf-8",
    )

    first = run_automation_job(loaded_manifest, loaded_automation, job_name="mem0-dump", trigger_source="pytest")
    assert first["status"] == "failed"

    bridge_path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "args = sys.argv[1:]\n"
        "if args and args[0] == 'dump':\n"
        "    output = args[args.index('--output') + 1] if '--output' in args else 'mem0-dump.json'\n"
        "    payload = {\n"
        "        'snapshot_type': 'unified',\n"
        "        'total_memories': 2,\n"
        "        'collection_totals': {'personal': 1, 'digest': 1},\n"
        "        'collections': {\n"
        "            'personal': {'collection_name': 'chronicle_personal', 'total_memories': 1},\n"
        "            'digest': {'collection_name': 'chronicle_digest', 'total_memories': 1},\n"
        "        },\n"
        "        'memories': [\n"
        "            {'id': 'retry-personal', 'memory': 'Recovered personal', 'metadata': {'project': 'status'}, 'source_collection': 'personal', 'source_collection_name': 'chronicle_personal'},\n"
        "            {'id': 'retry-digest', 'memory': 'Recovered digest', 'metadata': {'category': 'news_digest'}, 'source_collection': 'digest', 'source_collection_name': 'chronicle_digest'},\n"
        "        ],\n"
        "    }\n"
        "    with open(output, 'w', encoding='utf-8') as handle:\n"
        "        json.dump(payload, handle, ensure_ascii=False, indent=2)\n"
        "        handle.write('\\n')\n"
        "    print(json.dumps({'status': 'ok', 'output': output}, ensure_ascii=False))\n"
        "    raise SystemExit(0)\n"
        "raise SystemExit(1)\n",
        encoding="utf-8",
    )

    second = run_automation_job(loaded_manifest, loaded_automation, job_name="mem0-dump", trigger_source="pytest")
    assert second["status"] == "ok"
    assert second["run_id"] == first["run_id"]
    assert second["artifact_id"] is not None

    dump_path = chronicle_sandbox.status_root / "mem0-dump.json"
    payload = json.loads(dump_path.read_text(encoding="utf-8"))
    assert payload["snapshot_type"] == "unified"
    assert payload["total_memories"] == 2
    assert payload["collections"]["personal"]["total_memories"] == 1
    assert payload["collections"]["digest"]["total_memories"] == 1

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        run_rows = connection.execute(
            "SELECT id, status FROM automation_runs WHERE job_name = 'mem0-dump'"
        ).fetchall()
    assert len(run_rows) == 1
    assert run_rows[0]["status"] == "ok"


def test_mem0_dump_timeout_finishes_failed_soft(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    bridge_path = chronicle_sandbox.status_root / "scripts" / "mem0_bridge.py"
    bridge_path.write_text(
        "#!/usr/bin/env python3\n"
        "import sys, time\n"
        "sys.stderr.write('started blocking dump\\n')\n"
        "sys.stderr.flush()\n"
        "time.sleep(5)\n",
        encoding="utf-8",
    )
    # Long enough for the child to start and write on a slow CI runner; an
    # interpreter start alone can exceed 50 ms there.
    automation = replace(
        loaded_automation,
        guards=replace(loaded_automation.guards, subprocess_timeout_seconds=1.0),
    )

    result = run_automation_job(loaded_manifest, automation, job_name="mem0-dump", trigger_source="pytest")

    assert result["status"] == "failed_soft"
    assert result["reason"] == "subprocess_timeout"
    assert result["timeout_seconds"] == 1.0
    assert "started blocking dump" in result["stderr"]

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        run_row = connection.execute(
            "SELECT status, details_json FROM automation_runs WHERE job_name = 'mem0-dump'"
        ).fetchone()
    details = json.loads(run_row["details_json"])
    assert run_row["status"] == "failed_soft"
    assert details["reason"] == "subprocess_timeout"


@requires_mem0_bridge
def test_mem0_bridge_dump_exports_unified_snapshot_with_provenance(tmp_path, monkeypatch) -> None:
    module = _load_mem0_bridge_module()

    class FakeMem:
        def __init__(self, memories: list[dict[str, object]]) -> None:
            self._memories = memories

        def get_all(self, user_id: str) -> dict[str, object]:
            assert user_id == module.USER_ID
            return {"results": list(self._memories)}

    personal_memories = [
        {"id": "personal-1", "memory": "Personal memory", "metadata": {"project": "status"}},
    ]
    digest_memories = [
        {"id": "digest-1", "memory": "Digest memory", "metadata": {"category": "news_digest"}},
    ]

    monkeypatch.setattr(module, "_get_client", lambda collection: FakeMem(personal_memories if collection == "personal" else digest_memories))

    output = tmp_path / "mem0-dump.json"
    module.cmd_dump(argparse.Namespace(output=str(output), collection="both"))

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["snapshot_type"] == "unified"
    assert payload["total_memories"] == 2
    assert payload["collection_totals"] == {"personal": 1, "digest": 1}
    assert payload["collections"]["personal"]["collection_name"] == module.COLLECTION_PERSONAL
    assert payload["collections"]["digest"]["collection_name"] == module.COLLECTION_DIGEST
    assert {item["source_collection"] for item in payload["memories"]} == {"personal", "digest"}
    assert {item["source_collection_name"] for item in payload["memories"]} == {
        module.COLLECTION_PERSONAL,
        module.COLLECTION_DIGEST,
    }


@requires_mem0_bridge
def test_mem0_bridge_search_dedupes_cross_collection_duplicates(monkeypatch, capsys) -> None:
    module = _load_mem0_bridge_module()

    class FakeMem:
        def __init__(self, memories: list[dict[str, object]]) -> None:
            self._memories = memories

        def search(self, query: str, user_id: str, limit: int, filters: dict[str, object] | None = None) -> dict[str, object]:
            assert query == "portfolio shipped"
            assert user_id == module.USER_ID
            assert limit == 10
            assert filters is None
            return {"results": list(self._memories)}

    duplicate = {
        "id": "shared-id",
        "memory": "Portfolio V1 shipped to production on example.com",
        "metadata": {"project": "portfolio", "category": "milestone"},
        "score": 0.91,
    }
    monkeypatch.setattr(
        module,
        "_get_client",
        lambda collection: FakeMem([duplicate]),
    )

    module.cmd_search(argparse.Namespace(query=["portfolio", "shipped"], limit=10, category=None, collection="both"))
    output = capsys.readouterr().out
    assert "--- 1 results ---" in output
    assert "collections: personal,digest" in output
    assert "Deduped matches: 2" in output


@requires_mem0_bridge
def test_mem0_bridge_sync_batch_processes_multiple_items(tmp_path, monkeypatch, capsys) -> None:
    module = _load_mem0_bridge_module()
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    class FakeMem:
        def __init__(self, label: str) -> None:
            self.label = label

        def add(self, text: str, user_id: str, metadata: dict[str, object] | None = None) -> dict[str, object]:
            assert user_id == module.USER_ID
            calls.append((self.label, text, metadata))
            return {"id": f"{self.label}-{len(calls)}"}

    clients = {
        "personal": FakeMem("personal"),
        "digest": FakeMem("digest"),
    }
    monkeypatch.setattr(module, "_get_client", lambda collection: clients[collection])

    batch_path = tmp_path / "mem0-batch.json"
    batch_path.write_text(
        json.dumps(
            {
                "items": [
                    {"event_id": "evt-1", "text": "Portfolio shipped", "category": "milestone", "project": "portfolio"},
                    {"event_id": "evt-2", "text": "Digest event", "category": module.DIGEST_CATEGORY},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    module.cmd_sync_batch(argparse.Namespace(input=str(batch_path), max_retries=2, retry_base_delay=0.0))
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["processed_count"] == 2
    assert calls[0][0] == "personal"
    assert calls[1][0] == "digest"
    assert payload["processed"][0]["event_id"] == "evt-1"
    assert payload["processed"][1]["event_id"] == "evt-2"


def test_add_to_live_mem0_retries_transient_provider_errors(monkeypatch, loaded_manifest, loaded_automation) -> None:
    attempts: list[list[str]] = []

    class FakeResult:
        def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    responses = [
        FakeResult(returncode=1, stderr="503 UNAVAILABLE"),
        FakeResult(returncode=0, stdout='{"status":"ok"}'),
    ]

    monkeypatch.setattr(native_automation_module, "_MEM0_MAX_RETRIES", 2)
    monkeypatch.setattr(native_automation_module, "_MEM0_RETRY_BASE_DELAY", 0.0)
    monkeypatch.setattr(native_automation_module, "_maybe_load_env", lambda automation: None)
    monkeypatch.setattr(native_automation_module, "_mem0_command", lambda manifest: ["./scripts/mem0"])
    monkeypatch.setattr(native_automation_module.time, "sleep", lambda seconds: None)

    def fake_run(command: list[str], **kwargs: object) -> FakeResult:
        attempts.append(list(command))
        return responses.pop(0)

    monkeypatch.setattr(native_automation_module.subprocess, "run", fake_run)

    result = native_automation_module._add_to_live_mem0(
        loaded_manifest,
        loaded_automation,
        text="Retry me",
        category="decision",
        project="status",
    )

    assert result["ok"] is True
    assert result["attempts"] == 2
    assert len(attempts) == 2


def test_sync_mem0_outbox_uses_batch_writer(monkeypatch, loaded_manifest, loaded_automation) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "decision",
            "project": "status",
            "text": "Batch sync candidate",
            "why": "Need coverage for the long-lived Mem0 batch writer path.",
            "source_files": [],
            "mem0_status": "queued",
            "mem0_error": None,
            "mem0_raw": None,
        },
    )

    monkeypatch.setattr(
        native_automation_module,
        "_add_to_live_mem0_batch",
        lambda manifest, automation, items: {
            "ok": True,
            "payload": {
                "processed": [
                    {
                        "event_id": stored["id"],
                        "ok": True,
                        "raw": '{"status":"ok"}',
                    }
                ]
            },
        },
    )
    monkeypatch.setattr(
        native_automation_module,
        "_add_to_live_mem0",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("fallback path should not run")),
    )

    result = sync_mem0_outbox(loaded_manifest, loaded_automation, limit=5, trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["synced"] == 1
    assert result["failed"] == 0

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        row = connection.execute(
            "SELECT mem0_status, mem0_error FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
    assert row["mem0_status"] == "stored"
    assert row["mem0_error"] is None


def test_audit_flags_mem0_drift_legacy_refs_and_stale_dump(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    config = config_from_manifest(loaded_manifest)
    event = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "record",
        "Audit drift target",
        "--project",
        "status",
    )
    event_id = json.loads(event.stdout)["id"]
    with open_connection(config) as connection, connection:
        connection.execute(
            "UPDATE mem0_outbox SET status = 'synced', synced_at_utc = '2026-03-16T10:00:00Z' WHERE event_id = ?",
            (event_id,),
        )
    protocol_path = Path(loaded_manifest["source_map"]["chronicle_protocol"]["path"])
    protocol_path.write_text(protocol_path.read_text(encoding="utf-8") + "\nlegacy memory_healthcheck.py reference\n", encoding="utf-8")
    stale_ts = datetime(2026, 3, 10, 12, 0, 0).timestamp()
    os.utime(chronicle_sandbox.status_root / "mem0-dump.json", (stale_ts, stale_ts))

    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Audit seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    result = run_audit(loaded_manifest, loaded_automation, trigger_source="pytest")
    issue_kinds = {issue["kind"] for issue in result["issues"]}
    assert "mem0_dump_stale" in issue_kinds
    assert "mem0_state_drift" in issue_kinds
    assert "legacy_surface_reference" in issue_kinds


def test_audit_force_creates_fresh_run_same_day(loaded_manifest, loaded_automation) -> None:
    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Audit force seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    first = run_audit(loaded_manifest, loaded_automation, trigger_source="pytest")
    second = run_audit(loaded_manifest, loaded_automation, trigger_source="pytest")
    third = run_audit(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    assert second["status"] == "existing"
    assert third["status"] in {"ok", "issues"}
    assert first["run_id"] != third["run_id"]

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        run_count = connection.execute(
            "SELECT COUNT(*) FROM curation_runs WHERE curation_type = 'daily_audit'"
        ).fetchone()[0]
    assert run_count == 2


@requires_darwin
def test_launchd_install_and_doctor(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    install = install_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
        log_dir=chronicle_sandbox.status_root / "logs" / "launchd",
        load_jobs=False,
    )
    assert len(install["installed"]) == 5

    doctor = doctor_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
    )
    assert doctor["status"] == "ok"
    assert doctor["issue_count"] == 0
    assert all(job["plist_exists"] for job in doctor["jobs"])
    assert all(job["wrapper_exists"] for job in doctor["jobs"])
    assert all(job["wrapper_executable"] for job in doctor["jobs"])
    assert all(job["plist_lint_ok"] for job in doctor["jobs"])


@requires_darwin
def test_launchd_doctor_strict_flags_unloaded_jobs(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    install_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
        log_dir=chronicle_sandbox.status_root / "logs" / "launchd",
        load_jobs=False,
    )

    doctor = doctor_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
        strict_loaded=True,
    )
    assert doctor["status"] == "issues"
    assert doctor["issue_count"] >= 1
    assert any("not_loaded" in job["issues"] for job in doctor["jobs"])


@requires_darwin
def test_launchd_doctor_flags_missing_wrapper_as_issue(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    install_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
        log_dir=chronicle_sandbox.status_root / "logs" / "launchd",
        load_jobs=False,
    )
    missing_wrapper = chronicle_sandbox.status_root / "runtime" / "launchd" / "mem0_dump.sh"
    missing_wrapper.unlink()

    doctor = doctor_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
    )
    expected_label = launchd_labels(loaded_automation)["mem0_dump"]
    mem0_job = next(job for job in doctor["jobs"] if job["label"] == expected_label)
    assert doctor["status"] == "issues"
    assert doctor["issue_count"] >= 1
    assert mem0_job["status"] == "critical"
    assert mem0_job["wrapper_exists"] is False
    assert "missing_wrapper" in mem0_job["issues"]


def test_daily_capture_keeps_mem0_sync_non_blocking(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "record",
        "Queued Chronicle memory",
        "--project",
        "status",
    )
    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["snapshot_id"]
    assert result["sync"]["status"] == "ok"
    assert result["sync"]["synced"] >= 1


def test_daily_capture_exception_finalizes_failed_automation_run(monkeypatch, loaded_manifest, loaded_automation) -> None:
    def raise_mid_run(*args: object, **kwargs: object) -> dict[str, object]:
        raise RuntimeError("snapshot capture exploded")

    monkeypatch.setattr(native_automation_module, "capture_runtime_snapshot", raise_mid_run)

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")

    assert result["status"] == "failed"
    assert result["error"]["type"] == "RuntimeError"
    assert "snapshot capture exploded" in result["error"]["message"]

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        rows = connection.execute(
            """
            SELECT status, finished_at_utc, details_json
            FROM automation_runs
            WHERE job_name = 'daily-capture'
            """
        ).fetchall()
        running_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM automation_runs
            WHERE job_name = 'daily-capture' AND status = 'running'
            """
        ).fetchone()[0]

    assert running_count == 0
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["finished_at_utc"] is not None
    details = json.loads(rows[0]["details_json"])
    assert details["error"]["type"] == "RuntimeError"
    assert "snapshot capture exploded" in details["error"]["summary"]


def test_sync_mem0_outbox_marks_entries_synced(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "record",
        "Replay this durable note",
        "--project",
        "status",
    )
    result = sync_mem0_outbox(loaded_manifest, loaded_automation, limit=10, trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["synced"] >= 1

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        statuses = connection.execute("SELECT DISTINCT status FROM mem0_outbox").fetchall()
    assert {"synced"} <= {row[0] for row in statuses}


def test_sync_mem0_outbox_skips_guarded_local_only_entries(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    stored = record_event(
        loaded_manifest,
        {
            "agent": "pytest",
            "domain": "global",
            "category": "state_change",
            "project": "guard-shadow",
            "text": "## Protocol Update\nOld flow: max-chronicle activate.\nNew flow: chronicle-mcp-chronicler only.\nDO NOT use the old shell path.",
            "why": None,
            "source_files": [],
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_mcp",
        imported_from="tests.test_native_automation.guarded_sync",
    )
    assert stored["memory_guard"]["verdict"] == "local_only"

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection, connection:
        event_row = connection.execute(
            "SELECT payload_json FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox_row = connection.execute(
            "SELECT payload_json FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
        event_payload = json.loads(event_row["payload_json"])
        outbox_payload = json.loads(outbox_row["payload_json"])
        event_payload["mem0_status"] = "queued"
        outbox_payload["memory_guard"] = stored["memory_guard"]
        connection.execute(
            "UPDATE events SET mem0_status = 'queued', payload_json = ? WHERE id = ?",
            (json.dumps(event_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )
        connection.execute(
            "UPDATE mem0_outbox SET status = 'pending', last_error = NULL, payload_json = ? WHERE event_id = ?",
            (json.dumps(outbox_payload, ensure_ascii=False, sort_keys=True), stored["id"]),
        )

    monkeypatch.setattr(
        native_automation_module,
        "_add_to_live_mem0_batch",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("guarded entries must not reach batch sync")),
    )
    monkeypatch.setattr(
        native_automation_module,
        "_add_to_live_mem0",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("guarded entries must not reach single sync")),
    )

    result = sync_mem0_outbox(loaded_manifest, loaded_automation, limit=10, trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["skipped"] == 1
    assert {"event_id": stored["id"], "status": "skipped", "reason": "guarded_local_only"} in result["processed"]

    with open_connection(config) as connection:
        row = connection.execute(
            "SELECT mem0_status, mem0_error FROM events WHERE id = ?",
            (stored["id"],),
        ).fetchone()
        outbox = connection.execute(
            "SELECT status, last_error FROM mem0_outbox WHERE event_id = ?",
            (stored["id"],),
        ).fetchone()
    assert row["mem0_status"] == "skipped"
    assert row["mem0_error"] == "guarded_local_only"
    assert outbox["status"] == "skipped"
    assert outbox["last_error"] == "guarded_local_only"


def test_install_git_hooks_updates_repo_config(chronicle_sandbox, loaded_automation) -> None:
    result = install_git_hooks(loaded_automation, repos=["demo-portfolio"])
    assert result["status"] == "ok"
    hooks_path = subprocess.run(
        ["git", "-C", str(chronicle_sandbox.portfolio_repo), "config", "--get", "core.hooksPath"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert hooks_path == str(chronicle_sandbox.status_root / "git-hooks")


def test_jsonl_rotation_archives_oversized_file_and_resets(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    """Backup maintenance rotates chronicle-snapshots.jsonl when it exceeds the cap."""
    from dataclasses import replace as dc_replace
    from max_chronicle.automation import GuardSettings

    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)

    # Write a file that is just over 1 byte cap (tiny cap for testing).
    snapshots_path = chronicle_sandbox.status_root / "chronicle-snapshots.jsonl"
    snapshots_path.write_text('{"event":"seed"}\n', encoding="utf-8")
    assert snapshots_path.stat().st_size > 0

    # Use a 1-byte cap so the file triggers rotation.
    tiny_guards = dc_replace(loaded_automation.guards, jsonl_rotate_mb=1 / (1024 * 1024))
    automation_tiny = dc_replace(loaded_automation, guards=tiny_guards)

    result = run_backup(loaded_manifest, automation_tiny, trigger_source="pytest", force=True)

    assert result["status"] == "ok"
    rotation = result["maintenance"]["jsonl_rotation"]
    assert rotation["snapshots"]["status"] == "rotated"
    archive_path = Path(rotation["snapshots"]["archive_path"])
    assert archive_path.exists(), "gzip archive must be created"
    assert archive_path.suffix == ".gz"
    # The live file should now be empty (fresh start).
    assert snapshots_path.stat().st_size == 0


def test_jsonl_rotation_skips_when_below_cap(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    """Backup maintenance skips rotation when file is below the cap."""
    from dataclasses import replace as dc_replace

    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)

    snapshots_path = chronicle_sandbox.status_root / "chronicle-snapshots.jsonl"
    snapshots_path.write_text('{"event":"seed"}\n', encoding="utf-8")

    # Use a very large cap so no rotation happens.
    large_guards = dc_replace(loaded_automation.guards, jsonl_rotate_mb=1000.0)
    automation_large = dc_replace(loaded_automation, guards=large_guards)

    result = run_backup(loaded_manifest, automation_large, trigger_source="pytest", force=True)

    assert result["status"] == "ok"
    rotation = result["maintenance"]["jsonl_rotation"]
    assert rotation["snapshots"]["status"] == "skipped"
    assert rotation["snapshots"]["skip_reason"] == "below_cap"


def test_audit_emits_artifact_store_large_warn_when_over_cap(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    """Audit emits artifact_store_large warn when store size exceeds the configured cap."""
    from dataclasses import replace as dc_replace

    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Artifact store size seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )

    # Write a real file in the artifact dir so the store has non-zero size.
    artifact_dir = chronicle_sandbox.status_root / "chronicle-artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    test_blob = artifact_dir / "test-blob.bin"
    test_blob.write_bytes(b"x" * 1024)  # 1 KB

    # Use a 1-byte cap → 1 KB > 1 byte → triggers warn.
    tiny_guards = dc_replace(loaded_automation.guards, artifact_store_warn_gb=1 / (1024 ** 3))
    automation_tiny_cap = dc_replace(loaded_automation, guards=tiny_guards)

    result = run_audit(loaded_manifest, automation_tiny_cap, trigger_source="pytest")
    issue_kinds = {issue["kind"] for issue in result["issues"]}
    assert "artifact_store_large" in issue_kinds


def test_audit_does_not_emit_artifact_store_warn_when_below_cap(
    loaded_manifest, loaded_automation
) -> None:
    """Audit does NOT emit artifact_store_large when store is small."""
    from dataclasses import replace as dc_replace

    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Artifact store below cap seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )

    # Very large cap — real artifact dir in sandbox is tiny.
    large_guards = dc_replace(loaded_automation.guards, artifact_store_warn_gb=1000.0)
    automation_large_cap = dc_replace(loaded_automation, guards=large_guards)

    result = run_audit(loaded_manifest, automation_large_cap, trigger_source="pytest")
    issue_kinds = {issue["kind"] for issue in result["issues"]}
    assert "artifact_store_large" not in issue_kinds


def test_end_to_end_acceptance(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    local_date = _local_date(loaded_manifest["settings"]["timezone"])

    repo = chronicle_sandbox.digest_repo
    tracked = repo / "new.md"
    tracked.write_text("Tracked update\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "acceptance change"], check=True, capture_output=True, text=True)
    commit_sha = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    git_result = run_git_commit_hook(
        loaded_manifest,
        loaded_automation,
        repo_slug="news-digest",
        commit_sha=commit_sha,
        repo_root=repo,
        trigger_source="pytest",
    )

    daily_capture = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")
    daybook = run_daybook(loaded_manifest, loaded_automation, target_date=local_date, trigger_source="pytest")
    backup = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        summary = database_summary(connection)

    assert git_result["status"] == "stored"
    assert daily_capture["status"] == "ok"
    assert daybook["status"] in {"ok", "failed_soft"}
    assert backup["status"] == "ok"
    assert summary["events"] >= 2
    assert summary["snapshots"] >= 1


def test_backup_registers_manifest_artifact_so_weekly_restore_drill_passes(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    """The weekly drill asserts the manifest is registered inside the backup copy.

    _upsert_backup_manifest_in_db existed but was never called, so every weekly
    audit reported ok=false on healthy backups.
    """
    from max_chronicle.native_automation import _restore_check

    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    capture_runtime_snapshot(
        loaded_manifest,
        domain_id="global",
        agent="pytest",
        title="Backup manifest registration seed",
        focus="tests",
        append_compat=True,
        render_generated=True,
    )
    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    assert result["status"] == "ok"

    backup_path = Path(result["backup_path"])
    backup_db = backup_path / "chronicle.db"
    with sqlite3.connect(backup_db) as connection:
        connection.row_factory = sqlite3.Row
        artifacts = connection.execute(
            "SELECT COUNT(*) FROM artifacts WHERE artifact_type = 'backup-manifest' AND storage_path = ?",
            (result["manifest_path"],),
        ).fetchone()[0]
        links = connection.execute(
            """
            SELECT COUNT(*)
            FROM artifact_links
            WHERE target_type = 'backup_run' AND target_id = ? AND link_role = 'generated'
            """,
            (result["run_id"],),
        ).fetchone()[0]
    assert artifacts == 1
    assert links == 1

    # The strict drill (require_manifest=True) is what the weekly audit runs.
    drill = _restore_check(backup_db, backup_path=backup_path, require_manifest=True)
    assert drill["backup_manifest_artifacts"] == 1
    assert drill["backup_manifest_links"] == 1
    assert drill["ok"] is True

    # Registration must happen before the final checkpoint+hash, otherwise the
    # rows sit in the WAL and the advertised db_sha256 describes a database
    # that does not include them.
    manifest_payload = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    assert manifest_payload["db_sha256"] == hashlib.sha256(backup_db.read_bytes()).hexdigest(), (
        "manifest advertises a hash that does not match the shipped database"
    )
    wal_path = Path(str(backup_db) + "-wal")
    assert not wal_path.exists() or wal_path.stat().st_size == 0, (
        "manifest rows stranded in the backup WAL — copying chronicle.db alone would lose them"
    )


def test_backup_reports_intentional_purge_without_claiming_file_is_archived(
    chronicle_sandbox, loaded_manifest, loaded_automation
):
    from max_chronicle.store import store_artifact_from_path

    source = chronicle_sandbox.status_root / "retired-evidence.txt"
    source.write_text("Explicitly removed historical evidence")
    config = config_from_manifest(loaded_manifest)
    artifact = store_artifact_from_path(config, source_path=source, artifact_type="event-source")
    Path(artifact["storage_path"]).unlink()
    with open_connection(config) as connection, connection:
        connection.execute(
            "UPDATE artifacts SET metadata_json=json_set(metadata_json,'$.storage_mode','purged') WHERE id=?",
            (artifact["id"],),
        )
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    assert result["status"] == "ok"
    assert result["restore_check"]["intentionally_purged_artifacts"] == 1
    assert result["restore_check"]["artifact_hashes_ok"] is True
