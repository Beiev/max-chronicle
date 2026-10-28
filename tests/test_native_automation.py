from __future__ import annotations

import argparse
from dataclasses import replace
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
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.service import capture_runtime_snapshot, record_event
from max_chronicle.store import config_from_manifest, open_connection


PROJECT_STATUS_ROOT = Path(__file__).resolve().parents[1]


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


# doctor_launchd inspects real launchd state through macOS-only tooling; the
# code degrades gracefully elsewhere, but these assertions only mean something
# on darwin.
requires_darwin = pytest.mark.skipif(
    sys.platform != "darwin",
    reason="launchd inspection is macOS-only",
)


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


def test_backup_records_wal_maintenance(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    assert result["status"] == "ok"
    assert set(result["maintenance"]) >= {"jsonl_rotation", "vacuum", "wal_checkpoint"}
    assert result["maintenance"]["wal_checkpoint"] is not None
    assert result["maintenance"]["wal_checkpoint"]["reason"] == "post_backup"


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


def test_audit_flags_legacy_refs(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    protocol_path = Path(loaded_manifest["source_map"]["chronicle_protocol"]["path"])
    protocol_path.write_text(protocol_path.read_text(encoding="utf-8") + "\nlegacy memory_healthcheck.py reference\n", encoding="utf-8")

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
    assert "legacy_surface_reference" in issue_kinds
    assert not any("mem0" in kind for kind in issue_kinds)
    assert not any(key.startswith("mem0") for key in result)


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
    assert len(install["installed"]) == 4

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
    missing_wrapper = chronicle_sandbox.status_root / "runtime" / "launchd" / "daybook.sh"
    missing_wrapper.unlink()

    doctor = doctor_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
    )
    expected_label = launchd_labels(loaded_automation)["daybook"]
    daybook_job = next(job for job in doctor["jobs"] if job["label"] == expected_label)
    assert doctor["status"] == "issues"
    assert doctor["issue_count"] >= 1
    assert daybook_job["status"] == "critical"
    assert daybook_job["wrapper_exists"] is False
    assert "missing_wrapper" in daybook_job["issues"]


def test_daily_capture_stores_a_snapshot(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "record",
        "Chronicle memory before the capture",
        "--project",
        "status",
    )
    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["snapshot_id"]
    assert "sync" not in result


def test_daily_capture_with_failed_side_effects_is_not_ok(monkeypatch, loaded_manifest, loaded_automation) -> None:
    import max_chronicle.service as service_module
    from max_chronicle.brief import build_brief

    def broken(*args: object, **kwargs: object) -> list[dict[str, object]]:
        raise RuntimeError("projection renderer broke")

    monkeypatch.setattr(service_module, "render_projections", broken)

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")

    assert result["status"] == "failed_soft" and result["snapshot_id"]
    assert "projection renderer broke" in result["side_effect_errors"]["projections"]
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        [(status, details)] = connection.execute(
            "SELECT status, details_json FROM automation_runs WHERE job_name = 'daily-capture'").fetchall()
    assert status == "failed_soft" and "projections" in json.loads(details)["side_effect_errors"]
    assert "Scheduled job daily-capture: failed_soft" in build_brief(loaded_manifest)["text"]


def test_a_snapshot_without_a_jsonl_file_in_the_manifest_keeps_none(loaded_manifest) -> None:
    manifest = {**loaded_manifest, "paths": {key: value for key, value in loaded_manifest["paths"].items()
                                             if key != "snapshot_file"}}

    stored = capture_runtime_snapshot(manifest, domain_id="global", agent="pytest", append_compat=True,
                                      render_generated=False)

    assert "compat_snapshot" not in stored["side_effect_errors"]


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


def test_a_backup_reports_retention_and_removes_nothing_by_default(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    first = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    second = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    assert second["status"] == "ok"
    assert second["retention"]["applied"] is False
    assert second["retention"]["backups"] == 2
    assert Path(first["backup_path"]).exists()


def test_a_backup_with_pruning_on_keeps_what_the_policy_keeps(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    from dataclasses import replace

    from max_chronicle.backup_retention import RetentionPolicy

    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    old = chronicle_sandbox.backup_root / "2025" / "2025-01-01T23-00-00Z"
    old.mkdir(parents=True)
    (old / "backup-manifest.json").write_text("{}", encoding="utf-8")
    automation = replace(loaded_automation, backup_retention=RetentionPolicy(daily=1, weekly=0, monthly=0, prune=True))

    result = run_backup(loaded_manifest, automation, trigger_source="pytest", force=True)

    assert result["retention"]["result"]["removed"] == 1
    assert not old.exists() and Path(result["backup_path"]).exists()


def _legacy_without_auto_vacuum(config) -> None:
    """The production database predates INCREMENTAL auto-vacuum."""
    from max_chronicle.store import prepare_database

    prepare_database(config, allow_upgrade=True)
    with sqlite3.connect(config.db_path) as connection:
        connection.execute("PRAGMA auto_vacuum = NONE")
        connection.execute("VACUUM")


def test_the_backup_skips_a_full_vacuum_when_little_is_free(loaded_manifest) -> None:
    from max_chronicle.native_automation import _vacuum_db

    config = config_from_manifest(loaded_manifest)
    _legacy_without_auto_vacuum(config)

    result = _vacuum_db(config)

    assert result["status"] == "skipped" and result["reason"] == "little_free_space"


def test_the_backup_returns_every_free_page_of_an_incremental_file(loaded_manifest) -> None:
    from max_chronicle.native_automation import _vacuum_db

    config = config_from_manifest(loaded_manifest)
    with sqlite3.connect(config.db_path) as connection:
        connection.execute("CREATE TABLE scratch_fill(blob BLOB)")
        connection.executemany("INSERT INTO scratch_fill VALUES (?)", [(os.urandom(4096),) for _ in range(200)])
        connection.commit()
        connection.execute("DROP TABLE scratch_fill")
        connection.commit()
        assert connection.execute("PRAGMA freelist_count").fetchone()[0] > 100

    result = _vacuum_db(config)

    assert (result["status"], result["mode"]) == ("ok", "incremental_vacuum")
    with sqlite3.connect(config.db_path) as connection:
        assert connection.execute("PRAGMA freelist_count").fetchone()[0] == 0


def test_the_backup_still_vacuums_a_file_that_is_mostly_free(loaded_manifest) -> None:
    from max_chronicle.native_automation import _vacuum_db

    config = config_from_manifest(loaded_manifest)
    _legacy_without_auto_vacuum(config)
    with sqlite3.connect(config.db_path) as connection:
        connection.execute("CREATE TABLE scratch_fill(blob BLOB)")
        connection.executemany("INSERT INTO scratch_fill VALUES (?)", [(os.urandom(4096),) for _ in range(2000)])
        connection.commit()
        connection.execute("DROP TABLE scratch_fill")
        connection.commit()

    result = _vacuum_db(config)

    assert (result["status"], result["mode"]) == ("ok", "vacuum")
    with sqlite3.connect(config.db_path) as connection:  # the next run is incremental
        assert connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 2


def test_launchd_backup_job_still_starts_when_its_disk_is_mounted(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    """A disk unplugged at the scheduled time is backed up when it comes back."""
    from max_chronicle.native_automation import install_launchd
    import plistlib

    agents = chronicle_sandbox.root / "agents"
    install_launchd(loaded_manifest, loaded_automation, agent_dir=agents,
                    runtime_dir=chronicle_sandbox.root / "rt", log_dir=chronicle_sandbox.root / "logs", load_jobs=False)

    plists = {path.name: plistlib.loads(path.read_bytes()) for path in agents.glob("*.plist")}
    backup = [payload for name, payload in plists.items() if name.endswith("backup.plist")]
    assert backup and backup[0]["StartOnMount"] is True


def test_backup_prune_refuses_an_unmounted_backup_disk(loaded_automation, monkeypatch, capsys, tmp_path) -> None:
    """An unplugged disk leaves a bare folder of its name; pruning there removes the wrong copies."""
    from max_chronicle import cli

    monkeypatch.setattr("max_chronicle.native_automation._backup_volume_root", lambda root: tmp_path / "Volumes" / "Gone")

    code = cli.cmd_backup_prune(argparse.Namespace(automation_config=loaded_automation.root_path, apply=True))

    assert code == 0 and json.loads(capsys.readouterr().out)["reason"] == "backup_target_not_mounted"


def test_embed_backfill_reports_the_whole_backlog_not_the_batch(loaded_manifest, monkeypatch) -> None:
    from max_chronicle import service as service_module

    for index in range(5):
        record_event(loaded_manifest, {"agent": "agent-a", "domain": "global", "text": f"Event {index}"})
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda *a, **k: [1.0, 0.5])

    report = service_module.embed_backfill(loaded_manifest, limit=2)

    assert (report["embedded"], report["total_without_embedding"]) == (2, 5)



def test_a_backup_skipped_for_a_missing_disk_is_deferred_and_retried_the_same_day(
    chronicle_sandbox, loaded_manifest, loaded_automation
) -> None:
    """Round-2 review: the skip took the day's key, so plugging the disk in later backed up nothing."""
    import shutil

    shutil.rmtree(chronicle_sandbox.backup_root, ignore_errors=True)

    first = run_automation_job(loaded_manifest, loaded_automation, job_name="backup", trigger_source="pytest")
    chronicle_sandbox.backup_root.mkdir(parents=True)
    second = run_automation_job(loaded_manifest, loaded_automation, job_name="backup", trigger_source="pytest")

    assert first["status"] == "deferred"
    assert second["status"] == "ok"
