from __future__ import annotations

import argparse
from dataclasses import replace
import importlib.util
from datetime import datetime
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from typing import Any
from zoneinfo import ZoneInfo

import max_chronicle.native_automation as native_automation_module
from max_chronicle.db import database_summary
from max_chronicle.native_automation import (
    doctor_launchd,
    install_git_hooks,
    install_launchd,
    run_audit,
    run_minimax_canary,
    run_minimax_smoke,
    run_automation_job,
    run_backup,
    run_daybook,
    run_digest_hook,
    run_git_commit_hook,
    sync_mem0_outbox,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.service import capture_runtime_snapshot, record_event
from max_chronicle.store import config_from_manifest, open_connection


PROJECT_STATUS_ROOT = Path("/Users/maksymbeiev/Projects/status")
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


def _load_mem0_bridge_module():
    spec = importlib.util.spec_from_file_location("mem0_bridge_test_module", MEM0_BRIDGE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _local_date_from_hook(result: dict[str, object], timezone_name: str) -> str:
    hook_event = result["hook_event"]
    if not isinstance(hook_event, dict):
        raise AssertionError("Expected hook_event payload in automation result")
    triggered_at = hook_event.get("triggered_at_utc")
    if not isinstance(triggered_at, str):
        raise AssertionError("Expected hook_event.triggered_at_utc")
    return datetime.fromisoformat(triggered_at.replace("Z", "+00:00")).astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")


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
    assert len(first_payload["applied"]) == 7
    assert second_payload["applied"] == []
    assert first_payload["summary"]["user_version"] == 7
    assert second_payload["summary"]["user_version"] == 7


def test_digest_hook_is_idempotent(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    first = run_digest_hook(loaded_manifest, loaded_automation, trigger_source="pytest")
    second = run_digest_hook(loaded_manifest, loaded_automation, trigger_source="pytest")
    assert first["status"] == "stored"
    assert first["snapshot_id"] is not None
    assert second["status"] == "existing"

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        hook_count = connection.execute("SELECT COUNT(*) FROM hook_events WHERE hook_type = 'digest-run'").fetchone()[0]
        event_ref_count = connection.execute("SELECT COUNT(*) FROM event_external_refs WHERE ref_type = 'digest_run'").fetchone()[0]
    assert hook_count == 1
    assert event_ref_count == 1


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
        repo_slug="rzmrn-portfolio",
        commit_sha=commit_sha,
        repo_root=repo,
        trigger_source="pytest",
    )
    second = run_git_commit_hook(
        loaded_manifest,
        loaded_automation,
        repo_slug="rzmrn-portfolio",
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


def test_daybook_uses_deterministic_fallback(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    digest_result = run_digest_hook(loaded_manifest, loaded_automation, trigger_source="pytest")
    local_date = _local_date_from_hook(digest_result, loaded_manifest["settings"]["timezone"])
    monkeypatch.setattr("max_chronicle.native_automation._call_minimax", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("offline")))
    result = run_daybook(loaded_manifest, loaded_automation, target_date=local_date, trigger_source="pytest")
    assert result["status"] == "failed_soft"
    daybook_path = chronicle_sandbox.status_root / "daybooks" / local_date[:4] / f"{local_date}.md"
    assert daybook_path.exists()
    assert "## Key Events" in daybook_path.read_text(encoding="utf-8")


def test_daybook_uses_mocked_minimax(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    digest_result = run_digest_hook(loaded_manifest, loaded_automation, trigger_source="pytest")
    local_date = _local_date_from_hook(digest_result, loaded_manifest["settings"]["timezone"])
    monkeypatch.setattr("max_chronicle.native_automation._call_minimax", lambda *args, **kwargs: "Narrative from MiniMax.")
    result = run_daybook(loaded_manifest, loaded_automation, target_date=local_date, trigger_source="pytest")
    assert result["status"] == "ok"
    daybook_path = chronicle_sandbox.status_root / "daybooks" / local_date[:4] / f"{local_date}.md"
    assert "## Narrative" in daybook_path.read_text(encoding="utf-8")


def test_minimax_smoke_uses_high_enough_token_budget(monkeypatch, loaded_automation) -> None:
    captured: dict[str, Any] = {}

    def fake_call(automation, *, system_prompt: str, user_content: str, max_tokens: int | None = None) -> str:
        captured["model"] = automation.minimax.model
        captured["system_prompt"] = system_prompt
        captured["user_content"] = user_content
        captured["max_tokens"] = max_tokens
        return "OK"

    monkeypatch.setattr("max_chronicle.native_automation._call_minimax", fake_call)
    payload = run_minimax_smoke(loaded_automation)

    assert payload["status"] == "ok"
    assert payload["text"] == "OK"
    assert captured["model"] == "MiniMax-M2.7"
    assert captured["max_tokens"] == 256


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
    assert result["restore_check"]["backup_manifest_present"] is True
    assert result["restore_check"]["backup_manifest_links"] == 1
    assert result["restore_check"]["artifact_tree_complete"] is True
    assert result["backup_policy"] == "opportunistic_external"
    assert result["maintenance"]["reason"] in {"nothing_to_prune", "pruned_retained_rows"}
    assert result["maintenance"]["wal_checkpoint"]["reason"] == "post_backup"

    backup_db = Path(result["backup_path"]) / "chronicle.db"
    with sqlite3.connect(backup_db) as connection:
        connection.row_factory = sqlite3.Row
        run_row = connection.execute(
            "SELECT status, manifest_path, restore_ok FROM backup_runs WHERE id = ?",
            (result["run_id"],),
        ).fetchone()
        link_count = connection.execute(
            """
            SELECT COUNT(*)
            FROM artifact_links
            WHERE target_type = 'backup_run'
              AND target_id = ?
              AND link_role = 'generated'
            """,
            (result["run_id"],),
        ).fetchone()[0]
    assert run_row["status"] == "ok"
    assert run_row["manifest_path"] == result["manifest_path"]
    assert run_row["restore_ok"] == 1
    assert link_count == 1


def test_backup_uses_hardlinks_for_artifacts(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    artifact_source = chronicle_sandbox.status_root / "chronicle-artifacts" / "snapshots" / "shared.json"
    artifact_source.parent.mkdir(parents=True, exist_ok=True)
    artifact_source.write_text('{"hello":"world"}\n', encoding="utf-8")

    result = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)
    backup_artifact = Path(result["backup_path"]) / "chronicle-artifacts" / "snapshots" / "shared.json"

    assert result["status"] == "ok"
    assert backup_artifact.exists()
    assert backup_artifact.stat().st_ino == artifact_source.stat().st_ino
    assert result["artifact_files_copied"] == 0
    assert result["artifact_copy_strategy"]["hardlinked"] >= 1


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
        "            'personal': {'collection_name': 'napaarnik_personal', 'total_memories': 1},\n"
        "            'digest': {'collection_name': 'napaarnik_memory', 'total_memories': 1},\n"
        "        },\n"
        "        'memories': [\n"
        "            {'id': 'personal-memory', 'memory': 'Chronicle sandbox memory', 'metadata': {'project': 'status'}, 'source_collection': 'personal', 'source_collection_name': 'napaarnik_personal'},\n"
        "            {'id': 'digest-memory', 'memory': 'Digest sandbox memory', 'metadata': {'category': 'intel_digest'}, 'source_collection': 'digest', 'source_collection_name': 'napaarnik_memory'},\n"
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
    assert payload["collections"]["personal"]["collection_name"] == "napaarnik_personal"
    assert payload["collections"]["digest"]["collection_name"] == "napaarnik_memory"
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
        "            'personal': {'collection_name': 'napaarnik_personal', 'total_memories': 1},\n"
        "            'digest': {'collection_name': 'napaarnik_memory', 'total_memories': 1},\n"
        "        },\n"
        "        'memories': [\n"
        "            {'id': 'retry-personal', 'memory': 'Recovered personal', 'metadata': {'project': 'status'}, 'source_collection': 'personal', 'source_collection_name': 'napaarnik_personal'},\n"
        "            {'id': 'retry-digest', 'memory': 'Recovered digest', 'metadata': {'category': 'intel_digest'}, 'source_collection': 'digest', 'source_collection_name': 'napaarnik_memory'},\n"
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


def test_company_intel_automation_refreshes_output_and_archives_artifact(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    script_path = chronicle_sandbox.status_root / "scripts" / "company_intel_bridge.py"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    output_path = chronicle_sandbox.status_root / "company-intel.json"
    script_path.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os\n"
        "from pathlib import Path\n"
        "if not os.environ.get('GOOGLE_API_KEY'):\n"
        "    raise SystemExit(3)\n"
        f"output = Path({str(output_path)!r})\n"
        "payload = {\n"
        "    'last_updated': '2026-03-21T22:00:00',\n"
        "    'days_covered': 14,\n"
        "    'companies': {'adobe': {'name': 'Adobe', 'mentions': 2, 'latest': '2026-03-21', 'events': ['Adobe expands hiring (2026-03-21)']}},\n"
        "    'industry_context': [{'date': '2026-03-21', 'domain': 'AI + JOBS', 'headline': 'Hiring signal', 'impact': 'Positive'}],\n"
        "    'lead_companies': {'with_intel': ['Adobe'], 'without_intel': []},\n"
        "}\n"
        "output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\\n', encoding='utf-8')\n"
        "print(json.dumps({'status': 'ok', 'output': str(output)}))\n",
        encoding="utf-8",
    )

    result = run_automation_job(loaded_manifest, loaded_automation, job_name="company-intel", trigger_source="pytest")
    assert result["status"] == "ok"
    assert result["artifact_id"] is not None
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["companies"]["adobe"]["name"] == "Adobe"
    assert result["company_count"] == 1
    assert result["industry_event_count"] == 1

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        run_row = connection.execute(
            "SELECT status FROM automation_runs WHERE job_name = 'company-intel'"
        ).fetchone()
        artifact_row = connection.execute(
            "SELECT artifact_type FROM artifacts WHERE id = ?",
            (result["artifact_id"],),
        ).fetchone()
    assert run_row["status"] == "ok"
    assert artifact_row["artifact_type"] == "company-intel"


def test_mem0_bridge_dump_exports_unified_snapshot_with_provenance(tmp_path, monkeypatch) -> None:
    module = _load_mem0_bridge_module()

    class FakeMem:
        def __init__(self, memories: list[dict[str, object]]) -> None:
            self._memories = memories

        def get_all(self, user_id: str) -> dict[str, object]:
            assert user_id == "rzmrn"
            return {"results": list(self._memories)}

    personal_memories = [
        {"id": "personal-1", "memory": "Personal memory", "metadata": {"project": "status"}},
    ]
    digest_memories = [
        {"id": "digest-1", "memory": "Digest memory", "metadata": {"category": "intel_digest"}},
    ]

    monkeypatch.setattr(module, "_get_client", lambda collection: FakeMem(personal_memories if collection == "personal" else digest_memories))

    output = tmp_path / "mem0-dump.json"
    module.cmd_dump(argparse.Namespace(output=str(output), collection="both"))

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["snapshot_type"] == "unified"
    assert payload["total_memories"] == 2
    assert payload["collection_totals"] == {"personal": 1, "digest": 1}
    assert payload["collections"]["personal"]["collection_name"] == "napaarnik_personal"
    assert payload["collections"]["digest"]["collection_name"] == "napaarnik_memory"
    assert {item["source_collection"] for item in payload["memories"]} == {"personal", "digest"}
    assert {item["source_collection_name"] for item in payload["memories"]} == {"napaarnik_personal", "napaarnik_memory"}


def test_mem0_bridge_search_dedupes_cross_collection_duplicates(monkeypatch, capsys) -> None:
    module = _load_mem0_bridge_module()

    class FakeMem:
        def __init__(self, memories: list[dict[str, object]]) -> None:
            self._memories = memories

        def search(self, query: str, user_id: str, limit: int, filters: dict[str, object] | None = None) -> dict[str, object]:
            assert query == "portfolio shipped"
            assert user_id == "rzmrn"
            assert limit == 10
            assert filters is None
            return {"results": list(self._memories)}

    duplicate = {
        "id": "shared-id",
        "memory": "Portfolio V1 shipped to production on rzmrn.com",
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


def test_mem0_bridge_sync_batch_processes_multiple_items(tmp_path, monkeypatch, capsys) -> None:
    module = _load_mem0_bridge_module()
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    class FakeMem:
        def __init__(self, label: str) -> None:
            self.label = label

        def add(self, text: str, user_id: str, metadata: dict[str, object] | None = None) -> dict[str, object]:
            assert user_id == "rzmrn"
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
                    {"event_id": "evt-2", "text": "Digest event", "category": "intel_digest"},
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

    loaded_automation = replace(
        loaded_automation,
        minimax=replace(loaded_automation.minimax, max_retries=2, retry_base_delay=0.0),
    )
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


def test_launchd_install_and_doctor(chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    install = install_launchd(
        loaded_manifest,
        loaded_automation,
        agent_dir=chronicle_sandbox.root / "LaunchAgents",
        runtime_dir=chronicle_sandbox.status_root / "runtime" / "launchd",
        log_dir=chronicle_sandbox.status_root / "logs" / "launchd",
        load_jobs=False,
    )
    assert len(install["installed"]) == 7

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
    mem0_job = next(job for job in doctor["jobs"] if job["label"] == "com.rzmrn.chronicle.mem0-dump")
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


def test_minimax_canary_records_recovery(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    config = config_from_manifest(loaded_manifest)
    monkeypatch.setattr("max_chronicle.native_automation.run_minimax_smoke", lambda automation: {
        "status": "error",
        "checked_at_utc": "2026-03-15T10:00:00Z",
        "base_url": automation.minimax.base_url,
        "model": automation.minimax.model,
        "error": "network",
    })
    first = run_automation_job(loaded_manifest, loaded_automation, job_name="minimax-canary", trigger_source="pytest")
    assert first["status"] == "failed_soft"

    monkeypatch.setattr("max_chronicle.native_automation.run_minimax_smoke", lambda automation: {
        "status": "ok",
        "checked_at_utc": "2026-03-15T11:00:00Z",
        "base_url": automation.minimax.base_url,
        "model": automation.minimax.model,
        "text": "OK",
    })
    second = run_minimax_canary(loaded_manifest, loaded_automation, trigger_source="pytest")
    assert second["status"] == "ok"
    assert second["event_id"] is not None

    with open_connection(config) as connection:
        row = connection.execute(
            "SELECT text, category FROM events WHERE id = ?",
            (second["event_id"],),
        ).fetchone()
    assert row["category"] == "maintenance"
    assert "recovered" in row["text"].casefold()


def test_install_git_hooks_updates_repo_config(chronicle_sandbox, loaded_automation) -> None:
    result = install_git_hooks(loaded_automation, repos=["rzmrn-portfolio"])
    assert result["status"] == "ok"
    hooks_path = subprocess.run(
        ["git", "-C", str(chronicle_sandbox.portfolio_repo), "config", "--get", "core.hooksPath"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert hooks_path == str(chronicle_sandbox.status_root / "git-hooks")


def test_end_to_end_acceptance(monkeypatch, chronicle_sandbox, loaded_manifest, loaded_automation) -> None:
    chronicle_sandbox.backup_root.mkdir(parents=True, exist_ok=True)
    digest_result = run_digest_hook(loaded_manifest, loaded_automation, trigger_source="pytest")
    local_date = _local_date_from_hook(digest_result, loaded_manifest["settings"]["timezone"])

    repo = chronicle_sandbox.digest_repo
    tracked = repo / "new.md"
    tracked.write_text("Tracked update\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "acceptance change"], check=True, capture_output=True, text=True)
    commit_sha = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    git_result = run_git_commit_hook(
        loaded_manifest,
        loaded_automation,
        repo_slug="intel-digest",
        commit_sha=commit_sha,
        repo_root=repo,
        trigger_source="pytest",
    )

    daily_capture = run_automation_job(loaded_manifest, loaded_automation, job_name="daily-capture", trigger_source="pytest")
    monkeypatch.setattr("max_chronicle.native_automation._call_minimax", lambda *args, **kwargs: "Acceptance narrative.")
    daybook = run_daybook(loaded_manifest, loaded_automation, target_date=local_date, trigger_source="pytest")
    backup = run_backup(loaded_manifest, loaded_automation, trigger_source="pytest", force=True)

    config = config_from_manifest(loaded_manifest)
    with open_connection(config) as connection:
        summary = database_summary(connection)

    assert digest_result["status"] == "stored"
    assert git_result["status"] == "stored"
    assert daily_capture["status"] == "ok"
    assert daybook["status"] in {"ok", "failed_soft"}
    assert backup["status"] == "ok"
    assert summary["events"] >= 3
    assert summary["snapshots"] >= 2
