from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import shlex
from pathlib import Path
import plistlib
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any
from zoneinfo import ZoneInfo

from .automation import AutomationConfig, ensure_automation_dirs, load_automation_config, load_env_file, repo_by_slug, repo_slug_for_path
from .config import default_automation_path
from .runtime_context import expand_path, read_json, utc_now
from .service import backfill_mem0_queue, build_freshness_audit, capture_runtime_snapshot, record_event, repair_mem0_state
from .store import (
    config_from_manifest,
    count_mem0_outbox,
    ensure_entity,
    fetch_automation_run,
    fetch_curation_run,
    fetch_events_between,
    fetch_hook_event,
    fetch_latest_automation_run,
    fetch_latest_backup_run,
    fetch_latest_curation_run,
    fetch_latest_hook_event,
    fetch_latest_projection_run,
    fetch_latest_snapshot,
    fetch_mem0_outbox_entries,
    finish_automation_run,
    finish_backup_run,
    finish_curation_run,
    has_snapshot_for_local_date,
    link_artifact,
    link_event_external_ref,
    open_connection,
    start_automation_run,
    start_backup_run,
    start_curation_run,
    store_artifact_from_path,
    store_artifact_text,
    update_event_mem0_state,
    upsert_hook_event,
    upsert_relation,
)


LAUNCHD_LABELS = {
    "daily_capture": "com.rzmrn.chronicle.daily-capture",
    "daybook": "com.rzmrn.chronicle.daybook",
    "mem0_dump": "com.rzmrn.chronicle.mem0-dump",
    "company_intel": "com.rzmrn.chronicle.company-intel",
    "minimax_canary": "com.rzmrn.chronicle.minimax-canary",
    "weekly_audit": "com.rzmrn.chronicle.weekly-audit",
    "backup": "com.rzmrn.chronicle.backup",
}

MEM0_OUTBOX_PRUNE_AFTER_DAYS = 30
MEM0_OUTBOX_PRUNE_BATCH = 5000
WAL_CHECKPOINT_THRESHOLD_BYTES = 64 * 1024 * 1024


def load_native_automation(path: Path | None = None) -> AutomationConfig:
    config = load_automation_config(path or default_automation_path())
    ensure_automation_dirs(config)
    return config


def _status_root(manifest: dict[str, Any]) -> Path:
    return expand_path(manifest["paths"]["status_root"])


def _chronicle_script(manifest: dict[str, Any]) -> Path:
    return _status_root(manifest) / "scripts" / "chronicle"


def _local_now(config) -> datetime:
    return datetime.now(ZoneInfo(config.timezone))


def _local_day_bounds(config, target_date: str | None = None) -> tuple[datetime, datetime]:
    tz = ZoneInfo(config.timezone)
    if target_date:
        base = datetime.fromisoformat(target_date).replace(tzinfo=tz)
    else:
        base = datetime.now(tz)
    start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(seconds=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _maybe_load_env(automation: AutomationConfig) -> dict[str, str]:
    return load_env_file(automation.env_file)


def _strip_think_tags(text: str) -> str:
    import re

    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


def _call_minimax(
    automation: AutomationConfig,
    *,
    system_prompt: str,
    user_content: str,
    max_tokens: int | None = None,
) -> str:
    _maybe_load_env(automation)
    api_key = os.environ.get("MINIMAX_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("MINIMAX_API_KEY is not set")

    from openai import OpenAI

    client = OpenAI(
        api_key=api_key,
        base_url=automation.minimax.base_url,
        timeout=automation.minimax.timeout_seconds,
        max_retries=0,
    )
    last_error: Exception | None = None
    for attempt in range(1, automation.minimax.max_retries + 1):
        try:
            response = client.chat.completions.create(
                model=automation.minimax.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content},
                ],
                max_tokens=max_tokens or automation.minimax.max_tokens,
                temperature=automation.minimax.temperature,
                extra_body={"reasoning_split": True},
            )
            raw = response.choices[0].message.content or ""
            return _strip_think_tags(raw)
        except Exception as exc:  # pragma: no cover - exercised with mocks in tests
            last_error = exc
            if attempt == automation.minimax.max_retries:
                break
            time.sleep(automation.minimax.retry_base_delay * (2 ** (attempt - 1)))
    raise RuntimeError(f"MiniMax API failed: {last_error}")


def _compose_mem0_text(text: str, why: str | None) -> str:
    return text if not why else f"{text} Reason: {why}"


def _mem0_command(manifest: dict[str, Any]) -> list[str]:
    status_root = _status_root(manifest)
    wrapper = status_root / "scripts" / "mem0"
    if wrapper.exists():
        return [str(wrapper)]

    bridge = expand_path(manifest["paths"]["mem0_bridge"])
    mem0_python = status_root / ".venv-mem0" / "bin" / "python3"
    if mem0_python.exists():
        return [str(mem0_python), str(bridge)]
    return [sys.executable, str(bridge)]


def _script_runner(script_path: Path) -> list[str]:
    uv = shutil.which("uv")
    if uv:
        return [uv, "run", str(script_path)]
    return [sys.executable, str(script_path)]


def _run_mem0_dump(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    trigger_source: str,
    automation_run_id: str | None = None,
) -> dict[str, Any]:
    _maybe_load_env(automation)
    config = config_from_manifest(manifest)
    dump_path = expand_path(manifest["paths"]["mem0_dump"])
    dump_path.parent.mkdir(parents=True, exist_ok=True)
    command = _mem0_command(manifest) + ["dump", "--output", str(dump_path)]
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        return {
            "status": "failed",
            "command": command,
            "dump_path": str(dump_path),
            "error": stderr or stdout or f"exit_{proc.returncode}",
            "trigger_source": trigger_source,
        }

    dump_payload = read_json(dump_path) or {}
    memories = dump_payload.get("memories")
    memory_count = dump_payload.get("total_memories")
    if memory_count is None and isinstance(memories, list):
        memory_count = len(memories)

    observed_at_utc = utc_now()
    artifact = store_artifact_from_path(
        config,
        source_path=dump_path,
        artifact_type="mem0-dump",
        observed_at_utc=observed_at_utc,
        entity_type="system",
        entity_name="mem0",
        metadata={
            "job_name": "mem0-dump",
            "trigger_source": trigger_source,
            "memory_count": memory_count,
        },
    )
    latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)
    if artifact is not None and automation_run_id is not None:
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="automation_run",
            target_id=automation_run_id,
            link_role="documents",
            metadata={"job_name": "mem0-dump"},
        )
    if artifact is not None and latest_snapshot is not None:
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="snapshot",
            target_id=latest_snapshot["id"],
            link_role="documents",
            metadata={"job_name": "mem0-dump"},
        )

    return {
        "status": "ok",
        "command": command,
        "dump_path": str(dump_path),
        "artifact_id": artifact["id"] if artifact else None,
        "snapshot_id": latest_snapshot["id"] if latest_snapshot else None,
        "memory_count": memory_count,
        "trigger_source": trigger_source,
        "stdout": stdout,
    }


def _company_intel_output_path(manifest: dict[str, Any]) -> Path:
    explicit = (manifest.get("paths") or {}).get("company_intel_json")
    if explicit:
        return expand_path(explicit)
    return _status_root(manifest) / "company-intel.json"


def _run_company_intel_refresh(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    trigger_source: str,
    automation_run_id: str | None = None,
) -> dict[str, Any]:
    _maybe_load_env(automation)
    config = config_from_manifest(manifest)
    script_path = _status_root(manifest) / "scripts" / "company_intel_bridge.py"
    output_path = _company_intel_output_path(manifest)
    if not script_path.exists():
        return {
            "status": "failed",
            "script_path": str(script_path),
            "company_intel_path": str(output_path),
            "error": "script_missing",
            "trigger_source": trigger_source,
        }

    command = _script_runner(script_path)
    proc = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if proc.returncode != 0:
        return {
            "status": "failed",
            "command": command,
            "script_path": str(script_path),
            "company_intel_path": str(output_path),
            "error": stderr or stdout or f"exit_{proc.returncode}",
            "trigger_source": trigger_source,
        }
    if not output_path.exists():
        return {
            "status": "failed",
            "command": command,
            "script_path": str(script_path),
            "company_intel_path": str(output_path),
            "error": "output_missing",
            "trigger_source": trigger_source,
            "stdout": stdout,
        }

    payload = read_json(output_path) or {}
    company_count = len(payload.get("companies") or {})
    industry_event_count = len(payload.get("industry_context") or [])
    observed_at_utc = utc_now()
    artifact = store_artifact_from_path(
        config,
        source_path=output_path,
        artifact_type="company-intel",
        observed_at_utc=observed_at_utc,
        entity_type="system",
        entity_name="company-intel",
        metadata={
            "job_name": "company-intel",
            "trigger_source": trigger_source,
            "company_count": company_count,
            "industry_event_count": industry_event_count,
        },
    )
    latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)
    if artifact is not None and automation_run_id is not None:
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="automation_run",
            target_id=automation_run_id,
            link_role="documents",
            metadata={"job_name": "company-intel"},
        )
    if artifact is not None and latest_snapshot is not None:
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="snapshot",
            target_id=latest_snapshot["id"],
            link_role="documents",
            metadata={"job_name": "company-intel"},
        )

    return {
        "status": "ok",
        "command": command,
        "script_path": str(script_path),
        "company_intel_path": str(output_path),
        "artifact_id": artifact["id"] if artifact else None,
        "snapshot_id": latest_snapshot["id"] if latest_snapshot else None,
        "company_count": company_count,
        "industry_event_count": industry_event_count,
        "trigger_source": trigger_source,
        "stdout": stdout,
    }


def _legacy_surface_findings(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    status_root = _status_root(manifest)
    manifest_path = status_root / "SSOT_MANIFEST.toml"
    active_source_ids = sorted({source_id for domain in manifest.get("domains", []) for source_id in domain.get("source_ids", [])})
    deprecated_source_names = {
        "CODEX_MEM0_PROTOCOL.md": "deprecated_source",
        "POST_MIGRATION_CHECKLIST.md": "deprecated_source",
    }
    deprecated_refs = {
        "memory_compaction.py": "deprecated_upkeep_tool",
        "memory_healthcheck.py": "deprecated_upkeep_tool",
        "POST_MIGRATION_CHECKLIST.md": "deprecated_checklist",
    }
    findings: list[dict[str, Any]] = []

    for source_id in active_source_ids:
        source = manifest["source_map"][source_id]
        source_path = expand_path(source["path"])
        basename = source_path.name
        if basename in deprecated_source_names:
            findings.append(
                {
                    "kind": deprecated_source_names[basename],
                    "path": str(source_path),
                    "detail": f"Active source `{source_id}` still points to deprecated document `{basename}`.",
                }
            )
        if not source_path.exists():
            continue
        text = source_path.read_text(encoding="utf-8", errors="replace")
        for pattern, kind in deprecated_refs.items():
            if pattern in text:
                findings.append(
                    {
                        "kind": kind,
                        "path": str(source_path),
                        "detail": f"Active source `{source_id}` still references `{pattern}`.",
                    }
                )

    if manifest_path.exists():
        manifest_text = manifest_path.read_text(encoding="utf-8", errors="replace")
        for pattern, kind in deprecated_refs.items():
            if pattern in manifest_text:
                findings.append(
                    {
                        "kind": kind,
                        "path": str(manifest_path),
                        "detail": f"Manifest still references `{pattern}`.",
                    }
                )

    unique: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for finding in findings:
        key = (finding["kind"], finding["path"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(finding)
    return unique


def _add_to_live_mem0(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    text: str,
    category: str | None,
    project: str | None,
) -> dict[str, Any]:
    _maybe_load_env(automation)
    command = _mem0_command(manifest) + ["add", text]
    if category:
        command.extend(["--category", category])
    if project:
        command.extend(["--project", project])

    env = os.environ.copy()
    max_retries = max(int(getattr(automation.minimax, "max_retries", 1) or 1), 1)
    retry_base_delay = float(getattr(automation.minimax, "retry_base_delay", 1.0) or 1.0)
    timeout_seconds = max(float(getattr(automation.minimax, "timeout_seconds", 45.0) or 45.0), 5.0)
    last_error = ""

    for attempt in range(1, max_retries + 1):
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                env=env,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            last_error = f"mem0 add timed out after {timeout_seconds:.0f}s"
            if attempt == max_retries:
                break
            time.sleep(retry_base_delay * (2 ** (attempt - 1)))
            continue
        raw = (proc.stdout or "").strip()
        error = (proc.stderr or raw or "").strip()
        if proc.returncode == 0:
            return {"ok": True, "raw": raw, "command": command, "attempts": attempt}

        last_error = error
        normalized = error.casefold()
        transient = any(
            marker in normalized
            for marker in (
                "503",
                "unavailable",
                "timeout",
                "timed out",
                "rate limit",
                "temporar",
                "connection reset",
                "deadline exceeded",
            )
        )
        if not transient or attempt == max_retries:
            break
        time.sleep(retry_base_delay * (2 ** (attempt - 1)))

    return {"ok": False, "error": last_error, "command": command, "attempts": max_retries}


def _add_to_live_mem0_batch(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    if not items:
        return {"ok": True, "payload": {"status": "ok", "processed": []}}

    _maybe_load_env(automation)
    max_retries = max(int(getattr(automation.minimax, "max_retries", 1) or 1), 1)
    retry_base_delay = float(getattr(automation.minimax, "retry_base_delay", 1.0) or 1.0)
    timeout_seconds = max(float(getattr(automation.minimax, "timeout_seconds", 45.0) or 45.0), 5.0)
    batch_timeout_seconds = max(min(timeout_seconds * max(len(items), 1), 120.0), 30.0)

    temp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as handle:
            json.dump({"items": items}, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            temp_path = handle.name

        command = _mem0_command(manifest) + [
            "sync-batch",
            "--input",
            temp_path,
            "--max-retries",
            str(max_retries),
            "--retry-base-delay",
            str(retry_base_delay),
        ]
        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                env=os.environ.copy(),
                timeout=batch_timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            return {
                "ok": False,
                "error": f"sync-batch timed out after {batch_timeout_seconds:.0f}s",
                "command": command,
            }
        raw = (proc.stdout or "").strip()
        error = (proc.stderr or raw or "").strip()
        if proc.returncode != 0:
            return {"ok": False, "error": error, "command": command, "raw": raw}
        try:
            payload = json.loads(raw or "{}")
        except json.JSONDecodeError as exc:
            return {
                "ok": False,
                "error": f"Invalid sync-batch JSON output: {exc}",
                "command": command,
                "raw": raw,
            }
        return {"ok": True, "payload": payload, "command": command, "raw": raw}
    finally:
        if temp_path:
            Path(temp_path).unlink(missing_ok=True)


def sync_mem0_outbox(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    limit: int | None = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    batch_limit = limit or automation.guards.mem0_sync_batch_size
    entries = fetch_mem0_outbox_entries(config, limit=batch_limit)
    synced = 0
    failed = 0
    skipped = 0
    processed: list[dict[str, Any]] = []
    batch_ready: list[dict[str, Any]] = []

    for entry in entries:
        memory_guard = entry.get("payload", {}).get("memory_guard")
        if isinstance(memory_guard, dict) and memory_guard.get("verdict") == "local_only":
            update_event_mem0_state(
                config,
                event_id=entry["event_id"],
                mem0_status="skipped",
                mem0_error="guarded_local_only",
                attempted=False,
            )
            skipped += 1
            processed.append({"event_id": entry["event_id"], "status": "skipped", "reason": "guarded_local_only"})
            continue
        text = _compose_mem0_text(entry.get("text") or "", entry.get("why"))
        if not text.strip():
            update_event_mem0_state(
                config,
                event_id=entry["event_id"],
                mem0_status="skipped",
                mem0_error="empty_text",
                attempted=False,
            )
            skipped += 1
            processed.append({"event_id": entry["event_id"], "status": "skipped", "reason": "empty_text"})
            continue
        batch_ready.append(
            {
                "event_id": entry["event_id"],
                "text": text,
                "category": entry.get("category"),
                "project": entry.get("project"),
            }
        )

    def handle_result(event_id: str, result: dict[str, Any]) -> None:
        nonlocal synced, failed
        if result.get("ok"):
            update_event_mem0_state(
                config,
                event_id=event_id,
                mem0_status="stored",
                mem0_error=None,
                mem0_raw=result.get("raw"),
                mem0_synced_at=utc_now(),
                attempted=True,
            )
            synced += 1
            processed.append({"event_id": event_id, "status": "synced"})
            return

        update_event_mem0_state(
            config,
            event_id=event_id,
            mem0_status="failed",
            mem0_error=result.get("error"),
            attempted=True,
        )
        failed += 1
        processed.append(
            {
                "event_id": event_id,
                "status": "failed",
                "error": result.get("error"),
            }
        )

    chunk_size = max(1, min(batch_limit, 5))
    for offset in range(0, len(batch_ready), chunk_size):
        chunk = batch_ready[offset : offset + chunk_size]
        batch_result = _add_to_live_mem0_batch(
            manifest,
            automation,
            items=chunk,
        )
        if batch_result["ok"]:
            result_map = {
                item.get("event_id"): item
                for item in batch_result["payload"].get("processed", [])
                if item.get("event_id")
            }
            for item in chunk:
                event_id = item["event_id"]
                result = result_map.get(event_id)
                if result is None:
                    handle_result(
                        event_id,
                        {
                            "ok": False,
                            "error": "sync-batch returned no result for event",
                        },
                    )
                    continue
                handle_result(event_id, result)
            continue

        for item in chunk:
            result = _add_to_live_mem0(
                manifest,
                automation,
                text=item["text"],
                category=item.get("category"),
                project=item.get("project"),
            )
            handle_result(item["event_id"], result)

    return {
        "status": "ok" if failed == 0 else "issues",
        "trigger_source": trigger_source,
        "requested_limit": batch_limit,
        "seen": len(entries),
        "synced": synced,
        "failed": failed,
        "skipped": skipped,
        "processed": processed,
        "remaining_pending": count_mem0_outbox(config, status="pending"),
        "remaining_failed": count_mem0_outbox(config, status="failed"),
    }


def _latest_digest_log(manifest: dict[str, Any]) -> Path | None:
    digest_repo = expand_path(manifest["paths"]["intel_digest_repo"])
    logs_dir = digest_repo / "logs"
    candidates = sorted(logs_dir.glob("digest_*.log"), key=lambda item: item.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _git_root(path: Path) -> Path:
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip()).resolve()


def _changed_files_for_commit(repo_root: Path, commit_sha: str) -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "diff-tree", "--no-commit-id", "--name-only", "-r", commit_sha],
        capture_output=True,
        text=True,
        check=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _infer_repo_slugs(automation: AutomationConfig, repo_root: Path, changed_files: list[str]) -> list[str]:
    repo_root = repo_root.resolve()
    touched: list[str] = []
    for repo in automation.repos:
        try:
            relative_repo = repo.path.resolve().relative_to(repo_root)
        except ValueError:
            continue
        prefix = str(relative_repo).rstrip("/") + "/"
        if prefix == "/":
            prefix = ""
        if any(path == str(relative_repo) or path.startswith(prefix) for path in changed_files):
            touched.append(repo.slug)
    return touched


def _artifact_for_event(
    manifest: dict[str, Any],
    *,
    event_id: str,
    source_path: Path,
    artifact_type: str,
    observed_at_utc: str,
    entity_type: str,
    entity_name: str,
    link_role: str = "source",
) -> dict[str, Any] | None:
    config = config_from_manifest(manifest)
    artifact = store_artifact_from_path(
        config,
        source_path=source_path,
        artifact_type=artifact_type,
        observed_at_utc=observed_at_utc,
        entity_type=entity_type,
        entity_name=entity_name,
        metadata={"event_id": event_id},
    )
    if artifact is None:
        return None
    link_artifact(
        config,
        artifact_id=artifact["id"],
        target_type="event",
        target_id=event_id,
        link_role=link_role,
        metadata={"artifact_type": artifact_type},
    )
    return artifact


def _store_text_artifact(
    manifest: dict[str, Any],
    *,
    target_type: str,
    target_id: str,
    artifact_type: str,
    content: str,
    filename: str,
    observed_at_utc: str,
    entity_type: str | None = None,
    entity_name: str | None = None,
    link_role: str = "generated",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    artifact = store_artifact_text(
        config,
        artifact_type=artifact_type,
        content=content,
        filename=filename,
        observed_at_utc=observed_at_utc,
        entity_type=entity_type,
        entity_name=entity_name,
        metadata={target_type: target_id},
    )
    link_artifact(
        config,
        artifact_id=artifact["id"],
        target_type=target_type,
        target_id=target_id,
        link_role=link_role,
        metadata={"artifact_type": artifact_type},
    )
    return artifact


def _render_deterministic_daybook(
    *,
    local_date: str,
    events: list[dict[str, Any]],
    snapshot: dict[str, Any] | None,
    audit_status: dict[str, Any] | None,
    backup_status: dict[str, Any] | None,
) -> str:
    decisions = [event for event in events if event.get("why")]
    blockers = [event for event in events if "block" in (event.get("category") or "") or "block" in event.get("text", "").casefold()]
    repos = sorted({event.get("project") for event in events if event.get("project")})
    world_headlines = ((snapshot or {}).get("digest") or {}).get("world_headlines") or []

    lines = [
        f"# Chronicle Daybook — {local_date}",
        "",
        "## Key Events",
    ]
    if events:
        for event in events[-12:]:
            lines.append(f"- {event['recorded_at']} | {event.get('category') or 'note'} | {event['text']}")
    else:
        lines.append("- No durable events recorded.")

    lines.extend(["", "## Decisions With Why"])
    if decisions:
        for event in decisions[-8:]:
            lines.append(f"- {event['text']}")
            lines.append(f"  why: {event['why']}")
    else:
        lines.append("- No explicit decisions recorded.")

    lines.extend(["", "## Blockers"])
    if blockers:
        for event in blockers[-8:]:
            lines.append(f"- {event['text']}")
    else:
        lines.append("- No blockers recorded.")

    lines.extend(["", "## World Context"])
    if world_headlines:
        for item in world_headlines[:4]:
            lines.append(f"- {item}")
    else:
        lines.append("- No world context available in the latest snapshot.")

    lines.extend(["", "## Repos Touched"])
    if repos:
        for repo in repos:
            lines.append(f"- {repo}")
    else:
        lines.append("- No tracked repos recorded.")

    lines.extend(["", "## Automation Health"])
    if backup_status:
        lines.append(f"- Backup: {backup_status['status']}")
    else:
        lines.append("- Backup: no backup run recorded.")
    if audit_status:
        lines.append(f"- Audit: {audit_status['status']}")
    else:
        lines.append("- Audit: no audit run recorded.")

    return "\n".join(lines) + "\n"


def _compose_daybook(
    automation: AutomationConfig,
    *,
    local_date: str,
    events: list[dict[str, Any]],
    snapshot: dict[str, Any] | None,
    audit_status: dict[str, Any] | None,
    backup_status: dict[str, Any] | None,
) -> tuple[str, str, str | None]:
    deterministic = _render_deterministic_daybook(
        local_date=local_date,
        events=events,
        snapshot=snapshot,
        audit_status=audit_status,
        backup_status=backup_status,
    )
    prompt_payload = {
        "local_date": local_date,
        "events": events[-20:],
        "world_headlines": ((snapshot or {}).get("digest") or {}).get("world_headlines") or [],
        "backup_status": backup_status,
        "audit_status": audit_status,
    }
    prompt_json = json.dumps(prompt_payload, ensure_ascii=False, indent=2)
    narrative = ""
    try:
        narrative = _call_minimax(
            automation,
            system_prompt=(
                "You are a precise historian. Write concise markdown for a personal daybook. "
                "Do not invent facts. Explain what changed, why it mattered, and what remained blocked."
            ),
            user_content=prompt_json,
            max_tokens=1600,
        )
    except Exception as exc:
        return deterministic, "failed_soft", str(exc)

    lines = [deterministic.rstrip(), "", "## Narrative", "", narrative.strip(), ""]
    return "\n".join(lines), "ok", None


def run_digest_hook(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    status_path: Path | None = None,
    synthesis_path: Path | None = None,
    previous_summary_path: Path | None = None,
    log_path: Path | None = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    status_path = status_path or expand_path(manifest["paths"]["digest_status_json"])
    synthesis_path = synthesis_path or expand_path(manifest["paths"]["digest_synthesis_md"])
    previous_summary_path = previous_summary_path or expand_path(manifest["paths"]["digest_previous_summary"])
    log_path = log_path or _latest_digest_log(manifest)

    status_payload = read_json(status_path) or {}
    payload = {
        "status_path": str(status_path),
        "synthesis_path": str(synthesis_path),
        "previous_summary_path": str(previous_summary_path),
        "log_path": str(log_path) if log_path else None,
        "status": status_payload.get("status"),
        "deployed": status_payload.get("deployed"),
        "timestamp": status_payload.get("timestamp"),
    }
    dedupe_source = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    dedupe_key = hashlib.sha256(dedupe_source.encode("utf-8")).hexdigest()
    existing = fetch_hook_event(config, hook_type="digest-run", dedupe_key=dedupe_key)
    if existing:
        return {
            "status": "existing",
            "hook_event": existing,
            "event_id": existing.get("event_id"),
            "snapshot_id": existing.get("snapshot_id"),
        }

    latest_previous = fetch_latest_hook_event(config, hook_type="digest-run")
    previous_payload = latest_previous.get("payload") if latest_previous else {}
    previous_status = previous_payload.get("status")
    previous_deployed = previous_payload.get("deployed")
    current_status = status_payload.get("status", "unknown")
    current_deployed = status_payload.get("deployed")
    state_changed = previous_status not in {None, current_status} or previous_deployed != current_deployed

    summary = (
        f"Digest run status={current_status}, sources={status_payload.get('sources_ok', 0)}/"
        f"{status_payload.get('sources_total', 0)}, items={status_payload.get('items', 0)}, "
        f"deployed={current_deployed}."
    )
    source_files = [str(status_path), str(synthesis_path), str(previous_summary_path)]
    if log_path:
        source_files.append(str(log_path))

    stored = record_event(
        manifest,
        {
            "agent": "chronicle",
            "domain": "global",
            "category": "digest_run",
            "project": "intel-digest",
            "text": summary,
            "why": "Digest completed a new pipeline run and updated the latest world-context outputs.",
            "source_files": source_files,
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
            "skip_generic_source_archives": True,
        },
        append_compat=True,
        source_kind="chronicle_hook.digest_run",
        imported_from=f"chronicle.hook.digest-run:{trigger_source}",
    )
    link_event_external_ref(
        config,
        event_id=stored["id"],
        ref_type="digest_run",
        ref_value=status_payload.get("timestamp") or dedupe_key,
        metadata={"dedupe_key": dedupe_key},
    )

    _artifact_for_event(
        manifest,
        event_id=stored["id"],
        source_path=status_path,
        artifact_type="digest-run-status",
        observed_at_utc=stored["recorded_at"],
        entity_type="system",
        entity_name="digest",
    )
    if synthesis_path.exists():
        _artifact_for_event(
            manifest,
            event_id=stored["id"],
            source_path=synthesis_path,
            artifact_type="digest-run-synthesis",
            observed_at_utc=stored["recorded_at"],
            entity_type="system",
            entity_name="digest",
        )
    if previous_summary_path.exists():
        _artifact_for_event(
            manifest,
            event_id=stored["id"],
            source_path=previous_summary_path,
            artifact_type="digest-run-previous-summary",
            observed_at_utc=stored["recorded_at"],
            entity_type="system",
            entity_name="digest",
        )
    if log_path and log_path.exists():
        _artifact_for_event(
            manifest,
            event_id=stored["id"],
            source_path=log_path,
            artifact_type="digest-run-log",
            observed_at_utc=stored["recorded_at"],
            entity_type="system",
            entity_name="digest",
        )

    local_date = datetime.now(ZoneInfo(config.timezone)).strftime("%Y-%m-%d")
    should_capture = state_changed or not has_snapshot_for_local_date(config, domain="global", local_date=local_date)
    snapshot = None
    if should_capture:
        snapshot = capture_runtime_snapshot(
            manifest,
            domain_id="global",
            agent="chronicle-hook",
            title="Digest state transition",
            focus="digest automation",
            append_compat=True,
            render_generated=True,
        )

    hook_row = upsert_hook_event(
        config,
        hook_type="digest-run",
        dedupe_key=dedupe_key,
        source_ref=status_payload.get("timestamp"),
        status="handled",
        payload=payload,
        event_id=stored["id"],
        snapshot_id=snapshot["id"] if snapshot else None,
    )
    return {
        "status": "stored",
        "event_id": stored["id"],
        "snapshot_id": snapshot["id"] if snapshot else None,
        "state_changed": state_changed,
        "hook_event": hook_row,
    }


def run_git_commit_hook(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    repo_slug: str | None = None,
    commit_sha: str,
    repo_root: Path | None = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    resolved_root = repo_root.expanduser().resolve() if repo_root else repo_by_slug(automation, repo_slug or "status").path
    git_root = _git_root(resolved_root)
    changed_files = _changed_files_for_commit(git_root, commit_sha)
    touched_repos = [repo_slug] if repo_slug else _infer_repo_slugs(automation, git_root, changed_files)
    resolved_slug = touched_repos[0] if touched_repos else (repo_slug or repo_slug_for_path(automation, resolved_root))
    dedupe_key = f"{resolved_slug}:{commit_sha}"
    existing = fetch_hook_event(config, hook_type="git-commit", dedupe_key=dedupe_key)
    if existing:
        return {
            "status": "existing",
            "hook_event": existing,
            "event_id": existing.get("event_id"),
        }

    pretty = subprocess.run(
        ["git", "-C", str(git_root), "show", "--stat", "--format=fuller", "--no-patch", commit_sha],
        capture_output=True,
        text=True,
        check=True,
    )
    message = subprocess.run(
        ["git", "-C", str(git_root), "log", "-1", "--pretty=%s", commit_sha],
        capture_output=True,
        text=True,
        check=True,
    )
    summary = message.stdout.strip() or f"Commit {commit_sha[:12]}"

    stored = record_event(
        manifest,
        {
            "agent": "chronicle",
            "domain": "global",
            "category": "git_commit",
            "project": resolved_slug,
            "text": f"{resolved_slug} commit {commit_sha[:12]}: {summary}",
            "why": "A tracked repository recorded a new commit that should remain part of the Chronicle timeline.",
            "source_files": [],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_hook.git_commit",
        imported_from=f"chronicle.hook.git-commit:{trigger_source}",
    )

    commit_entity_id, _ = ensure_entity(
        config,
        entity_type="commit",
        slug_or_name=commit_sha,
        name=f"{resolved_slug} {commit_sha[:12]}",
        metadata={"repo": resolved_slug, "commit_sha": commit_sha},
    )
    project_entity_id, _ = ensure_entity(
        config,
        entity_type="project",
        slug_or_name=resolved_slug,
        metadata={"repo_path": str(git_root)},
    )
    relation_targets = touched_repos or [resolved_slug]
    for target_slug in relation_targets:
        upsert_relation(
            config,
            from_entity_type="commit",
            from_entity_name=commit_sha,
            relation_type="touches_repo",
            to_entity_type="project",
            to_entity_name=target_slug,
            rationale=f"Commit {commit_sha[:12]} touched tracked repo {target_slug}.",
            metadata={"event_id": stored["id"], "project_entity_id": project_entity_id, "commit_entity_id": commit_entity_id},
        )
    link_event_external_ref(
        config,
        event_id=stored["id"],
        ref_type="git_commit",
        ref_value=dedupe_key,
        metadata={"repo": resolved_slug, "commit_sha": commit_sha, "changed_files": changed_files, "touched_repos": relation_targets},
    )

    artifact = _store_text_artifact(
        manifest,
        target_type="event",
        target_id=stored["id"],
        artifact_type="git-commit-summary",
        content=pretty.stdout + "\nChanged files:\n" + "\n".join(changed_files) + "\n",
        filename=f"{resolved_slug}-{commit_sha[:12]}.txt",
        observed_at_utc=stored["recorded_at"],
        entity_type="commit",
        entity_name=commit_sha,
        link_role="source",
    )

    hook_row = upsert_hook_event(
        config,
        hook_type="git-commit",
        dedupe_key=dedupe_key,
        source_ref=str(git_root),
        status="handled",
        payload={"repo_slug": resolved_slug, "commit_sha": commit_sha, "artifact_id": artifact["id"], "touched_repos": relation_targets},
        event_id=stored["id"],
        snapshot_id=None,
    )
    return {"status": "stored", "event_id": stored["id"], "hook_event": hook_row}


def run_daybook(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    target_date: str | None = None,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    start_dt, end_dt = _local_day_bounds(config, target_date)
    local_date = start_dt.astimezone(ZoneInfo(config.timezone)).strftime("%Y-%m-%d")
    run_id, created = start_curation_run(
        config,
        curation_type="daybook",
        run_key=local_date,
        model_name=automation.minimax.model,
        prompt_sha256=None,
        payload={"trigger_source": trigger_source},
    )
    if not created:
        existing = fetch_curation_run(config, curation_type="daybook", run_key=local_date)
        return {"status": "existing", "run": existing}

    events = fetch_events_between(
        config,
        start_utc=start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_utc=end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        limit=1000,
    )
    latest_backup = fetch_latest_backup_run(config, successful_only=True)
    latest_audit = fetch_latest_curation_run(config, curation_type="weekly_audit")
    snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)

    if not events:
        finish_curation_run(config, run_id=run_id, status="skipped", notes="No durable delta for this date.")
        return {"status": "skipped", "run_id": run_id, "reason": "no_events"}

    content, llm_status, llm_error = _compose_daybook(
        automation,
        local_date=local_date,
        events=events,
        snapshot=snapshot,
        audit_status=latest_audit,
        backup_status=latest_backup,
    )

    daybook_dir = automation.daybook_dir / local_date[:4]
    daybook_dir.mkdir(parents=True, exist_ok=True)
    daybook_path = daybook_dir / f"{local_date}.md"
    daybook_path.write_text(content, encoding="utf-8")

    artifact = _store_text_artifact(
        manifest,
        target_type="snapshot" if snapshot else "event",
        target_id=snapshot["id"] if snapshot else run_id,
        artifact_type="daybook",
        content=content,
        filename=f"{local_date}.md",
        observed_at_utc=utc_now(),
        entity_type="system",
        entity_name="chronicle",
        link_role="generated",
    )

    summary_event = record_event(
        manifest,
        {
            "agent": "chronicle",
            "domain": "global",
            "category": "daily_summary",
            "project": "status",
            "text": f"Chronicle daybook generated for {local_date}.",
            "why": "Summarize the day's durable changes into a readable historical record.",
            "source_files": [str(daybook_path)],
            "mem0_status": "off",
            "mem0_error": None,
            "mem0_raw": None,
        },
        append_compat=True,
        source_kind="chronicle_curator.daybook",
        imported_from=f"chronicle.curate.daybook:{trigger_source}",
    )
    link_artifact(
        config,
        artifact_id=artifact["id"],
        target_type="event",
        target_id=summary_event["id"],
        link_role="summarizes",
        metadata={"local_date": local_date},
    )
    finish_curation_run(
        config,
        run_id=run_id,
        status="ok" if llm_status == "ok" else "failed_soft",
        payload={
            "local_date": local_date,
            "event_count": len(events),
            "daybook_path": str(daybook_path),
            "llm_error": llm_error,
        },
        artifact_id=artifact["id"],
        event_id=summary_event["id"],
        notes=llm_error or llm_status,
    )
    return {
        "status": "ok" if llm_status == "ok" else "failed_soft",
        "run_id": run_id,
        "event_id": summary_event["id"],
        "artifact_id": artifact["id"],
        "path": str(daybook_path),
    }


def run_minimax_smoke(
    automation: AutomationConfig,
    *,
    prompt: str = "Reply with OK only.",
) -> dict[str, Any]:
    _maybe_load_env(automation)
    started_at = utc_now()
    try:
        text = _call_minimax(
            automation,
            system_prompt="You are a concise healthcheck assistant.",
            user_content=prompt,
            max_tokens=256,
        )
        return {
            "status": "ok",
            "checked_at_utc": started_at,
            "base_url": automation.minimax.base_url,
            "model": automation.minimax.model,
            "text": text,
        }
    except Exception as exc:
        return {
            "status": "error",
            "checked_at_utc": started_at,
            "base_url": automation.minimax.base_url,
            "model": automation.minimax.model,
            "error": str(exc),
        }


def run_minimax_canary(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    trigger_source: str = "manual",
    previous_health: str | None = None,
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    local_now = _local_now(config)
    run_key = local_now.strftime("%Y-%m-%d")
    previous = None if previous_health is not None else fetch_latest_automation_run(config, job_name="minimax-canary")
    latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)
    result = run_minimax_smoke(automation)
    details = {
        "health_status": result["status"],
        "checked_at_utc": result["checked_at_utc"],
        "base_url": result["base_url"],
        "model": result["model"],
        "trigger_source": trigger_source,
    }
    if result["status"] == "ok":
        details["text"] = result.get("text")
    else:
        details["error"] = result.get("error")

    artifact = _store_text_artifact(
        manifest,
        target_type="snapshot" if latest_snapshot else "automation_run",
        target_id=latest_snapshot["id"] if latest_snapshot else run_key,
        artifact_type="minimax-health-check",
        content=json.dumps(details, ensure_ascii=False, indent=2) + "\n",
        filename=f"minimax-canary-{run_key}.json",
        observed_at_utc=utc_now(),
        entity_type="system",
        entity_name="chronicle",
        link_role="generated",
    )

    resolved_previous_health = previous_health
    if resolved_previous_health is None and previous:
        resolved_previous_health = previous.get("details", {}).get("health_status") or previous.get("status")

    event_id = None
    if result["status"] != "ok":
        event = record_event(
            manifest,
            {
                "agent": "chronicle",
                "domain": "memory",
                "category": "anomaly",
                "project": "status",
                "text": "MiniMax canary failed during Chronicle automation.",
                "why": result.get("error") or "MiniMax completion check returned an error.",
                "source_files": [],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
            },
            append_compat=True,
            source_kind="chronicle_canary.minimax",
            imported_from=f"chronicle.canary.minimax:{trigger_source}",
        )
        event_id = event["id"]
    elif resolved_previous_health and resolved_previous_health != "ok":
        event = record_event(
            manifest,
            {
                "agent": "chronicle",
                "domain": "memory",
                "category": "maintenance",
                "project": "status",
                "text": "MiniMax canary recovered and returned to healthy status.",
                "why": "The previous recorded MiniMax canary did not complete successfully, and the provider is now reachable again.",
                "source_files": [],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
            },
            append_compat=True,
            source_kind="chronicle_canary.minimax",
            imported_from=f"chronicle.canary.minimax:{trigger_source}",
        )
        event_id = event["id"]

    if event_id:
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="event",
            target_id=event_id,
            link_role="documents",
            metadata={"job_name": "minimax-canary"},
        )

    return {
        "status": "ok" if result["status"] == "ok" else "failed_soft",
        "artifact_id": artifact["id"],
        "event_id": event_id,
        "snapshot_id": latest_snapshot["id"] if latest_snapshot else None,
        "health": result,
        "details": details,
    }


def _copy_tree_incremental(source_root: Path, target_root: Path) -> tuple[int, int, dict[str, int]]:
    copied_files = 0
    copied_bytes = 0
    strategy = {"hardlinked": 0, "copied": 0, "skipped_existing": 0}
    if not source_root.exists():
        return copied_files, copied_bytes, strategy

    for source_path in source_root.rglob("*"):
        if not source_path.is_file():
            continue
        relative = source_path.relative_to(source_root)
        target_path = target_root / relative
        target_path.parent.mkdir(parents=True, exist_ok=True)
        source_size = source_path.stat().st_size
        if target_path.exists():
            if target_path.stat().st_size == source_size:
                strategy["skipped_existing"] += 1
                continue
            if target_path.is_file():
                target_path.unlink()
        try:
            os.link(source_path, target_path)
            strategy["hardlinked"] += 1
        except OSError:
            shutil.copy2(source_path, target_path)
            copied_files += 1
            copied_bytes += source_size
            strategy["copied"] += 1
    return copied_files, copied_bytes, strategy


def _prune_mem0_outbox(config) -> dict[str, Any]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=MEM0_OUTBOX_PRUNE_AFTER_DAYS)
    cutoff_utc = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    with open_connection(config) as connection:
        retained_pending = int(
            connection.execute("SELECT COUNT(*) FROM mem0_outbox WHERE status = 'pending'").fetchone()[0]
        )
        retained_failed = int(
            connection.execute("SELECT COUNT(*) FROM mem0_outbox WHERE status = 'failed'").fetchone()[0]
        )
        prunable_count = int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM mem0_outbox
                WHERE status IN ('synced', 'skipped')
                  AND COALESCE(synced_at_utc, created_at_utc) < ?
                """,
                (cutoff_utc,),
            ).fetchone()[0]
        )

        rows = connection.execute(
            """
            SELECT id, event_id, status
            FROM mem0_outbox
            WHERE status IN ('synced', 'skipped')
              AND COALESCE(synced_at_utc, created_at_utc) < ?
            ORDER BY COALESCE(synced_at_utc, created_at_utc) ASC
            LIMIT ?
            """,
            (cutoff_utc, MEM0_OUTBOX_PRUNE_BATCH),
        ).fetchall()
        if not rows:
            return {
                "pruned": 0,
                "cutoff_utc": cutoff_utc,
                "skipped": False,
                "reason": "nothing_to_prune",
                "prunable": prunable_count,
                "retained_pending": retained_pending,
                "retained_failed": retained_failed,
            }

        with connection:
            connection.executemany(
                "DELETE FROM mem0_outbox WHERE id = ?",
                ((row["id"],) for row in rows),
            )

    pruned_by_status = Counter(row["status"] for row in rows)
    return {
        "pruned": len(rows),
        "cutoff_utc": cutoff_utc,
        "skipped": False,
        "reason": "pruned_retained_rows",
        "prunable": prunable_count,
        "retained_pending": retained_pending,
        "retained_failed": retained_failed,
        "pruned_by_status": dict(pruned_by_status),
        "sample_event_ids": [row["event_id"] for row in rows[:5]],
    }


def _wal_path(config) -> Path:
    return Path(f"{config.db_path}-wal")


def _maybe_checkpoint_wal(config, *, reason: str) -> dict[str, Any]:
    wal_path = _wal_path(config)
    wal_bytes_before = wal_path.stat().st_size if wal_path.exists() else 0
    result: dict[str, Any] = {
        "reason": reason,
        "mode": "TRUNCATE",
        "threshold_bytes": WAL_CHECKPOINT_THRESHOLD_BYTES,
        "wal_bytes_before": wal_bytes_before,
    }
    if wal_bytes_before == 0:
        result["status"] = "skipped"
        result["skip_reason"] = "wal_missing"
        return result
    if wal_bytes_before <= WAL_CHECKPOINT_THRESHOLD_BYTES:
        result["status"] = "skipped"
        result["skip_reason"] = "below_threshold"
        return result
    try:
        with open_connection(config) as connection:
            row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row is not None:
            busy = int(row[0])
            result["busy"] = busy
            result["log"] = int(row[1])
            result["checkpointed"] = int(row[2])
            result["status"] = "ok" if busy == 0 else "busy"
            result["wal_bytes_after"] = wal_path.stat().st_size if wal_path.exists() else 0
    except sqlite3.Error as exc:
        result["status"] = "error"
        result["error"] = str(exc)
    return result


def _upsert_backup_run_in_db(
    backup_db: Path,
    *,
    run_id: str,
    started_at_utc: str,
    finished_at_utc: str,
    target_root: str,
    status: str,
    backup_path: str,
    manifest_path: str,
    db_sha256: str,
    artifact_files: int,
    artifact_bytes: int,
    restore_ok: bool | None,
    snapshot_id: str | None,
    restore_details: dict[str, Any] | None,
    error_text: str | None,
) -> None:
    with sqlite3.connect(backup_db) as connection, connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(
            """
            INSERT INTO backup_runs(
                id,
                started_at_utc,
                finished_at_utc,
                status,
                target_root,
                backup_path,
                manifest_path,
                db_sha256,
                artifact_files,
                artifact_bytes,
                restore_ok,
                snapshot_id,
                restore_details_json,
                error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                finished_at_utc = excluded.finished_at_utc,
                status = excluded.status,
                target_root = excluded.target_root,
                backup_path = excluded.backup_path,
                manifest_path = excluded.manifest_path,
                db_sha256 = excluded.db_sha256,
                artifact_files = excluded.artifact_files,
                artifact_bytes = excluded.artifact_bytes,
                restore_ok = excluded.restore_ok,
                snapshot_id = excluded.snapshot_id,
                restore_details_json = excluded.restore_details_json,
                error_text = excluded.error_text
            """,
            (
                run_id,
                started_at_utc,
                finished_at_utc,
                status,
                target_root,
                backup_path,
                manifest_path,
                db_sha256,
                artifact_files,
                artifact_bytes,
                None if restore_ok is None else int(restore_ok),
                snapshot_id,
                json.dumps(restore_details, ensure_ascii=False, sort_keys=True) if restore_details else None,
                error_text,
            ),
        )


def _upsert_backup_manifest_in_db(
    backup_db: Path,
    *,
    run_id: str,
    manifest_path: Path,
    observed_at_utc: str,
) -> None:
    payload = manifest_path.read_text(encoding="utf-8")
    sha256 = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    metadata_json = json.dumps(
        {
            "source_name": manifest_path.name,
            "source_size": len(payload.encode("utf-8")),
            "run_id": run_id,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    with sqlite3.connect(backup_db) as connection, connection:
        connection.execute("PRAGMA foreign_keys = ON")
        artifact_row = connection.execute(
            """
            INSERT INTO artifacts(
                id,
                artifact_type,
                sha256,
                storage_path,
                source_path,
                mime_type,
                observed_at_utc,
                entity_id,
                metadata_json
            )
            VALUES (?, 'backup-manifest', ?, ?, NULL, 'application/json', ?, NULL, ?)
            ON CONFLICT(sha256, storage_path) DO UPDATE SET
                observed_at_utc = excluded.observed_at_utc,
                metadata_json = excluded.metadata_json
            RETURNING id
            """,
            (
                f"{run_id}:backup-manifest",
                sha256,
                str(manifest_path),
                observed_at_utc,
                metadata_json,
            ),
        ).fetchone()
        artifact_id = artifact_row[0]
        connection.execute(
            """
            INSERT INTO artifact_links(
                id,
                artifact_id,
                target_type,
                target_id,
                link_role,
                created_at_utc,
                metadata_json
            )
            VALUES (?, ?, 'backup_run', ?, 'generated', ?, ?)
            ON CONFLICT(artifact_id, target_type, target_id, link_role) DO UPDATE SET
                metadata_json = excluded.metadata_json
            """,
            (
                f"{run_id}:backup-manifest-link",
                artifact_id,
                run_id,
                observed_at_utc,
                json.dumps({"artifact_type": "backup-manifest", "path": str(manifest_path)}, ensure_ascii=False, sort_keys=True),
            ),
        )


def _restore_check(source_db: Path, *, backup_path: Path | None = None) -> dict[str, Any]:
    backup_root = backup_path or source_db.parent
    manifest_path = backup_root / "backup-manifest.json"
    artifact_root = backup_root / "chronicle-artifacts"
    with tempfile.TemporaryDirectory(prefix="chronicle-restore-") as tmp_dir:
        restored_path = Path(tmp_dir) / "chronicle.db"
        with sqlite3.connect(f"file:{source_db}?mode=ro", uri=True) as source_connection:
            with sqlite3.connect(restored_path) as restored_connection:
                source_connection.backup(restored_connection)
        with sqlite3.connect(restored_path) as connection:
            connection.row_factory = sqlite3.Row
            quick_check = connection.execute("PRAGMA quick_check").fetchone()[0]
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()
            events = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            snapshots = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            mem0_outbox = connection.execute("SELECT COUNT(*) FROM mem0_outbox").fetchone()[0]
            latest_snapshot = connection.execute(
                "SELECT id FROM snapshots ORDER BY captured_at_utc DESC LIMIT 1"
            ).fetchone()
            running_backup_runs = connection.execute(
                "SELECT COUNT(*) FROM backup_runs WHERE status = 'running'"
            ).fetchone()[0]
            latest_backup_run = connection.execute(
                """
                SELECT id, status, manifest_path, artifact_files
                FROM backup_runs
                ORDER BY started_at_utc DESC
                LIMIT 1
                """
            ).fetchone()
            manifest_artifacts = 0
            manifest_links = 0
            if latest_backup_run is not None:
                manifest_artifacts = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM artifacts
                    WHERE artifact_type = 'backup-manifest'
                      AND storage_path = ?
                    """,
                    (str(manifest_path),),
                ).fetchone()[0]
                manifest_links = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM artifact_links
                    WHERE target_type = 'backup_run'
                      AND target_id = ?
                      AND link_role = 'generated'
                    """,
                    (latest_backup_run["id"],),
                ).fetchone()[0]
        artifact_tree_files = sum(1 for path in artifact_root.rglob("*") if path.is_file()) if artifact_root.exists() else 0
        expected_artifact_files = int(latest_backup_run["artifact_files"]) if latest_backup_run is not None else 0
        backup_manifest_present = manifest_path.exists()
        backup_manifest_declared = bool(
            latest_backup_run is not None and latest_backup_run["manifest_path"] == str(manifest_path)
        )
        artifact_tree_complete = artifact_tree_files == expected_artifact_files
        ok = (
            quick_check == "ok"
            and integrity == "ok"
            and not foreign_key_violations
            and running_backup_runs == 0
            and backup_manifest_present
            and backup_manifest_declared
            and manifest_artifacts > 0
            and manifest_links > 0
            and artifact_tree_complete
        )
        return {
            "quick_check": quick_check,
            "integrity_check": integrity,
            "foreign_key_violations": len(foreign_key_violations),
            "events": int(events),
            "snapshots": int(snapshots),
            "mem0_outbox": int(mem0_outbox),
            "backup_runs_running": int(running_backup_runs),
            "latest_snapshot_id": latest_snapshot[0] if latest_snapshot else None,
            "latest_backup_run_id": latest_backup_run["id"] if latest_backup_run is not None else None,
            "latest_backup_run_status": latest_backup_run["status"] if latest_backup_run is not None else None,
            "backup_manifest_present": backup_manifest_present,
            "backup_manifest_declared": backup_manifest_declared,
            "backup_manifest_artifacts": int(manifest_artifacts),
            "backup_manifest_links": int(manifest_links),
            "artifact_tree_files": artifact_tree_files,
            "artifact_tree_expected_files": expected_artifact_files,
            "artifact_tree_complete": artifact_tree_complete,
            "ok": ok,
        }


def run_backup(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    trigger_source: str = "manual",
    force: bool = False,
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    target_root = automation.backup_root
    if not target_root.exists():
        return {
            "status": "skipped",
            "reason": "target_unmounted",
            "target_root": str(target_root),
            "backup_policy": "opportunistic_external",
        }

    latest = fetch_latest_backup_run(config, successful_only=True)
    if latest and not force:
        last_dt = datetime.fromisoformat(latest["started_at_utc"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) - last_dt < timedelta(days=automation.guards.backup_interval_days):
            return {
                "status": "skipped",
                "reason": "interval_guard",
                "last_backup": latest["started_at_utc"],
            }

    maintenance = _prune_mem0_outbox(config)

    started_at = datetime.now(timezone.utc)
    stamp = started_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    year_root = target_root / started_at.strftime("%Y")
    try:
        year_root.mkdir(parents=True, exist_ok=True)
    except (OSError, sqlite3.Error) as exc:
        return {
            "status": "skipped",
            "reason": "target_unwritable",
            "target_root": str(target_root),
            "backup_policy": "opportunistic_external",
            "error": str(exc),
        }

    source_db = config.db_path
    backup_path = year_root / stamp
    backup_path_staging: Path | None = None
    run_id: str | None = None
    started_at_utc: str | None = None
    artifact_files = 0
    artifact_files_copied = 0
    artifact_bytes_copied = 0
    artifact_copy_strategy = {"hardlinked": 0, "copied": 0, "skipped_existing": 0}
    manifest_payload: dict[str, Any] = {}
    manifest_path: Path | None = None
    db_sha256 = ""
    restore_result: dict[str, Any] = {"ok": False}
    latest_snapshot: dict[str, Any] | None = None

    try:
        with tempfile.TemporaryDirectory(prefix=f".{stamp}-", dir=year_root, ignore_cleanup_errors=True) as staging_dir:
            backup_path_staging = Path(staging_dir)
            backup_db = backup_path_staging / "chronicle.db"
            source_conn = sqlite3.connect(source_db)
            dest_conn = sqlite3.connect(backup_db)
            try:
                with dest_conn:
                    source_conn.backup(dest_conn)
            finally:
                source_conn.close()
                dest_conn.close()

            run_id = start_backup_run(config, target_root=str(target_root))
            with open_connection(config) as connection:
                started_row = connection.execute(
                    "SELECT started_at_utc FROM backup_runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
            started_at_utc = started_row["started_at_utc"] if started_row is not None else utc_now()

            if backup_path.exists():
                backup_path = year_root / f"{stamp}-{run_id[:8]}"

            artifact_target = backup_path_staging / "chronicle-artifacts"
            artifact_files_copied, artifact_bytes_copied, artifact_copy_strategy = _copy_tree_incremental(
                config.artifact_dir,
                artifact_target,
            )
            artifact_files = sum(1 for path in artifact_target.rglob("*") if path.is_file())
            db_sha256 = hashlib.sha256(backup_db.read_bytes()).hexdigest()
            latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)

            manifest_payload = {
                "created_at_utc": utc_now(),
                "trigger_source": trigger_source,
                "source_db": str(source_db),
                "backup_db": str(backup_path / "chronicle.db"),
                "backup_path": str(backup_path),
                "db_sha256": db_sha256,
                "artifact_files": artifact_files,
                "artifact_files_copied": artifact_files_copied,
                "artifact_bytes_copied": artifact_bytes_copied,
                "artifact_copy_strategy": artifact_copy_strategy,
                "latest_snapshot_id": latest_snapshot.get("id") if latest_snapshot else None,
                "maintenance": maintenance,
                "run_id": run_id,
            }
            backup_path_staging.rename(backup_path)
            manifest_path = backup_path / "backup-manifest.json"
            manifest_path.write_text(json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except (OSError, sqlite3.Error) as exc:
        if run_id is not None:
            finish_backup_run(
                config,
                run_id=run_id,
                status="skipped",
                backup_path=str(backup_path),
                error_text=f"target_unwritable: {exc}",
            )
        return {
            "status": "skipped",
            "reason": "target_unwritable",
            "target_root": str(target_root),
            "backup_policy": "opportunistic_external",
            "error": str(exc),
            "maintenance": maintenance,
        }

    if run_id is None:
        raise RuntimeError("backup run id was not created")
    if manifest_path is None:
        raise RuntimeError("backup manifest path was not created")
    if latest_snapshot is None:
        latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)

    backup_db = backup_path / "chronicle.db"
    db_sha256 = hashlib.sha256(backup_db.read_bytes()).hexdigest()
    finished_at_utc = utc_now()
    _upsert_backup_run_in_db(
        backup_db,
        run_id=run_id,
        started_at_utc=started_at_utc or finished_at_utc,
        finished_at_utc=finished_at_utc,
        target_root=str(target_root),
        status="ok",
        backup_path=str(backup_path),
        manifest_path=str(manifest_path),
        db_sha256=db_sha256,
        artifact_files=artifact_files,
        artifact_bytes=artifact_bytes_copied,
        restore_ok=None,
        snapshot_id=latest_snapshot.get("id") if latest_snapshot else None,
        restore_details=None,
        error_text=None,
    )
    _upsert_backup_manifest_in_db(
        backup_db,
        run_id=run_id,
        manifest_path=manifest_path,
        observed_at_utc=finished_at_utc,
    )
    restore_result = _restore_check(backup_db, backup_path=backup_path)
    _upsert_backup_run_in_db(
        backup_db,
        run_id=run_id,
        started_at_utc=started_at_utc or finished_at_utc,
        finished_at_utc=finished_at_utc,
        target_root=str(target_root),
        status="ok" if restore_result["ok"] else "failed",
        backup_path=str(backup_path),
        manifest_path=str(manifest_path),
        db_sha256=db_sha256,
        artifact_files=artifact_files,
        artifact_bytes=artifact_bytes_copied,
        restore_ok=restore_result["ok"],
        snapshot_id=latest_snapshot.get("id") if latest_snapshot else None,
        restore_details=restore_result,
        error_text=None if restore_result["ok"] else "restore_check_failed",
    )

    # Checkpoint backup DB WAL so all metadata is flushed into the main file.
    # Without this, copying only chronicle.db would lose backup_run/manifest data.
    try:
        with sqlite3.connect(backup_db) as bkp_conn:
            bkp_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass  # Best-effort; backup is still usable with WAL files present.

    # Recompute SHA after checkpoint to reflect the complete backup state.
    db_sha256 = hashlib.sha256(backup_db.read_bytes()).hexdigest()

    finish_backup_run(
        config,
        run_id=run_id,
        status="ok" if restore_result["ok"] else "failed",
        backup_path=str(backup_path),
        manifest_path=str(manifest_path),
        db_sha256=db_sha256,
        artifact_files=artifact_files,
        artifact_bytes=artifact_bytes_copied,
        restore_ok=restore_result["ok"],
        snapshot_id=latest_snapshot.get("id") if latest_snapshot else None,
        restore_details=restore_result,
        error_text=None if restore_result["ok"] else "restore_check_failed",
    )
    maintenance["wal_checkpoint"] = _maybe_checkpoint_wal(config, reason="post_backup") if restore_result["ok"] else {
        "status": "skipped",
        "skip_reason": "backup_failed",
        "reason": "post_backup",
    }

    artifact = _store_text_artifact(
        manifest,
        target_type="snapshot" if latest_snapshot else "event",
        target_id=latest_snapshot["id"] if latest_snapshot else run_id,
        artifact_type="backup-manifest",
        content=json.dumps(manifest_payload, ensure_ascii=False, indent=2) + "\n",
        filename="backup-manifest.json",
        observed_at_utc=utc_now(),
        entity_type="system",
        entity_name="chronicle",
        link_role="backs_up",
    )
    return {
        "status": "ok" if restore_result["ok"] else "failed",
        "run_id": run_id,
        "backup_path": str(backup_path),
        "manifest_path": str(manifest_path),
        "artifact_id": artifact["id"],
        "restore_check": restore_result,
        "target_root": str(target_root),
        "backup_policy": "opportunistic_external",
        "maintenance": maintenance,
        "artifact_files": artifact_files,
        "artifact_files_copied": artifact_files_copied,
        "artifact_bytes_copied": artifact_bytes_copied,
        "artifact_copy_strategy": artifact_copy_strategy,
    }


def _evaluate_audit(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    weekly: bool,
    trigger_source: str,
    local_now: datetime,
    run_key: str,
    base_run_key: str,
    forced: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any] | None]:
    config = config_from_manifest(manifest)
    issues: list[dict[str, Any]] = []
    latest_snapshot = fetch_latest_snapshot(config, domain="global") or fetch_latest_snapshot(config)
    latest_projection = fetch_latest_projection_run(config)
    latest_backup = fetch_latest_backup_run(config, successful_only=True)
    backup_target_available = automation.backup_root.exists()
    latest_canary = fetch_latest_automation_run(config, job_name="minimax-canary")
    pending_mem0 = count_mem0_outbox(config, status="pending")
    failed_mem0 = count_mem0_outbox(config, status="failed")
    freshness_audit = build_freshness_audit(manifest, domain_id="memory" if "memory" in manifest["domain_map"] else "global")
    mem0_state_drift = repair_mem0_state(manifest, dry_run=True)
    mem0_queue_backfill = backfill_mem0_queue(manifest, dry_run=True)
    legacy_surface_findings = _legacy_surface_findings(manifest)

    if not latest_snapshot:
        issues.append({"severity": "critical", "kind": "snapshot_missing", "detail": "No Chronicle snapshot found."})
    else:
        snapshot_age = local_now - datetime.fromisoformat(latest_snapshot["captured_at_local"])
        if snapshot_age > timedelta(hours=automation.guards.snapshot_stale_hours):
            issues.append({"severity": "warn", "kind": "snapshot_stale", "detail": f"Latest snapshot is {snapshot_age} old."})

    if not latest_projection:
        issues.append({"severity": "warn", "kind": "projection_missing", "detail": "No projection runs recorded."})
    else:
        projection_age = datetime.now(timezone.utc) - datetime.fromisoformat(latest_projection["rendered_at_utc"].replace("Z", "+00:00"))
        if projection_age > timedelta(hours=automation.guards.projection_stale_hours):
            issues.append({"severity": "warn", "kind": "projection_stale", "detail": f"Latest projection is {projection_age} old."})

    if not latest_backup:
        if backup_target_available:
            issues.append({"severity": "warn", "kind": "backup_missing", "detail": "No successful backup run recorded."})
    else:
        backup_age = datetime.now(timezone.utc) - datetime.fromisoformat(latest_backup["started_at_utc"].replace("Z", "+00:00"))
        if backup_target_available and backup_age > timedelta(days=automation.guards.backup_interval_days + 1):
            issues.append({"severity": "warn", "kind": "backup_stale", "detail": f"Latest backup is {backup_age} old."})
        if latest_backup.get("restore_ok") == 0:
            issues.append({"severity": "critical", "kind": "restore_failed", "detail": "Latest backup restore check failed."})

    if not latest_canary:
        issues.append({"severity": "warn", "kind": "minimax_canary_missing", "detail": "No MiniMax canary run recorded."})
    else:
        canary_age = datetime.now(timezone.utc) - datetime.fromisoformat(latest_canary["started_at_utc"].replace("Z", "+00:00"))
        health_status = latest_canary.get("details", {}).get("health_status") or latest_canary.get("status")
        if canary_age > timedelta(hours=automation.guards.minimax_canary_stale_hours):
            issues.append({"severity": "warn", "kind": "minimax_canary_stale", "detail": f"Latest MiniMax canary is {canary_age} old."})
        if health_status != "ok":
            issues.append({"severity": "warn", "kind": "minimax_unhealthy", "detail": f"Latest MiniMax canary status is {health_status}."})

    if pending_mem0 or failed_mem0:
        issues.append(
            {
                "severity": "warn",
                "kind": "mem0_backlog",
                "detail": f"mem0 backlog pending={pending_mem0} failed={failed_mem0}",
            }
        )

    for freshness_issue in freshness_audit["issues"]:
        issues.append(
            {
                "severity": freshness_issue["severity"],
                "kind": freshness_issue["kind"],
                "detail": freshness_issue["detail"],
                "path": freshness_issue.get("path"),
                "source_id": freshness_issue.get("source_id"),
            }
        )

    if mem0_state_drift["repaired_count"]:
        sample_ids = [item["id"] for item in mem0_state_drift["repaired"][:5]]
        issues.append(
            {
                "severity": "critical",
                "kind": "mem0_state_drift",
                "detail": (
                    "events.mem0_* drift from mem0_outbox for "
                    f"{mem0_state_drift['repaired_count']} row(s); sample={sample_ids}"
                ),
            }
        )

    if mem0_queue_backfill["requeued_count"]:
        sample_ids = [item["id"] for item in mem0_queue_backfill["requeued"][:5]]
        issues.append(
            {
                "severity": "warn",
                "kind": "mem0_queue_backfill",
                "detail": (
                    "Eligible MCP events are still skipped from semantic sync for "
                    f"{mem0_queue_backfill['requeued_count']} row(s); sample={sample_ids}"
                ),
            }
        )

    if legacy_surface_findings:
        issues.append(
            {
                "severity": "warn",
                "kind": "legacy_surface_reference",
                "detail": (
                    f"Found {len(legacy_surface_findings)} active manifest/doc reference(s) "
                    "to deprecated Chronicle upkeep surfaces."
                ),
            }
        )

    retention_status = _prune_mem0_outbox(config) if weekly else {
        "pruned": 0,
        "cutoff_utc": (datetime.now(timezone.utc) - timedelta(days=MEM0_OUTBOX_PRUNE_AFTER_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "skipped": True,
        "reason": "audit_read_only",
        "retained_pending": pending_mem0,
        "retained_failed": failed_mem0,
    }
    wal_checkpoint = _maybe_checkpoint_wal(config, reason="weekly_audit") if weekly else None

    report = {
        "generated_at_utc": utc_now(),
        "weekly": weekly,
        "forced": forced,
        "run_key": run_key,
        "base_run_key": base_run_key,
        "trigger_source": trigger_source,
        "issues": issues,
        "freshness_audit": freshness_audit,
        "latest_snapshot_id": latest_snapshot.get("id") if latest_snapshot else None,
        "latest_backup": latest_backup.get("started_at_utc") if latest_backup else None,
        "backup_target_root": str(automation.backup_root),
        "backup_target_available": backup_target_available,
        "latest_minimax_canary": latest_canary.get("started_at_utc") if latest_canary else None,
        "mem0_state_drift_count": mem0_state_drift["repaired_count"],
        "mem0_state_drift_sample": mem0_state_drift["repaired"][:10],
        "mem0_outbox_retention": retention_status,
        "wal_checkpoint": wal_checkpoint,
        "legacy_surface_findings": legacy_surface_findings,
    }

    if weekly and latest_backup and latest_backup.get("backup_path"):
        backup_db = Path(latest_backup["backup_path"]) / "chronicle.db"
        if backup_db.exists():
            report["weekly_restore_check"] = _restore_check(
                backup_db,
                backup_path=Path(latest_backup["backup_path"]),
            )
            if not report["weekly_restore_check"]["ok"]:
                issues.append({"severity": "critical", "kind": "weekly_restore_failed", "detail": "Weekly restore drill failed."})

    return report, issues, latest_snapshot


def run_audit(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    weekly: bool = False,
    trigger_source: str = "manual",
    force: bool = False,
    persist: bool = True,
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    local_now = _local_now(config)
    base_run_key = local_now.strftime("%G-W%V") if weekly else local_now.strftime("%Y-%m-%d")
    run_key = base_run_key if not force else f"{base_run_key}:force:{datetime.now(timezone.utc).strftime('%H%M%S%f')}"
    curation_type = "weekly_audit" if weekly else "daily_audit"

    if persist:
        run_id, created = start_curation_run(
            config,
            curation_type=curation_type,
            run_key=run_key,
            model_name=automation.minimax.model if weekly else None,
            payload={"trigger_source": trigger_source, "forced": force, "base_run_key": base_run_key},
        )
        if not created:
            existing = fetch_curation_run(config, curation_type=curation_type, run_key=run_key)
            return {"status": "existing", "run": existing}
    else:
        run_id = None

    report, issues, latest_snapshot = _evaluate_audit(
        manifest,
        automation,
        weekly=weekly,
        trigger_source=trigger_source,
        local_now=local_now,
        run_key=run_key,
        base_run_key=base_run_key,
        forced=force,
    )

    if not persist:
        return {
            "status": "ok" if not issues else "issues",
            "run_id": None,
            "artifact_id": None,
            "event_id": None,
            "issue_count": len(issues),
            "issues": issues,
            "report": report,
        }

    artifact = _store_text_artifact(
        manifest,
        target_type="snapshot" if latest_snapshot else "event",
        target_id=latest_snapshot["id"] if latest_snapshot else run_id,
        artifact_type="audit-report",
        content=json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        filename=f"{curation_type}-{run_key.replace(':', '-')}.json",
        observed_at_utc=utc_now(),
        entity_type="system",
        entity_name="chronicle",
    )

    event_id = None
    if issues:
        counts = Counter(issue["severity"] for issue in issues)
        event = record_event(
            manifest,
            {
                "agent": "chronicle",
                "domain": "memory",
                "category": "maintenance" if counts["critical"] == 0 else "anomaly",
                "project": "status",
                "text": f"Chronicle audit {base_run_key}{' (forced)' if force else ''}: {len(issues)} issues detected.",
                "why": json.dumps(counts, ensure_ascii=False),
                "source_files": [],
                "mem0_status": "off",
                "mem0_error": None,
                "mem0_raw": None,
            },
            append_compat=True,
            source_kind="chronicle_audit",
            imported_from=f"chronicle.audit:{trigger_source}",
        )
        event_id = event["id"]
        link_artifact(
            config,
            artifact_id=artifact["id"],
            target_type="event",
            target_id=event_id,
            link_role="documents",
            metadata={"curation_type": curation_type},
        )

    finish_curation_run(
        config,
        run_id=run_id,
        status="ok" if not issues else ("failed_soft" if all(issue["severity"] != "critical" for issue in issues) else "failed"),
        payload=report,
        artifact_id=artifact["id"],
        event_id=event_id,
        notes=f"{len(issues)} issues",
    )
    return {
        "status": "ok" if not issues else "issues",
        "run_id": run_id,
        "artifact_id": artifact["id"],
        "event_id": event_id,
        "issue_count": len(issues),
        "issues": issues,
        "report": report,
    }


def run_automation_job(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    job_name: str,
    trigger_source: str = "manual",
) -> dict[str, Any]:
    config = config_from_manifest(manifest)
    local_now = _local_now(config)
    if job_name == "daily-capture":
        run_key = local_now.strftime("%Y-%m-%d")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        try:
            snapshot = capture_runtime_snapshot(
                manifest,
                domain_id="global",
                agent="chronicle-launchd",
                title="Scheduled daily capture",
                focus="native automation",
                append_compat=True,
                render_generated=True,
            )
            sync = sync_mem0_outbox(
                manifest,
                automation,
                limit=automation.guards.mem0_sync_batch_size,
                trigger_source=trigger_source,
            )
            finish_automation_run(
                config,
                run_id=run_id,
                status="ok",
                details={"sync": sync},
                snapshot_id=snapshot["id"],
            )
            return {"status": "ok", "run_id": run_id, "snapshot_id": snapshot["id"], "sync": sync}
        except Exception as exc:
            finish_automation_run(config, run_id=run_id, status="failed", details={"error": str(exc)})
            raise

    if job_name == "daybook":
        run_key = local_now.strftime("%Y-%m-%d")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        result = run_daybook(manifest, automation, trigger_source=trigger_source)
        finish_automation_run(
            config,
            run_id=run_id,
            status=result["status"] if result["status"] != "existing" else "ok",
            details=result,
            event_id=result.get("event_id"),
        )
        return result | {"run_id": run_id}

    if job_name == "mem0-dump":
        run_key = local_now.strftime("%Y-%m-%d")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        try:
            result = _run_mem0_dump(
                manifest,
                automation,
                trigger_source=trigger_source,
                automation_run_id=run_id,
            )
            finish_automation_run(
                config,
                run_id=run_id,
                status="ok" if result["status"] == "ok" else result["status"],
                details=result,
                snapshot_id=result.get("snapshot_id"),
            )
            return result | {"run_id": run_id}
        except Exception as exc:
            finish_automation_run(config, run_id=run_id, status="failed", details={"error": str(exc)})
            raise

    if job_name == "company-intel":
        run_key = local_now.strftime("%Y-%m-%d")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        try:
            result = _run_company_intel_refresh(
                manifest,
                automation,
                trigger_source=trigger_source,
                automation_run_id=run_id,
            )
            finish_automation_run(
                config,
                run_id=run_id,
                status=result["status"],
                details=result,
                snapshot_id=result.get("snapshot_id"),
            )
            return result | {"run_id": run_id}
        except Exception as exc:
            finish_automation_run(config, run_id=run_id, status="failed", details={"error": str(exc)})
            raise

    if job_name == "minimax-canary":
        run_key = local_now.strftime("%Y-%m-%d")
        previous = fetch_latest_automation_run(config, job_name=job_name)
        previous_health = None if not previous else (previous.get("details", {}).get("health_status") or previous.get("status"))
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        result = run_minimax_canary(
            manifest,
            automation,
            trigger_source=trigger_source,
            previous_health=previous_health,
        )
        finish_automation_run(
            config,
            run_id=run_id,
            status=result["status"] if result["status"] != "existing" else "ok",
            details=result.get("details") or result,
            event_id=result.get("event_id"),
            snapshot_id=result.get("snapshot_id"),
        )
        return result | {"run_id": run_id}

    if job_name == "weekly-audit":
        run_key = local_now.strftime("%G-W%V")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        result = run_audit(manifest, automation, weekly=True, trigger_source=trigger_source)
        finish_automation_run(
            config,
            run_id=run_id,
            status="ok" if result["status"] == "ok" else result["status"],
            details=result,
            event_id=result.get("event_id"),
        )
        return result | {"run_id": run_id}

    if job_name == "backup":
        run_key = local_now.strftime("%Y-%m-%d")
        run_id, created = start_automation_run(
            config,
            job_name=job_name,
            trigger_source=trigger_source,
            run_key=run_key,
            retry_failed=True,
        )
        if not created:
            return {"status": "existing", "run": fetch_automation_run(config, job_name=job_name, run_key=run_key)}
        result = run_backup(manifest, automation, trigger_source=trigger_source)
        finish_automation_run(
            config,
            run_id=run_id,
            status=result["status"],
            details=result,
            snapshot_id=result.get("snapshot_id"),
        )
        return result | {"run_id": run_id}

    raise ValueError(f"Unknown automation job: {job_name}")


def _launchd_plist(
    *,
    label: str,
    wrapper_path: Path,
    workdir: Path,
    stdout_path: Path,
    stderr_path: Path,
    throttle_seconds: int,
    hour: int,
    minute: int,
    weekday: int | None = None,
    start_on_mount: bool = False,
) -> bytes:
    interval: dict[str, int] = {"Hour": hour, "Minute": minute}
    if weekday is not None:
        interval["Weekday"] = weekday
    payload = {
        "Label": label,
        "ProgramArguments": ["/bin/zsh", str(wrapper_path)],
        "WorkingDirectory": str(workdir),
        "StartCalendarInterval": interval,
        "StandardOutPath": str(stdout_path),
        "StandardErrorPath": str(stderr_path),
        "ThrottleInterval": throttle_seconds,
    }
    if start_on_mount:
        payload["StartOnMount"] = True
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=False)


def _launchd_wrapper(manifest: dict[str, Any], automation: AutomationConfig, job_name: str) -> str:
    env_file = shlex.quote(str(automation.env_file))
    chronicle = shlex.quote(str(_chronicle_script(manifest)))
    job = shlex.quote(job_name)
    return "\n".join(
        [
            "#!/bin/zsh",
            "set -euo pipefail",
            f"if [[ -f {env_file} ]]; then",
            f"  set -a; source {env_file} >/dev/null 2>&1; set +a",
            "fi",
            f"exec {chronicle} automation run --job {job}",
            "",
        ]
    )


def _write_executable_script(path: Path, content: str) -> None:
    """Atomically write an executable shell script."""
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(content, encoding="utf-8")
    temp_path.chmod(0o755)
    temp_path.replace(path)


def _launchd_job_health(
    *,
    wrapper_path: Path,
    plist_exists: bool,
    plist_lint_ok: bool,
    wrapper_exists: bool,
    loaded: bool,
    require_loaded: bool,
) -> tuple[str, list[str]]:
    issues: list[str] = []
    if not plist_exists:
        issues.append("missing_plist")
    elif not plist_lint_ok:
        issues.append("bad_plist")

    if not wrapper_exists:
        issues.append("missing_wrapper")
    elif not wrapper_path.is_file():
        issues.append("wrapper_not_file")
    elif not os.access(wrapper_path, os.X_OK):
        issues.append("wrapper_not_executable")
    if require_loaded and plist_exists and not loaded:
        issues.append("not_loaded")

    return ("ok" if not issues else "critical", issues)


def install_launchd(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    agent_dir: Path | None = None,
    runtime_dir: Path | None = None,
    log_dir: Path | None = None,
    load_jobs: bool = False,
) -> dict[str, Any]:
    ensure_automation_dirs(automation)
    agent_dir = (agent_dir or automation.launch_agent_dir).expanduser()
    runtime_dir = (runtime_dir or automation.launchd_runtime_dir).expanduser()
    log_dir = (log_dir or automation.launchd_log_dir).expanduser()
    agent_dir.mkdir(parents=True, exist_ok=True)
    runtime_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    installed: list[dict[str, Any]] = []
    for key, label in LAUNCHD_LABELS.items():
        wrapper_path = runtime_dir / f"{key}.sh"
        _write_executable_script(wrapper_path, _launchd_wrapper(manifest, automation, key.replace("_", "-")))

        schedule = automation.jobs[key]
        plist_path = agent_dir / f"{label}.plist"
        plist_path.write_bytes(
            _launchd_plist(
                label=label,
                wrapper_path=wrapper_path,
                workdir=_status_root(manifest),
                stdout_path=log_dir / f"{key}.out.log",
                stderr_path=log_dir / f"{key}.err.log",
                throttle_seconds=schedule.throttle_seconds,
                hour=schedule.hour,
                minute=schedule.minute,
                weekday=schedule.weekday,
                start_on_mount=key == "backup",
            )
        )
        installed.append({"label": label, "plist_path": str(plist_path), "wrapper_path": str(wrapper_path)})

        if load_jobs:
            uid = str(os.getuid())
            subprocess.run(["launchctl", "bootout", f"gui/{uid}", str(plist_path)], check=False, capture_output=True, text=True)
            subprocess.run(["launchctl", "bootstrap", f"gui/{uid}", str(plist_path)], check=True, capture_output=True, text=True)
            subprocess.run(["launchctl", "enable", f"gui/{uid}/{label}"], check=False, capture_output=True, text=True)

    return {
        "status": "ok",
        "agent_dir": str(agent_dir),
        "runtime_dir": str(runtime_dir),
        "log_dir": str(log_dir),
        "installed": installed,
        "loaded": load_jobs,
    }


def doctor_launchd(
    manifest: dict[str, Any],
    automation: AutomationConfig,
    *,
    agent_dir: Path | None = None,
    runtime_dir: Path | None = None,
    strict_loaded: bool = False,
) -> dict[str, Any]:
    scoped_paths = agent_dir is not None or runtime_dir is not None
    agent_dir = (agent_dir or automation.launch_agent_dir).expanduser()
    runtime_dir = (runtime_dir or automation.launchd_runtime_dir).expanduser()
    uid = str(os.getuid())
    jobs: list[dict[str, Any]] = []
    issue_count = 0
    for key, label in LAUNCHD_LABELS.items():
        plist_path = agent_dir / f"{label}.plist"
        wrapper_path = runtime_dir / f"{key}.sh"
        lint = subprocess.run(["plutil", "-lint", str(plist_path)], capture_output=True, text=True, check=False) if plist_path.exists() else None
        loaded_probe = subprocess.run(["launchctl", "print", f"gui/{uid}/{label}"], capture_output=True, text=True, check=False) if plist_path.exists() else None
        loaded = bool(loaded_probe and loaded_probe.returncode == 0)
        if loaded and scoped_paths and loaded_probe is not None:
            loaded_output = "\n".join(part for part in [loaded_probe.stdout, loaded_probe.stderr] if part)
            loaded = str(wrapper_path) in loaded_output or str(plist_path) in loaded_output
        job_status, job_issues = _launchd_job_health(
            wrapper_path=wrapper_path,
            plist_exists=plist_path.exists(),
            plist_lint_ok=bool(lint and lint.returncode == 0),
            wrapper_exists=wrapper_path.exists(),
            loaded=loaded,
            require_loaded=strict_loaded,
        )
        issue_count += len(job_issues)
        jobs.append(
            {
                "label": label,
                "plist_exists": plist_path.exists(),
                "wrapper_exists": wrapper_path.exists(),
                "wrapper_executable": wrapper_path.exists() and os.access(wrapper_path, os.X_OK),
                "plist_lint_ok": bool(lint and lint.returncode == 0),
                "loaded": loaded,
                "lint_stdout": lint.stdout if lint else "",
                "lint_stderr": lint.stderr if lint else "",
                "status": job_status,
                "issues": job_issues,
            }
        )
    return {
        "status": "ok" if issue_count == 0 else "issues",
        "issue_count": issue_count,
        "strict_loaded": strict_loaded,
        "jobs": jobs,
    }


def install_git_hooks(
    automation: AutomationConfig,
    *,
    repos: list[str] | None = None,
) -> dict[str, Any]:
    ensure_automation_dirs(automation)
    hook_path = automation.git_hooks_dir
    post_commit = hook_path / "post-commit"
    if post_commit.exists():
        post_commit.chmod(0o755)

    selected = [repo.slug for repo in automation.repos] if not repos else repos
    updated: list[dict[str, Any]] = []
    seen_roots: set[str] = set()
    for slug in selected:
        repo = repo_by_slug(automation, slug)
        git_root = _git_root(repo.path)
        root_key = str(git_root)
        if root_key in seen_roots:
            updated.append({"repo": slug, "hooks_path": str(hook_path), "git_root": root_key, "status": "already-configured"})
            continue
        subprocess.run(
            ["git", "-C", str(git_root), "config", "core.hooksPath", str(hook_path)],
            check=True,
            capture_output=True,
            text=True,
        )
        seen_roots.add(root_key)
        updated.append({"repo": slug, "hooks_path": str(hook_path), "git_root": root_key, "status": "configured"})
    return {"status": "ok", "updated": updated}
