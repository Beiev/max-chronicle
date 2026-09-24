from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import math
from pathlib import Path
import sqlite3
import sys
from typing import Iterator
from zoneinfo import ZoneInfo

from .bootstrap import bootstrap_legacy
from .brief import BRIEF_DEFAULT_CHARS, build_brief
from .notes import notes_status, read_note, sync_notes
from .browse import render_browse_help, render_daybook, render_entity_timeline, render_recent_events, render_search_results
from .config import (
    ENV_CHRONICLE_AUTO_MIGRATE,
    EXIT_CONFIG_ERROR,
    EXIT_SCHEMA_ACTION,
    ChronicleConfig,
    ChronicleConfigError,
    default_automation_path,
    default_config,
    default_manifest_path,
    ensure_runtime_dirs,
    feature_enabled,
    resolve_status_root,
)
from .db import MigrationError, connect, database_summary, ensure_schema, read_schema_state
from .evals import build_report, check_thresholds, format_report, load_golden, run_eval
from .native_automation import (
    doctor_launchd,
    install_git_hooks,
    install_launchd,
    load_native_automation,
    run_audit,
    run_automation_job,
    run_backup,
    run_daybook,
    run_git_commit_hook,
    sync_mem0_outbox,
)
from .runtime_context import load_manifest
from .service import (
    backfill_mem0_queue,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    capture_runtime_snapshot,
    embed_backfill,
    guard_event,
    materialize_normalized_entities,
    project_state,
    query_context,
    query_memory,
    reconstruct_timeline,
    record_event,
    render_projections,
    repair_event_categories,
    repair_mem0_state,
    repair_stale_runs,
)
from .scaffold import scaffold_workspace
from .store import (
    DEFAULT_STALE_RUN_TTL_HOURS,
    config_from_manifest,
    fetch_recent_events,
    parse_when,
    store_snapshot,
)


@contextmanager
def _connection(config: ChronicleConfig) -> Iterator[sqlite3.Connection]:
    """Scoped connection for commands that drive migrations themselves.

    Deliberately not store.open_connection: that one applies migrations behind
    a per-process memo, which would make `chronicle migrate` report an empty
    applied-list. The inner `with connection` keeps the commit/rollback the
    callers rely on; the finally closes it (sqlite3's own __exit__ does not).
    """
    ensure_runtime_dirs(config)
    connection = connect(config.db_path)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


# Exit codes for errors an operator can act on without reading a traceback.
_STRICT_FAILURE_STATUSES = {"warn", "warning", "critical", "issues", "error", "failed", "failed_soft", "skipped"}


def _print_json(payload: object, *, strict: bool = False, failure_statuses: set[str] | None = None) -> int:
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not strict or not isinstance(payload, dict):
        return 0
    status = payload.get("status")
    statuses = failure_statuses or _STRICT_FAILURE_STATUSES
    return 1 if isinstance(status, str) and status in statuses else 0


def _doctor_status(*statuses: str | None) -> str:
    normalized = {status for status in statuses if status}
    if normalized & {"critical", "failed", "error"}:
        return "critical"
    if normalized & {"warn", "warning", "issues", "failed_soft", "skipped"}:
        return "issues"
    return "ok"


def _config_from_args(args: argparse.Namespace) -> ChronicleConfig:
    return default_config(
        args.db,
        manifest_path=getattr(args, "manifest", None),
        automation_path=getattr(args, "automation_config", None),
    )


def cmd_migrate(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    with _connection(config) as connection:
        prepared = ensure_schema(connection, config, allow_upgrade=True)
        summary = database_summary(connection)
    payload = {
        "db_path": str(config.db_path),
        "applied": [
            {"version": item.version, "name": item.name, "path": str(item.path)}
            for item in prepared.applied
        ],
        "backup_path": str(prepared.backup_path) if prepared.backup_path else None,
        "summary": summary,
    }
    return _print_json(payload)


def cmd_import_legacy(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    with _connection(config) as connection:
        ensure_schema(
            connection,
            config,
            allow_upgrade=feature_enabled(ENV_CHRONICLE_AUTO_MIGRATE, default=False),
        )
        result = bootstrap_legacy(connection, config, queue_mem0=args.queue_mem0)
        summary = database_summary(connection)
    payload = {
        "db_path": str(config.db_path),
        "import": result,
        "summary": summary,
    }
    return _print_json(payload)


def cmd_status(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    if not config.db_path.exists():
        payload = {
            "db_path": str(config.db_path),
            "exists": False,
            "message": "chronicle.db does not exist yet. Run `chronicle migrate` first.",
        }
        return _print_json(payload)

    # Read-only on the schema: `status` used to apply pending migrations as a
    # side effect, which made a diagnostic command an unannounced upgrade.
    with _connection(config) as connection:
        state = read_schema_state(connection, config)
        up_to_date = not state.pending and not state.unknown and not state.foreign
        summary = database_summary(connection) if up_to_date else None
    payload: dict[str, object] = {
        "db_path": str(config.db_path),
        "exists": True,
        "schema": {
            "current_version": state.current_version,
            "target_version": state.target_version,
            "pending": [item.version for item in state.pending],
            "unknown": list(state.unknown),
        },
        "summary": summary,
    }
    if state.foreign:
        payload["message"] = "This is not a Chronicle database; Chronicle will not modify it."
    elif state.unknown:
        payload["message"] = "The database is newer than this code; upgrade max-chronicle."
    elif state.pending:
        payload["message"] = "The schema is behind this code; run `chronicle migrate` (it backs up first)."
    _print_json(payload)
    return 0 if up_to_date else EXIT_SCHEMA_ACTION


def cmd_init(args: argparse.Namespace) -> int:
    payload = scaffold_workspace(
        args.root,
        force=args.force,
        timezone_name=args.timezone,
    )
    return _print_json(payload)


def cmd_timeline(args: argparse.Namespace) -> int:
    loaded_manifest = load_manifest(args.manifest)
    config = _config_from_args(args)
    target = parse_when(args.at, config.timezone)
    if not config.db_path.exists():
        print("chronicle.db does not exist yet.", file=sys.stderr)
        return 1

    payload = reconstruct_timeline(
        loaded_manifest,
        timestamp=target,
        domain=args.domain,
        window_hours=args.window_hours,
        limit=args.limit,
    )

    if args.format == "json":
        return _print_json(payload)

    print(f"# Chronicle Timeline — {target.astimezone(ZoneInfo(config.timezone)).isoformat(timespec='seconds')}")
    if payload.get("domain"):
        print(f"Domain: {payload['domain']}")
    print()
    print("## Nearest Snapshots")
    if payload["nearest_snapshots"]:
        for row in payload["nearest_snapshots"]:
            delta_minutes = int(int(row["delta_seconds"]) // 60)
            print(
                f"- {row['captured_at_local']} | "
                f"{row.get('title') or row.get('label') or 'snapshot'} | "
                f"domain={row.get('domain')} agent={row.get('agent')} delta={delta_minutes}m"
            )
    else:
        print("- No snapshots in chronicle.db yet.")
    print()
    print("## Events In Window")
    if payload["events"]:
        for row in payload["events"]:
            entity = row["entity_id"] or "n/a"
            category = row["category"] or "n/a"
            print(f"- {row['recorded_at']} | {category} | {entity}")
            print(f"  {row['text']}")
            if row["why"]:
                print(f"  why: {row['why']}")
    else:
        print("- No events in the selected window.")
    return 0


def cmd_record(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if args.db is not None:
        manifest = dict(manifest)
        manifest["paths"] = dict(manifest["paths"])
        manifest["paths"]["chronicle_db"] = str(args.db)
    entry = {
        "id": args.id,
        "recorded_at": args.recorded_at,
        "agent": args.agent,
        "domain": args.domain,
        "category": args.category,
        "project": args.project,
        "task_id": args.task_id,
        "session_id": args.session_id,
        "request_id": args.request_id,
        "checkpoint": json.loads(Path(args.checkpoint_file).read_text()) if args.checkpoint_file else None,
        "fact": json.loads(Path(args.fact_file).read_text()) if args.fact_file else None,
        "text": args.text,
        "why": args.why,
        "source_files": args.source_file or [],
        "mem0_status": args.mem0_status,
        "mem0_error": args.mem0_error,
        "mem0_raw": None,
    }
    stored = record_event(
        manifest,
        entry,
        append_compat=True,
        dedupe=True,
        source_kind="chronicle_cli",
        imported_from="chronicle.cli.record",
    )
    return _print_json(stored)


def cmd_capture(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    snapshot = {
        "id": args.id,
        "captured_at_utc": args.captured_at_utc,
        "captured_at_local": args.captured_at_local,
        "timezone": args.timezone or config.timezone,
        "agent": args.agent,
        "domain": args.domain,
        "title": args.title,
        "focus": args.focus,
        "label": args.label,
        "source_excerpts": [],
        "recent_ledger": [],
        "repos": [],
    }
    stored = store_snapshot(config, snapshot)
    return _print_json(stored)


def cmd_recent(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if args.db is not None:
        manifest = dict(manifest)
        manifest["paths"] = dict(manifest["paths"])
        manifest["paths"]["chronicle_db"] = str(args.db)
    config = config_from_manifest(manifest)
    events = fetch_recent_events(config, limit=args.limit, domain=args.domain, visibility="raw")
    if args.format == "json":
        return _print_json(events)

    if not events:
        print("No Chronicle events.")
        return 0

    for entry in events:
        project = entry.get("project") or "n/a"
        category = entry.get("category") or "n/a"
        print(f"- {entry.get('recorded_at')} | {category} | {project} | mem0={entry.get('mem0_status')}")
        print(f"  {entry.get('text')}")
        if entry.get("why"):
            print(f"  why: {entry['why']}")
    return 0


def cmd_guard_event(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = guard_event(
        manifest,
        event_id=args.event_id,
        verdict=args.verdict,
        reason=args.reason,
        apply=args.apply,
    )
    return _print_json(payload)


def cmd_capture_runtime(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    snapshot = capture_runtime_snapshot(
        manifest,
        domain_id=args.domain,
        agent=args.agent,
        title=args.title,
        focus=args.focus,
        append_compat=not args.no_compat,
        render_generated=not args.no_projections,
    )
    return _print_json(snapshot)


def cmd_activate(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = build_activation(
        manifest,
        domain_id=args.domain,
        agent=args.agent,
        title=args.title,
        focus=args.focus,
        capture=not args.no_capture,
    )
    if args.format == "prompt":
        print(payload["prompt"])
        return 0
    return _print_json(payload)


def cmd_brief(args: argparse.Namespace) -> int:
    brief = build_brief(load_manifest(args.manifest), cwd=args.cwd, project=args.project, max_chars=args.budget)
    if args.format == "json":
        return _print_json(brief)
    print(brief["text"], end="")
    return 0


def cmd_startup(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = build_startup_bundle(
        manifest,
        domain_id=args.domain,
        agent=args.agent,
        title=args.title,
        focus=args.focus,
        capture=args.capture,
        limit=args.limit,
        project=args.project,
        task_id=args.task_id,
        since=args.since,
        before=args.before,
        compact=not args.full,
    )
    if args.format == "json":
        return _print_json(payload)

    print(f"# Chronicle Startup — {payload['domain']['label']} ({payload['domain']['id']})")
    print(f"Generated: {payload['generated_at']}")
    source_health = payload.get("source_health", {})
    issues = source_health.get("issues") or []
    if issues:
        print()
        print("## Source Health Issues")
        for item in issues[:8]:
            print(f"- [{item.get('severity')}] {item.get('detail')}")

    print()
    print("## Recent Events")
    if payload["recent_events"]:
        for event in payload["recent_events"]:
            print(f"- {event['recorded_at']} | {event.get('category') or 'n/a'} | {event.get('project') or 'n/a'}")
            print(f"  {event.get('text') or ''}")
    else:
        print("- No Chronicle events for this domain.")

    for source in payload["sources"]:
        print()
        print(f"## {source['label']}")
        if not source.get("exists"):
            print("(missing)")
            continue
        print(source.get("content") or "")
    return 0


def cmd_query(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = query_context(
        manifest,
        query=args.query,
        domain=args.domain,
        limit=args.limit,
        mode=args.mode,
    )
    if args.format == "json":
        return _print_json(payload)

    print(f"# Chronicle Query — {payload['query']}")
    if payload.get("domain"):
        print(f"Domain: {payload['domain']}")
    print(f"Mode: {payload['query_mode']}")

    print()
    print("## Chronicle Hits")
    if payload["chronicle_hits"]:
        for hit in payload["chronicle_hits"]:
            print(f"- {hit['recorded_at']} | {hit.get('category') or 'n/a'} | {hit.get('project') or 'n/a'}")
            print(f"  {hit.get('text') or ''}")
            if hit.get("why"):
                print(f"  why: {hit['why']}")
    else:
        print("- No Chronicle hits.")

    print()
    print("## Source Hits")
    if payload["status_hits"]:
        for hit in payload["status_hits"]:
            print(f"- {hit['source_label']}:{hit['line']} score={hit['score']}")
            print(f"  {hit['snippet']}")
    else:
        print("- No SSOT source hits.")

    print()
    print("## Mem0 Recall")
    if payload["mem0_dump_hits"]:
        for hit in payload["mem0_dump_hits"]:
            collections = ", ".join(hit.get("source_collections") or [])
            duplicate_count = int(hit.get("duplicate_count", 1))
            suffix = f" collections={collections}" if collections else ""
            print(f"- {hit.get('id') or 'memory'} score={hit['score']}{suffix}")
            print(f"  {hit.get('memory') or ''}")
            if duplicate_count > 1:
                print(f"  deduped_matches={duplicate_count}")
    else:
        print("- No Mem0 dump hits.")

    if payload["interpretation_hits"]:
        print()
        print("## Interpretation Hits")
        for hit in payload["interpretation_hits"]:
            print(f"- {hit['record_kind']} score={hit['score']}")
            print(f"  {hit.get('summary_text') or hit.get('detail') or ''}")

    if payload["scenario_hits"]:
        print()
        print("## Scenario Hits")
        for hit in payload["scenario_hits"]:
            print(f"- {hit.get('scenario_name') or hit.get('summary_text') or 'scenario'} score={hit['score']}")
            print(f"  {hit.get('summary_text') or ''}")
    return 0


def cmd_sources_audit(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = build_sources_audit(manifest, domain_id=args.domain)
    if args.format == "json":
        return _print_json(payload, strict=args.strict)

    print(f"# Chronicle Sources Audit — {payload['domain']['label']}")
    print(f"Status: {payload['status']}")
    print(f"Generated: {payload['generated_at_utc']}")
    print()
    print("## Coverage")
    print(f"- total_sources={payload['coverage']['total_sources']}")
    print(f"- enabled_sources={payload['coverage']['enabled_sources']}")
    disabled = payload["coverage"]["disabled_sensitive_lanes"]
    print(f"- disabled_sensitive_lanes={', '.join(disabled) if disabled else 'none'}")
    print()
    print("## Lanes")
    for lane in payload["coverage"]["lanes"]:
        sensitive = " sensitive" if lane["sensitive"] else ""
        enabled = "enabled" if lane["enabled"] else "disabled"
        print(f"- {lane['lane']} [{enabled}{sensitive}] sources={lane['source_count']} stale={lane['stale_sources']}")
    print()
    print("## Sources")
    for source in payload["source_catalog"]:
        enabled = "enabled" if source["enabled"] else "disabled"
        freshness = source.get("freshness_status") or "unknown"
        print(f"- {source['source_id']} lane={source['lane']} class={source['class']} trust={source['trust_tier']} freshness={freshness} {enabled}")
    return 1 if args.strict and payload["status"] != "ok" else 0


def cmd_normalize_entities(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = materialize_normalized_entities(manifest, domain_id=args.domain)
    return _print_json({"domain": args.domain, "normalized_entities": payload, "count": len(payload)})


def cmd_render_projections(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    rendered = render_projections(manifest)
    return _print_json({"rendered": rendered})


def cmd_repair_categories(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = repair_event_categories(manifest, dry_run=args.dry_run)
    return _print_json(payload)


def cmd_repair_mem0(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = repair_mem0_state(manifest, dry_run=(args.dry_run or not args.apply))
    return _print_json(payload)


def cmd_repair_stale_runs(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = repair_stale_runs(manifest, dry_run=args.dry_run, ttl_hours=args.ttl_hours)
    return _print_json(payload)


def cmd_backfill_mem0(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = backfill_mem0_queue(manifest, dry_run=(args.dry_run or not args.apply))
    return _print_json(payload)


def cmd_hook_git_commit(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_git_commit_hook(
        manifest,
        automation,
        repo_slug=args.repo,
        commit_sha=args.commit,
        repo_root=Path(args.repo_root).expanduser() if args.repo_root else None,
        trigger_source=args.trigger_source,
    )
    return _print_json(payload)


def cmd_hook_install_git(args: argparse.Namespace) -> int:
    automation = load_native_automation(args.automation_config)
    payload = install_git_hooks(automation, repos=args.repo)
    return _print_json(payload)


def cmd_curate_daybook(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_daybook(
        manifest,
        automation,
        target_date=args.date,
        trigger_source=args.trigger_source,
        regenerate=True,
    )
    return _print_json(payload)


def cmd_audit(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_audit(
        manifest,
        automation,
        weekly=args.weekly,
        trigger_source=args.trigger_source,
        force=args.force,
    )
    if payload.get("status") == "existing" and isinstance(payload.get("run"), dict):
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        existing_status = payload["run"].get("status")
        if args.strict and isinstance(existing_status, str) and existing_status in _STRICT_FAILURE_STATUSES:
            return 1
        return 0
    return _print_json(payload, strict=args.strict)


def cmd_backup(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_backup(
        manifest,
        automation,
        trigger_source=args.trigger_source,
        force=args.force,
    )
    return _print_json(payload)


def cmd_launchd_install(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = install_launchd(
        manifest,
        automation,
        agent_dir=Path(args.agent_dir).expanduser() if args.agent_dir else None,
        runtime_dir=Path(args.runtime_dir).expanduser() if args.runtime_dir else None,
        log_dir=Path(args.log_dir).expanduser() if args.log_dir else None,
        load_jobs=args.load,
    )
    return _print_json(payload)


def cmd_launchd_doctor(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = doctor_launchd(
        manifest,
        automation,
        agent_dir=Path(args.agent_dir).expanduser() if args.agent_dir else None,
        runtime_dir=Path(args.runtime_dir).expanduser() if args.runtime_dir else None,
        strict_loaded=args.strict,
    )
    return _print_json(payload, strict=args.strict)


def cmd_doctor(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    audit = run_audit(
        manifest,
        automation,
        weekly=False,
        trigger_source=args.trigger_source,
        force=True,
        persist=False,
    )
    sources = build_sources_audit(manifest, domain_id=args.domain)
    launchd = doctor_launchd(
        manifest,
        automation,
        agent_dir=Path(args.agent_dir).expanduser() if args.agent_dir else None,
        runtime_dir=Path(args.runtime_dir).expanduser() if args.runtime_dir else None,
        strict_loaded=args.strict,
    )
    payload = {
        "status": _doctor_status(audit.get("status"), sources.get("status"), launchd.get("status")),
        "generated_at_utc": audit.get("report", {}).get("generated_at_utc"),
        "strict": args.strict,
        "issue_count": int(audit.get("issue_count", 0)) + int(launchd.get("issue_count", 0)),
        "components": {
            "audit": audit,
            "sources_audit": sources,
            "launchd": launchd,
        },
    }
    return _print_json(payload, strict=args.strict, failure_statuses={"issues", "critical"})


def cmd_automation_run(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_automation_job(
        manifest,
        automation,
        job_name=args.job,
        trigger_source=args.trigger_source,
    )
    code = _print_json(payload)
    # Hard failures must surface a non-zero exit so launchd/wrappers see the
    # failure, not just the chronicle.db run record. failed_soft is an expected
    # degraded state (e.g. daybook without an LLM) and stays exit 0.
    if payload.get("status") == "failed":
        return 1
    return code


def cmd_sync_mem0(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = sync_mem0_outbox(
        manifest,
        automation,
        limit=args.limit,
        trigger_source=args.trigger_source,
    )
    return _print_json(payload)


def cmd_embed_backfill(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = embed_backfill(manifest, limit=args.limit)
    return _print_json(payload)


def cmd_notes(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if args.notes_command == "sync":
        return _print_json(sync_notes(manifest, embed=not args.no_embed, embed_limit=args.embed_limit))
    if args.notes_command == "status":
        return _print_json(notes_status(manifest))
    note = read_note(manifest, args.document_id)
    if note is None:
        print(json.dumps({"status": "not_found", "document_id": args.document_id}), file=sys.stderr)
        return 1
    return _print_json(note)


def cmd_query_memory(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = query_memory(
        manifest,
        query=args.query,
        domain=args.domain,
        limit=args.limit,
        project=args.project,
        task_id=args.task_id,
    )
    if args.format == "json":
        return _print_json(payload)

    print(f"# Chronicle Hybrid Recall — {payload['query']}")
    if payload.get("domain"):
        print(f"Domain: {payload['domain']}")
    channels = ", ".join(payload.get("channels_used") or [])
    degraded = " [DEGRADED: vector skipped]" if payload.get("degraded") else ""
    print(f"Channels: {channels or 'none'}{degraded}")
    print()
    results = payload.get("results") or []
    notes = payload.get("notes") or []
    if not results and not notes:
        print("No results.")
        return 0
    for note in notes:
        print(f"- note | {note.get('project') or 'global'} | {note['heading']} | rrf={note['rrf_score']}")
        print(f"  {note['text']}")
        print(f"  [{note['path']}, id {note['document_id']}]")
    for hit in results:
        project = hit.get("project") or "n/a"
        category = hit.get("category") or "n/a"
        print(
            f"- {hit.get('occurred_at_local') or hit.get('occurred_at_utc')} "
            f"| {category} | {project} | rrf={hit.get('rrf_score')}"
        )
        print(f"  {hit.get('text') or ''}")
        channels_detail = hit.get("channels") or {}
        parts = []
        if channels_detail.get("fts_rank") is not None:
            parts.append(f"fts_rank={channels_detail['fts_rank']}")
        if channels_detail.get("vector_similarity") is not None:
            parts.append(f"vec_sim={channels_detail['vector_similarity']}")
        if channels_detail.get("recency_rank") is not None:
            parts.append(f"recency_rank={channels_detail['recency_rank']}")
        if parts:
            print(f"  [{', '.join(parts)}]")
    return 0


def _threshold(value: str) -> tuple[str, float]:
    name, _, floor = value.partition("=")
    try:
        number = float(floor)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected METRIC=FLOOR, got {value!r}") from None
    if not math.isfinite(number):
        raise argparse.ArgumentTypeError(f"the floor must be a finite number, got {value!r}")
    return name.strip(), number


def cmd_eval(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    if args.db is not None:
        manifest = dict(manifest)
        manifest["paths"] = dict(manifest["paths"])
        manifest["paths"]["chronicle_db"] = str(args.db)
    try:
        cases = load_golden(args.golden)
    except (OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 2
    report = build_report(run_eval(manifest, cases), golden_path=args.golden)
    failures = check_thresholds(report, dict(args.fail_under))
    report["thresholds"] = {"floors": dict(args.fail_under), "failures": failures}
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if args.json:
        _print_json(report)
    else:
        print(format_report(report))
        for failure in failures:
            print(f"BELOW FLOOR {failure}")
    return 1 if failures else 0


def cmd_browse(args: argparse.Namespace) -> int:
    """Dispatcher for `chronicle browse` subcommands."""
    sub = getattr(args, "browse_command", None)
    if sub is None:
        render_browse_help()
        return 0
    return args.browse_handler(args)


def cmd_browse_search(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = query_memory(
        manifest,
        query=args.query,
        domain=args.domain,
        limit=args.limit,
    )
    render_search_results(payload)
    return 0


def cmd_browse_recent(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    config = config_from_manifest(manifest)
    events = fetch_recent_events(config, limit=args.limit, domain=args.domain)
    render_recent_events(events)
    return 0


def cmd_browse_entity(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    state = project_state(manifest, project=args.name)
    events = state.get("recent_events") or []
    relations = state.get("relations") or []
    render_entity_timeline(args.name, events, relations)
    return 0


def cmd_browse_daybook(args: argparse.Namespace) -> int:
    from .automation import load_automation_config

    automation = load_automation_config(args.automation_config)
    daybook_dir = automation.daybook_dir
    return render_daybook(daybook_dir, getattr(args, "date", None))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Chronicle foundation CLI for canonical SQLite memory."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Override path to chronicle.db",
    )
    # Resolved after parsing (see main): computing them here made `--help` and
    # explicit flags fail whenever the default workspace could not be resolved.
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Path to SSOT_MANIFEST.toml (default: the workspace's)",
    )
    parser.add_argument(
        "--automation-config",
        type=Path,
        default=None,
        help="Path to CHRONICLE_AUTOMATION.toml (default: the workspace's)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init", help="Scaffold a portable Chronicle workspace for a fresh install")
    p_init.add_argument(
        "--root",
        type=Path,
        default=Path.cwd() / "chronicle-workspace",
        help="Target directory for the new Chronicle workspace",
    )
    p_init.add_argument("--timezone", default=None, help="IANA timezone name (for example Europe/Warsaw)")
    p_init.add_argument("--force", action="store_true", help="Overwrite scaffolded files if they already exist")
    p_init.set_defaults(handler=cmd_init)

    p_migrate = sub.add_parser("migrate", help="Create chronicle.db and apply SQL migrations")
    p_migrate.set_defaults(handler=cmd_migrate)

    p_import = sub.add_parser(
        "import-legacy",
        help="Import transitional JSONL ledger/snapshots into chronicle.db",
    )
    p_import.add_argument(
        "--queue-mem0",
        action="store_true",
        help="Also seed mem0_outbox rows for imported durable events",
    )
    p_import.set_defaults(handler=cmd_import_legacy)

    p_status = sub.add_parser("status", help="Report chronicle.db health and counts")
    p_status.set_defaults(handler=cmd_status)

    p_activate = sub.add_parser("activate", help="Build the Chronicle activation contract directly from the native service")
    p_activate.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_activate.add_argument("--agent", default="codex", help="Agent name")
    p_activate.add_argument("--title", default=None, help="Optional snapshot title")
    p_activate.add_argument("--focus", default=None, help="Optional focus string")
    p_activate.add_argument("--no-capture", action="store_true", help="Reuse the latest snapshot instead of capturing a new one")
    p_activate.add_argument("--format", choices=["prompt", "json", "bundle"], default="prompt")
    p_activate.set_defaults(handler=cmd_activate)

    p_brief = sub.add_parser("brief", help="Print the compact read-only memory brief a new session starts with")
    p_brief.add_argument("--cwd", default=None, help="Working directory; its name or a configured project root picks the project")
    p_brief.add_argument("--project", default=None, help="Project slug (overrides --cwd)")
    p_brief.add_argument("--budget", type=int, default=BRIEF_DEFAULT_CHARS, help="Maximum characters (1000-8000)")
    p_brief.add_argument("--format", choices=["text", "json"], default="text")
    p_brief.set_defaults(handler=cmd_brief)

    p_startup = sub.add_parser("startup", help="Build the Chronicle startup bundle directly from the native service")
    p_startup.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_startup.add_argument("--agent", default="codex", help="Agent name")
    p_startup.add_argument("--title", default=None, help="Optional snapshot title")
    p_startup.add_argument("--focus", default=None, help="Optional focus string")
    p_startup.add_argument("--project", default=None, help="Project slug shared by cooperating agents")
    p_startup.add_argument("--task-id", default=None, help="Stable task ID within the project")
    p_startup.add_argument("--since", default=None, help="Cursor from a previous task_context response")
    p_startup.add_argument("--before", default=None, help="task_context.before cursor: read older changes")
    p_startup.add_argument("--full", action="store_true", help="Include full source content")
    p_startup.add_argument("--capture", action="store_true", help="Capture a fresh runtime snapshot before building the bundle")
    p_startup.add_argument("--limit", type=int, default=3, help="Number of recent events to include")
    p_startup.add_argument("--format", choices=["text", "json"], default="text")
    p_startup.set_defaults(handler=cmd_startup)

    p_query = sub.add_parser("query", help="Search Chronicle, active SSOT docs, and the Mem0 dump via the native service")
    p_query.add_argument("query", help="Search string")
    p_query.add_argument("--domain", default=None, help="Optional manifest domain filter")
    p_query.add_argument("--limit", type=int, default=5, help="Maximum hits per surface")
    p_query.add_argument(
        "--mode",
        choices=["truth_only", "truth_plus_interpretation", "truth_plus_interpretation_plus_scenarios"],
        default="truth_plus_interpretation",
        help="Retrieval mode for truth vs derived layers",
    )
    p_query.add_argument("--format", choices=["text", "json"], default="text")
    p_query.set_defaults(handler=cmd_query)

    p_sources_audit = sub.add_parser("sources-audit", help="Inspect source coverage, freshness, trust, and lane enablement")
    p_sources_audit.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_sources_audit.add_argument("--format", choices=["text", "json"], default="json")
    p_sources_audit.add_argument("--strict", action="store_true", help="Exit non-zero when the audit is not fully green")
    p_sources_audit.set_defaults(handler=cmd_sources_audit)

    p_normalize_entities = sub.add_parser("normalize-entities", help="Materialize normalized entities from current Chronicle truth and runtime evidence")
    p_normalize_entities.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_normalize_entities.set_defaults(handler=cmd_normalize_entities)

    p_record = sub.add_parser("record", help="Write a durable event directly to chronicle.db")
    p_record.add_argument("text", help="Durable fact or decision")
    p_record.add_argument("--id", default=None, help="Optional event id")
    p_record.add_argument("--recorded-at", default=None, help="Optional UTC timestamp")
    p_record.add_argument("--agent", default="codex", help="Agent name")
    p_record.add_argument("--domain", default="global", help="Domain id")
    p_record.add_argument("--category", default=None, help="Event category")
    p_record.add_argument("--project", default=None, help="Project key")
    p_record.add_argument("--task-id", default=None, help="Stable task ID within project")
    p_record.add_argument("--session-id", default=None, help="Originating agent session ID")
    p_record.add_argument("--request-id", default=None, help="Unique write ID reused unchanged on retries")
    p_record.add_argument("--checkpoint-file", default=None, help="JSON handoff: goal, completed, verification, open_questions, next_steps")
    p_record.add_argument("--fact-file", default=None, help="JSON assertion: slot, value, kind, optional supersedes")
    p_record.add_argument("--why", default=None, help="Reason or context")
    p_record.add_argument("--source-file", action="append", help="Source file path")
    p_record.add_argument("--mem0-status", default="queued", help="Mem0 state for outbox tracking")
    p_record.add_argument("--mem0-error", default=None, help="Optional Mem0 error text")
    p_record.set_defaults(handler=cmd_record)

    p_capture = sub.add_parser("capture", help="Write a snapshot directly to chronicle.db")
    p_capture.add_argument("--id", default=None, help="Optional snapshot id")
    p_capture.add_argument("--captured-at-utc", default=None, help="Optional UTC timestamp")
    p_capture.add_argument("--captured-at-local", default=None, help="Optional local timestamp")
    p_capture.add_argument("--timezone", default=None, help="Timezone name")
    p_capture.add_argument("--agent", default="codex", help="Agent name")
    p_capture.add_argument("--domain", default="global", help="Domain id")
    p_capture.add_argument("--title", default=None, help="Snapshot title")
    p_capture.add_argument("--focus", default=None, help="Snapshot focus")
    p_capture.add_argument("--label", default=None, help="Snapshot label")
    p_capture.set_defaults(handler=cmd_capture)

    p_recent = sub.add_parser("recent", help="Show recent Chronicle events")
    p_recent.add_argument("--limit", type=int, default=10)
    p_recent.add_argument("--domain", default=None, help="Optional domain filter")
    p_recent.add_argument("--format", choices=["text", "json"], default="text")
    p_recent.set_defaults(handler=cmd_recent)

    p_timeline = sub.add_parser("timeline", help="Query chronicle.db around a timestamp")
    p_timeline.add_argument("--at", required=True, help="Timestamp to inspect (ISO 8601)")
    p_timeline.add_argument("--domain", default=None, help="Optional domain filter")
    p_timeline.add_argument("--window-hours", type=int, default=6, help="Event window around the timestamp")
    p_timeline.add_argument("--limit", type=int, default=3, help="Number of nearest snapshots to show")
    p_timeline.add_argument("--format", choices=["text", "json"], default="text")
    p_timeline.set_defaults(handler=cmd_timeline)

    p_capture_runtime = sub.add_parser(
        "capture-runtime",
        help="Capture live runtime state into Chronicle, archive artifacts, and refresh projections",
    )
    p_capture_runtime.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_capture_runtime.add_argument("--agent", default="codex", help="Agent name")
    p_capture_runtime.add_argument("--title", default=None, help="Optional snapshot title")
    p_capture_runtime.add_argument("--focus", default=None, help="Optional focus string")
    p_capture_runtime.add_argument("--no-compat", action="store_true", help="Do not append compatibility JSONL logs")
    p_capture_runtime.add_argument("--no-projections", action="store_true", help="Skip markdown projection rendering")
    p_capture_runtime.set_defaults(handler=cmd_capture_runtime)

    p_render = sub.add_parser("render-projections", help="Render markdown projections from the latest Chronicle snapshot")
    p_render.set_defaults(handler=cmd_render_projections)

    p_repair = sub.add_parser("repair-categories", help="Normalize legacy Chronicle events with missing categories")
    p_repair.add_argument("--dry-run", action="store_true", help="Report affected rows without mutating chronicle.db")
    p_repair.set_defaults(handler=cmd_repair_categories)

    p_repair_mem0 = sub.add_parser("repair-mem0-state", help="Reconcile events.mem0_* fields from authoritative mem0_outbox rows")
    p_repair_mem0.add_argument("--dry-run", action="store_true", help="Report drift without mutating chronicle.db")
    p_repair_mem0.add_argument("--apply", action="store_true", help="Apply the reconciliation to chronicle.db")
    p_repair_mem0.set_defaults(handler=cmd_repair_mem0)

    p_repair_stale = sub.add_parser("repair-stale-runs", help="Mark stale running automation and curation rows failed")
    p_repair_stale.add_argument("--dry-run", action="store_true", help="Report stale running rows without mutating chronicle.db")
    p_repair_stale.add_argument(
        "--ttl-hours",
        type=float,
        default=DEFAULT_STALE_RUN_TTL_HOURS,
        help="Running rows older than this many hours are marked stale_failed",
    )
    p_repair_stale.set_defaults(handler=cmd_repair_stale_runs)

    p_backfill_mem0 = sub.add_parser(
        "backfill-mem0-queue",
        help="Requeue eligible skipped MCP events so high-signal backlog can sync into Mem0",
    )
    p_backfill_mem0.add_argument("--dry-run", action="store_true", help="Report eligible rows without mutating chronicle.db")
    p_backfill_mem0.add_argument("--apply", action="store_true", help="Apply the backfill and mark eligible rows pending")
    p_backfill_mem0.set_defaults(handler=cmd_backfill_mem0)

    p_guard_event = sub.add_parser(
        "guard-event",
        help="Mark an existing event as local_only without deleting it from Chronicle history",
    )
    p_guard_event.add_argument("--event-id", required=True, help="Chronicle event id")
    p_guard_event.add_argument("--verdict", required=True, choices=["local_only"], help="Guard verdict to apply")
    p_guard_event.add_argument("--reason", required=True, help="Operator reason for the guard")
    p_guard_event.add_argument("--apply", action="store_true", help="Apply the guard to chronicle.db")
    p_guard_event.set_defaults(handler=cmd_guard_event)

    p_hook = sub.add_parser("hook", help="Chronicle hook entrypoints")
    hook_sub = p_hook.add_subparsers(dest="hook_command", required=True)

    p_hook_git = hook_sub.add_parser("git-commit", help="Archive a tracked git commit into Chronicle")
    p_hook_git.add_argument("--repo", default=None, help="Tracked repo slug")
    p_hook_git.add_argument("--repo-root", default=None, help="Absolute repo path")
    p_hook_git.add_argument("--commit", required=True, help="Commit SHA")
    p_hook_git.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_hook_git.set_defaults(handler=cmd_hook_git_commit)

    p_hook_install = hook_sub.add_parser("install-git", help="Install versioned post-commit hooks into tracked repos")
    p_hook_install.add_argument("--repo", action="append", help="Install only for selected repo slug(s)")
    p_hook_install.set_defaults(handler=cmd_hook_install_git)

    p_curate = sub.add_parser("curate", help="Chronicle curator entrypoints")
    curate_sub = p_curate.add_subparsers(dest="curate_command", required=True)
    p_daybook = curate_sub.add_parser(
        "daybook",
        help="Write a day's daybook; rewrites it when that day's events changed, retries a skipped day",
    )
    p_daybook.add_argument("--date", default=None, help="Local date YYYY-MM-DD")
    p_daybook.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_daybook.set_defaults(handler=cmd_curate_daybook)

    p_audit = sub.add_parser("audit", help="Run Chronicle audit checks")
    p_audit.add_argument("--weekly", action="store_true", help="Run weekly audit mode")
    p_audit.add_argument("--force", action="store_true", help="Bypass same-day dedupe and recompute a fresh audit run")
    p_audit.add_argument("--strict", action="store_true", help="Exit non-zero when the audit is not fully green")
    p_audit.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_audit.set_defaults(handler=cmd_audit)

    p_doctor = sub.add_parser("doctor", help="Run a single Chronicle readiness check across audit, source freshness, and launchd")
    p_doctor.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_doctor.add_argument("--strict", action="store_true", help="Require launchd jobs to be loaded and exit non-zero on any issue")
    p_doctor.add_argument("--agent-dir", default=None, help="Override LaunchAgents directory")
    p_doctor.add_argument("--runtime-dir", default=None, help="Override wrapper scripts directory")
    p_doctor.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_doctor.set_defaults(handler=cmd_doctor)

    p_backup = sub.add_parser("backup", help="Run Chronicle backup to the configured target")
    p_backup.add_argument("--force", action="store_true", help="Ignore the backup interval guard")
    p_backup.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_backup.set_defaults(handler=cmd_backup)

    p_launchd = sub.add_parser("launchd", help="Install or inspect Chronicle LaunchAgents")
    launchd_sub = p_launchd.add_subparsers(dest="launchd_command", required=True)
    p_launchd_install = launchd_sub.add_parser("install", help="Install LaunchAgents and wrapper scripts")
    p_launchd_install.add_argument("--agent-dir", default=None, help="Override LaunchAgents directory")
    p_launchd_install.add_argument("--runtime-dir", default=None, help="Override wrapper scripts directory")
    p_launchd_install.add_argument("--log-dir", default=None, help="Override launchd logs directory")
    p_launchd_install.add_argument("--load", action="store_true", help="Load jobs with launchctl after install")
    p_launchd_install.set_defaults(handler=cmd_launchd_install)

    p_launchd_doctor = launchd_sub.add_parser("doctor", help="Validate LaunchAgent installation and load state")
    p_launchd_doctor.add_argument("--agent-dir", default=None, help="Override LaunchAgents directory")
    p_launchd_doctor.add_argument("--runtime-dir", default=None, help="Override wrapper scripts directory")
    p_launchd_doctor.add_argument("--strict", action="store_true", help="Treat unloaded jobs as issues and exit non-zero on failure")
    p_launchd_doctor.set_defaults(handler=cmd_launchd_doctor)

    p_automation = sub.add_parser("automation", help="Run Chronicle scheduled jobs")
    automation_sub = p_automation.add_subparsers(dest="automation_command", required=True)
    p_automation_run = automation_sub.add_parser("run", help="Run a native automation job")
    p_automation_run.add_argument(
        "--job",
        required=True,
        choices=["daily-capture", "daybook", "mem0-dump", "weekly-audit", "backup"],
        help="Job to execute",
    )
    p_automation_run.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_automation_run.set_defaults(handler=cmd_automation_run)

    p_sync_mem0 = sub.add_parser("sync-mem0", help="Replay queued Mem0 writes directly from Chronicle mem0_outbox")
    p_sync_mem0.add_argument("--limit", type=int, default=None, help="Maximum outbox rows to process")
    p_sync_mem0.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_sync_mem0.set_defaults(handler=cmd_sync_mem0)

    p_embed_backfill = sub.add_parser(
        "embed-backfill",
        help="Embed the events the active embedding model's index lacks (requires Ollama)",
    )
    p_embed_backfill.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum events to embed in this run (default: all missing)",
    )
    p_embed_backfill.set_defaults(handler=cmd_embed_backfill)

    p_notes = sub.add_parser("notes", help="Index notes written on purpose (FR-10): sync, status, show")
    notes_sub = p_notes.add_subparsers(dest="notes_command", required=True)
    p_notes_sync = notes_sub.add_parser("sync", help="Re-index changed notes and tombstone deleted ones")
    p_notes_sync.add_argument("--no-embed", action="store_true", help="Skip embedding new sections")
    p_notes_sync.add_argument("--embed-limit", type=int, default=None, help="Maximum sections to embed")
    notes_sub.add_parser("status", help="How many notes, sections and vectors the index holds")
    p_notes_show = notes_sub.add_parser("show", help="A note's indexed (redacted) text")
    p_notes_show.add_argument("document_id")
    p_notes.set_defaults(handler=cmd_notes)

    p_query_memory = sub.add_parser(
        "query-memory",
        help="Hybrid recall: RRF-fused FTS + vector + temporal search over Chronicle events",
    )
    p_query_memory.add_argument("query", help="Natural-language search string")
    p_query_memory.add_argument("--domain", default=None, help="Optional Chronicle domain filter")
    p_query_memory.add_argument("--project", default=None, help="Project slug")
    p_query_memory.add_argument("--task-id", default=None, help="Task ID within project")
    p_query_memory.add_argument("--limit", type=int, default=10, help="Maximum results to return")
    p_query_memory.add_argument("--format", choices=["text", "json"], default="text")
    p_query_memory.set_defaults(handler=cmd_query_memory)

    p_eval = sub.add_parser(
        "eval",
        help="Score recall against a golden question set (JSONL): hit@k, MRR, abstention, latency",
    )
    p_eval.add_argument("--golden", type=Path, required=True, help="Golden set, one JSON case per line")
    p_eval.add_argument("--json", action="store_true", help="Print the full JSON report")
    p_eval.add_argument("--out", type=Path, default=None, help="Also write the JSON report to this file")
    p_eval.add_argument(
        "--fail-under",
        type=_threshold,
        action="append",
        default=[],
        metavar="METRIC=FLOOR",
        help="Exit 1 when an overall metric is below its floor, e.g. hit@5=0.6 (repeatable); "
        "a failed query always exits 1",
    )
    p_eval.set_defaults(handler=cmd_eval)

    # ------------------------------------------------------------------
    # browse — human-facing navigation (read-only)
    # ------------------------------------------------------------------
    p_browse = sub.add_parser(
        "browse",
        help="Human-facing archive navigation (search, recent, entity, daybook)",
    )
    p_browse.set_defaults(handler=cmd_browse, browse_command=None)
    browse_sub = p_browse.add_subparsers(dest="browse_command")

    p_bs = browse_sub.add_parser("search", help="Hybrid recall search with ranked readable output")
    p_bs.add_argument("query", help="Natural-language search string")
    p_bs.add_argument("--limit", type=int, default=10, help="Maximum results")
    p_bs.add_argument("--domain", default=None, help="Optional domain filter")
    p_bs.set_defaults(browse_handler=cmd_browse_search)

    p_br = browse_sub.add_parser("recent", help="Recent events as a reverse-chron timeline")
    p_br.add_argument("--limit", type=int, default=10, help="Maximum results")
    p_br.add_argument("--domain", default=None, help="Optional domain filter")
    p_br.set_defaults(browse_handler=cmd_browse_recent)

    p_be = browse_sub.add_parser("entity", help="Events + relations touching a named entity")
    p_be.add_argument("name", help="Entity name (project, system, company, etc.)")
    p_be.set_defaults(browse_handler=cmd_browse_entity)

    p_bd = browse_sub.add_parser("daybook", help="Print a daybook markdown file")
    p_bd.add_argument("--date", default=None, help="Date YYYY-MM-DD (default: list recent)")
    p_bd.set_defaults(browse_handler=cmd_browse_daybook)

    return parser


def _print_error(exc: Exception, *, exit_code: int) -> int:
    print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc)}, ensure_ascii=False, indent=2))
    return exit_code


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        # `init` creates a workspace, so it must not require one to exist.
        if args.command != "init":
            # An explicit path names the workspace; only without one do the
            # environment and the implicit fallback come into play.
            root = resolve_status_root(manifest_path=args.manifest, automation_path=args.automation_config)
            if args.manifest is None:
                args.manifest = default_manifest_path(root)
            if args.automation_config is None:
                args.automation_config = default_automation_path(root)
        return args.handler(args)
    except ChronicleConfigError as exc:
        return _print_error(exc, exit_code=EXIT_CONFIG_ERROR)
    except MigrationError as exc:
        return _print_error(exc, exit_code=EXIT_SCHEMA_ACTION)


if __name__ == "__main__":
    raise SystemExit(main())
