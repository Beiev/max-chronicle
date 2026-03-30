from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

from .bootstrap import bootstrap_legacy
from .config import ChronicleConfig, default_config, ensure_runtime_dirs
from .db import apply_migrations, connect, database_summary
from .native_automation import (
    doctor_launchd,
    install_git_hooks,
    install_launchd,
    load_native_automation,
    run_audit,
    run_automation_job,
    run_backup,
    run_daybook,
    run_digest_hook,
    run_git_commit_hook,
    run_minimax_canary,
    run_minimax_smoke,
    sync_mem0_outbox,
)
from .runtime_context import load_manifest
from .service import (
    backfill_mem0_queue,
    build_activation,
    build_sources_audit,
    build_startup_bundle,
    capture_runtime_snapshot,
    guard_event,
    materialize_normalized_entities,
    materialize_situation_model,
    query_context,
    record_scenario,
    reconstruct_timeline,
    record_event,
    review_scenario,
    render_projections,
    repair_event_categories,
    repair_mem0_state,
    run_lenses,
)
from .scaffold import scaffold_workspace
from .store import config_from_manifest, fetch_recent_events, parse_when, store_snapshot


def _connection(config: ChronicleConfig):
    ensure_runtime_dirs(config)
    return connect(config.db_path)


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
        applied = apply_migrations(connection, config)
        summary = database_summary(connection)
    payload = {
        "db_path": str(config.db_path),
        "applied": [
            {"version": item.version, "name": item.name, "path": str(item.path)}
            for item in applied
        ],
        "summary": summary,
    }
    return _print_json(payload)


def cmd_import_legacy(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    with _connection(config) as connection:
        apply_migrations(connection, config)
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

    with _connection(config) as connection:
        apply_migrations(connection, config)
        summary = database_summary(connection)
    payload = {
        "db_path": str(config.db_path),
        "exists": True,
        "summary": summary,
    }
    return _print_json(payload)


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


def cmd_build_situation(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = materialize_situation_model(manifest, domain_id=args.domain)
    return _print_json(payload)


def cmd_run_lenses(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = run_lenses(manifest, domain_id=args.domain, persist=not args.no_persist)
    return _print_json({"domain": args.domain, "lens_runs": payload, "count": len(payload)})


def cmd_record_scenario(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    changed_variables: dict[str, str] = {}
    for item in args.changed_variable or []:
        if "=" not in item:
            raise ValueError(f"Invalid changed variable: {item}. Expected key=value.")
        key, value = item.split("=", 1)
        changed_variables[key.strip()] = value.strip()
    payload = record_scenario(
        manifest,
        domain_id=args.domain,
        scenario_name=args.name,
        assumptions=args.assumption or [],
        changed_variables=changed_variables,
        expected_outcomes=args.expected_outcome or [],
        failure_modes=args.failure_mode or [],
        confidence=args.confidence,
        review_due_at=args.review_due_at,
    )
    return _print_json(payload)


def cmd_review_scenario(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = review_scenario(
        manifest,
        scenario_id=args.scenario_id,
        status=args.status,
        review_summary=args.summary,
        outcome_event_id=args.outcome_event_id,
    )
    return _print_json(payload)


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


def cmd_backfill_mem0(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    payload = backfill_mem0_queue(manifest, dry_run=(args.dry_run or not args.apply))
    return _print_json(payload)


def cmd_hook_digest_run(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_digest_hook(
        manifest,
        automation,
        status_path=Path(args.status_path).expanduser() if args.status_path else None,
        synthesis_path=Path(args.synthesis_path).expanduser() if args.synthesis_path else None,
        previous_summary_path=Path(args.previous_summary_path).expanduser() if args.previous_summary_path else None,
        log_path=Path(args.log_path).expanduser() if args.log_path else None,
        trigger_source=args.trigger_source,
    )
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
    return _print_json(payload)


def cmd_minimax_smoke(args: argparse.Namespace) -> int:
    automation = load_native_automation(args.automation_config)
    payload = run_minimax_smoke(automation, prompt=args.prompt)
    return _print_json(payload)


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


def cmd_minimax_canary(args: argparse.Namespace) -> int:
    manifest = load_manifest(args.manifest)
    automation = load_native_automation(args.automation_config)
    payload = run_minimax_canary(
        manifest,
        automation,
        trigger_source=args.trigger_source,
    )
    return _print_json(payload)


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
    parser.add_argument(
        "--manifest",
        type=Path,
        default=default_config().manifest_path,
        help="Path to SSOT_MANIFEST.toml",
    )
    parser.add_argument(
        "--automation-config",
        type=Path,
        default=default_config().automation_path,
        help="Path to CHRONICLE_AUTOMATION.toml",
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

    p_startup = sub.add_parser("startup", help="Build the Chronicle startup bundle directly from the native service")
    p_startup.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_startup.add_argument("--agent", default="codex", help="Agent name")
    p_startup.add_argument("--title", default=None, help="Optional snapshot title")
    p_startup.add_argument("--focus", default=None, help="Optional focus string")
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

    p_build_situation = sub.add_parser("build-situation", help="Materialize the latest situation model for a domain")
    p_build_situation.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_build_situation.set_defaults(handler=cmd_build_situation)

    p_run_lenses = sub.add_parser("run-lenses", help="Materialize fixed lens runs for the latest situation model")
    p_run_lenses.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_run_lenses.add_argument("--no-persist", action="store_true", help="Build lens outputs without storing them")
    p_run_lenses.set_defaults(handler=cmd_run_lenses)

    p_record_scenario = sub.add_parser("record-scenario", help="Create a scenario run against the latest situation model")
    p_record_scenario.add_argument("--domain", default="global", help="Domain id from the manifest")
    p_record_scenario.add_argument("--name", required=True, help="Scenario name")
    p_record_scenario.add_argument("--assumption", action="append", help="Scenario assumption (repeatable)")
    p_record_scenario.add_argument("--changed-variable", action="append", help="Changed variable key=value (repeatable)")
    p_record_scenario.add_argument("--expected-outcome", action="append", help="Expected outcome (repeatable)")
    p_record_scenario.add_argument("--failure-mode", action="append", help="Failure mode (repeatable)")
    p_record_scenario.add_argument("--confidence", type=float, default=0.6, help="Scenario confidence 0-1")
    p_record_scenario.add_argument("--review-due-at", default=None, help="Optional ISO timestamp for review due date")
    p_record_scenario.set_defaults(handler=cmd_record_scenario)

    p_review_scenario = sub.add_parser("review-scenario", help="Store a replay/eval review for a scenario run")
    p_review_scenario.add_argument("--scenario-id", required=True, help="Scenario id")
    p_review_scenario.add_argument("--status", required=True, choices=["matched", "partial", "missed"], help="Review outcome")
    p_review_scenario.add_argument("--summary", required=True, help="Review summary")
    p_review_scenario.add_argument("--outcome-event-id", default=None, help="Optional supporting Chronicle event id")
    p_review_scenario.set_defaults(handler=cmd_review_scenario)

    p_record = sub.add_parser("record", help="Write a durable event directly to chronicle.db")
    p_record.add_argument("text", help="Durable fact or decision")
    p_record.add_argument("--id", default=None, help="Optional event id")
    p_record.add_argument("--recorded-at", default=None, help="Optional UTC timestamp")
    p_record.add_argument("--agent", default="codex", help="Agent name")
    p_record.add_argument("--domain", default="global", help="Domain id")
    p_record.add_argument("--category", default=None, help="Event category")
    p_record.add_argument("--project", default=None, help="Project key")
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

    p_hook_digest = hook_sub.add_parser("digest-run", help="Archive a Digest pipeline run into Chronicle")
    p_hook_digest.add_argument("--status-path", default=None, help="Override Digest last_status.json path")
    p_hook_digest.add_argument("--synthesis-path", default=None, help="Override Digest synthesis.md path")
    p_hook_digest.add_argument("--previous-summary-path", default=None, help="Override previous_summary.txt path")
    p_hook_digest.add_argument("--log-path", default=None, help="Override Digest log path")
    p_hook_digest.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_hook_digest.set_defaults(handler=cmd_hook_digest_run)

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
    p_daybook = curate_sub.add_parser("daybook", help="Generate a daybook markdown artifact from Chronicle delta")
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
        choices=["daily-capture", "daybook", "mem0-dump", "company-intel", "minimax-canary", "weekly-audit", "backup"],
        help="Job to execute",
    )
    p_automation_run.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_automation_run.set_defaults(handler=cmd_automation_run)

    p_sync_mem0 = sub.add_parser("sync-mem0", help="Replay queued Mem0 writes directly from Chronicle mem0_outbox")
    p_sync_mem0.add_argument("--limit", type=int, default=None, help="Maximum outbox rows to process")
    p_sync_mem0.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_sync_mem0.set_defaults(handler=cmd_sync_mem0)

    p_canary = sub.add_parser("minimax-canary", help="Run the MiniMax health canary and archive the result")
    p_canary.add_argument("--trigger-source", default="manual", help="Trigger source label")
    p_canary.set_defaults(handler=cmd_minimax_canary)

    p_minimax = sub.add_parser("minimax-smoke", help="Run a live MiniMax API smoke test with the configured endpoint/model")
    p_minimax.add_argument("--prompt", default="Reply with OK only.", help="Short smoke-test prompt")
    p_minimax.set_defaults(handler=cmd_minimax_smoke)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
