from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .config import default_config
from .db import connect, ensure_schema


def _detect_timezone() -> str:
    tzinfo = datetime.now().astimezone().tzinfo
    key = getattr(tzinfo, "key", None)
    if isinstance(key, str) and key:
        return key
    return "UTC"


def _path(path: Path) -> str:
    return str(path)


def _write_text(
    path: Path,
    content: str,
    *,
    created: list[str],
    skipped: list[str],
    force: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        skipped.append(str(path))
        return
    path.write_text(content, encoding="utf-8")
    created.append(str(path))


def _write_json(
    path: Path,
    payload: Any,
    *,
    created: list[str],
    skipped: list[str],
    force: bool,
) -> None:
    _write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        created=created,
        skipped=skipped,
        force=force,
    )


def _write_executable(
    path: Path,
    content: str,
    *,
    created: list[str],
    skipped: list[str],
    force: bool,
) -> None:
    before = path.exists()
    _write_text(path, content, created=created, skipped=skipped, force=force)
    if (not before) or force:
        path.chmod(0o755)


def _manifest_template(root: Path, timezone_name: str) -> str:
    """A fresh installation has no invented projects or external feeds."""
    paths = {
        "status_root": root, "chronicle_db": root / "chronicle.db",
        "chronicle_artifact_dir": root / "chronicle-artifacts",
        "chronicle_package_root": root, "ledger_file": root / "ssot-ledger.jsonl",
        "snapshot_file": root / "chronicle-snapshots.jsonl",
        "mem0_dump": root / "mem0-dump.json", "mem0_bridge": root / "scripts/mem0_bridge.py",
        "workspace_root": root / "workspaces",
    }
    lines = ["version = 1", 'title = "Chronicle Workspace"', "", "[settings]",
             f"timezone = {json.dumps(timezone_name)}", "", "[paths]"]
    lines.extend(f"{key} = {json.dumps(str(value))}" for key, value in paths.items())
    sources = [
        ("status", "Status", "status.md", "ssot"),
        ("priorities", "Priorities", "priorities.md", "strategy"),
        ("memory_system", "Memory System", "MEMORY_SYSTEM.md", "memory_policy"),
        ("chronicle_protocol", "Agent Protocol", "CHRONICLE_PROTOCOL.md", "timeline_protocol"),
    ]
    for source_id, label, path, role in sources:
        lines.extend(["", "[[sources]]", f'id = "{source_id}"', f'label = "{label}"',
                      f"path = {json.dumps(str(root / path))}", 'kind = "markdown"',
                      f'role = "{role}"', 'trust_tier = "operator_curated"', 'priority = 80'])
    for domain, label in [("global", "Shared Memory"), ("memory", "Memory Architecture")]:
        lines.extend(["", "[[domains]]", f'id = "{domain}"', f'label = "{label}"',
                      'source_ids = ["status", "priorities", "memory_system", "chronicle_protocol"]',
                      "mem0_queries = []"])
    return "\n".join(lines) + "\n"


def _automation_template(root: Path) -> str:
    runtime_root = root / "runtime"
    return f"""version = 1
title = "Chronicle Native Automation"

[paths]
backup_root = "{_path(root / 'backups')}"
env_file = "{_path(root / '.env')}"
launch_agent_dir = "{_path(runtime_root / 'launch-agents')}"
launchd_runtime_dir = "{_path(runtime_root / 'launchd')}"
launchd_log_dir = "{_path(root / 'logs' / 'launchd')}"
daybook_dir = "{_path(root / 'daybooks')}"
git_hooks_dir = "{_path(root / 'git-hooks')}"

[guards]
daybook_per_day = 1
weekly_audit_per_week = 1
backup_interval_days = 2
projection_stale_hours = 36
snapshot_stale_hours = 30
mem0_sync_batch_size = 25
jsonl_rotate_mb = 25
artifact_store_warn_gb = 6

[[repos]]
slug = "status"
path = "{_path(root)}"

[jobs.daily_capture]
hour = 2
minute = 20
throttle_seconds = 600

[jobs.daybook]
hour = 23
minute = 15
throttle_seconds = 600

[jobs.mem0_dump]
hour = 7
minute = 0
throttle_seconds = 900

[jobs.weekly_audit]
weekday = 0
hour = 6
minute = 10
throttle_seconds = 1800

[jobs.backup]
hour = 4
minute = 30
throttle_seconds = 1800
"""


def _status_template() -> str:
    return """# Status

## Active Projects
- Chronicle workspace initialized.
- Replace placeholder bullets with your real active projects.

## Decisions Log
- Chronicle is the canonical truth layer for durable events and snapshots.

## Priorities
- Define the current critical chain here.

## Blockers / Waiting
- None.

<!-- BEGIN GENERATED:CHRONICLE_STATUS -->
placeholder
<!-- END GENERATED:CHRONICLE_STATUS -->
"""


def _priorities_template() -> str:
    return """# Priorities

## Strategic Context
- Replace this with the current mission, runway, and operating constraints.

## Priority Stack (ordered)
1. Define the main shipping goal.
2. Define the next enabling goal.
3. Define maintenance-only surfaces.

## Anti-Patterns
- Do not use memory tooling as procrastination.
- Do not rewrite solved systems when integration is enough.

## Key Constraints
- Chronicle is truth.
- Mem0 and external signals are derived.
- Record new facts instead of rewriting history.
"""


def _job_search_template() -> str:
    return """# Pipeline Status

> Generated by Chronicle after the first runtime capture.

## Metrics
- Placeholder.
"""


def _memory_system_template() -> str:
    return """# Memory System

## 1. Truth Model

- `chronicle.db` is canonical truth.
- `status/*.md` are readable projections and operator docs.
- `Mem0` and other semantic recall layers are derived, not truth.
- Compatibility JSONL logs are transitional audit trails.

## 2. Main Layers

### L0 — Chronicle
- Durable events, snapshots, automation history, artifacts, and derived queues.

### L1 — SSOT Markdown
- Small human-readable context surface for operators and agents.

### L2 — Durable Docs
- ADRs, schema notes, runbooks, and project conventions.

### L3 — Optional Recall
- Semantic search systems such as Mem0 or vector stores.

## 5. How Optional Integrations Work

- Optional integrations may enrich Chronicle.
- Optional integrations must never replace Chronicle truth.
- If an integration is unavailable, Chronicle remains correct.

## 7. How Activation Works

- Agents build startup context from Chronicle events, selected SSOT docs, runtime evidence, and optional recall.
- New installs should run `chronicle startup --domain global --format json` after initialization.
"""


def _protocol_template(root: Path) -> str:
    return f"""# Chronicle Protocol

## Command Surface

Core commands:

```bash
chronicle init --root "{_path(root)}"
chronicle migrate
chronicle status
chronicle activate --domain global
chronicle startup --domain global --format json
chronicle query "current blockers" --domain global --format json
chronicle capture-runtime --domain global
chronicle render-projections
chronicle recent --limit 5
chronicle timeline --at "2026-03-15T12:55:00+01:00"
chronicle-mcp --profile chronicler
```

## Data Model

1. `chronicle.db` is truth.
2. `status/*.md` are readable projections.
3. `mem0-dump.json` is optional derived recall.
4. `chronicle-snapshots.jsonl` and `ssot-ledger.jsonl` are compatibility logs.

## Shared Agent Memory

Use a stable project and task_id across sessions and agents. startup_bundle returns
current facts, the latest checkpoint, changes, and a cursor. Pass that cursor as
since to resume the change feed. A record is an attributed assertion; inspect its
evidence and whether its kind is observed, decision, or assumption.

record_event accepts request_id (reuse unchanged on retry), session_id, task_id,
checkpoint (goal, completed, verification, open_questions, next_steps), and fact
(slot, value, kind, optional supersedes). Changing a fact requires the current
fact ID. Missing evidence is reported in the receipt; it is not archived content.

## Operational Constraint

- Startup/activation should happen before mutating Chronicle from MCP.
- Optional integrations may be absent on a fresh install; Chronicle should still run safely.
"""


def _runbook_template() -> str:
    return """# Agent Runbook

- Start with startup_bundle(project=..., task_id=...).
- Search with query_memory before making assumptions.
- Record significant decisions and evidence with request_id for safe retries.
- Record a checkpoint before handoff; include verification, unknowns, and next steps.
- Explicit facts use slot/value/kind; replacing a fact requires its current ID.
- Keep operational rules short and explicit.
- Prefer linking to durable docs over duplicating policy everywhere.
"""


def _adr_template() -> str:
    return """# ADR-0001 Canonical Memory

Chronicle is the canonical truth layer.
Derived recall systems may assist retrieval, but they must not redefine truth.
"""


def _schema_template() -> str:
    return """# Schema v10

- events
- snapshots
- artifacts
- automation_runs
- hook_events
- normalized_entities
- event_observations (agent identity, request receipts, checkpoints)
- facts / episodes / fact_observations / fact_supersessions
- event_embeddings (optional local vector index)
"""


def _checklist_template() -> str:
    return """# Implementation Checklist

- [x] Chronicle workspace initialized
- [ ] Replace placeholder SSOT docs with real operator context
- [ ] Connect optional runtime sources if needed
- [ ] Configure automation only after core capture/query flow works
"""


def _gitignore_template() -> str:
    return """chronicle.db
chronicle.db-shm
chronicle.db-wal
chronicle-artifacts/
chronicle-snapshots.jsonl
ssot-ledger.jsonl
logs/
daybooks/
backups/
"""


def _mem0_stub_template() -> str:
    return """#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys


def _dump(args: list[str]) -> int:
    output = Path("mem0-dump.json")
    if "--output" in args:
        output = Path(args[args.index("--output") + 1]).expanduser()
    payload = {
        "snapshot_type": "stub",
        "total_memories": 0,
        "collections": {},
        "collection_totals": {},
        "memories": [],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\\n", encoding="utf-8")
    print(json.dumps({"status": "ok", "output": str(output), "message": "Mem0 stub wrote an empty dump."}, ensure_ascii=False))
    return 0


def main() -> int:
    args = sys.argv[1:]
    if args and args[0] == "dump":
        return _dump(args[1:])

    print(
        json.dumps(
            {
                "status": "disabled",
                "message": "Mem0 bridge is not configured in this workspace yet.",
            },
            ensure_ascii=False,
        )
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
"""


def scaffold_workspace(
    root: Path,
    *,
    force: bool = False,
    timezone_name: str | None = None,
) -> dict[str, Any]:
    resolved_root = root.expanduser().resolve()
    timezone_value = timezone_name or _detect_timezone()
    created: list[str] = []
    skipped: list[str] = []

    base_files = {
        resolved_root / "SSOT_MANIFEST.toml": _manifest_template(resolved_root, timezone_value),
        resolved_root / "CHRONICLE_AUTOMATION.toml": _automation_template(resolved_root),
        resolved_root / "status.md": _status_template(),
        resolved_root / "priorities.md": _priorities_template(),
        resolved_root / "job-search-status.md": _job_search_template(),
        resolved_root / "MEMORY_SYSTEM.md": _memory_system_template(),
        resolved_root / "CHRONICLE_PROTOCOL.md": _protocol_template(resolved_root),
        resolved_root / "docs" / "ADR-0001-canonical-memory.md": _adr_template(),
        resolved_root / "docs" / "SCHEMA_V1.md": _schema_template(),
        resolved_root / "docs" / "IMPLEMENTATION_CHECKLIST.md": _checklist_template(),
        resolved_root / ".gitignore": _gitignore_template(),
        resolved_root / ".env.example": "# Optional secrets for Chronicle integrations.\n",
        resolved_root / "ssot-ledger.jsonl": "",
        resolved_root / "chronicle-snapshots.jsonl": "",
    }
    for path, content in base_files.items():
        _write_text(path, content, created=created, skipped=skipped, force=force)

    json_files: dict[Path, Any] = {
        resolved_root / "mem0-dump.json": {
            "snapshot_type": "stub",
            "total_memories": 0,
            "collection_totals": {},
            "collections": {},
            "memories": [],
        },
    }
    for path, payload in json_files.items():
        _write_json(path, payload, created=created, skipped=skipped, force=force)

    for directory in (
        resolved_root / "chronicle-artifacts",
        resolved_root / "backups",
        resolved_root / "daybooks",
        resolved_root / "git-hooks",
        resolved_root / "logs" / "launchd",
        resolved_root / "runtime" / "launch-agents",
        resolved_root / "runtime" / "launchd",
        resolved_root / "workspaces",
        resolved_root / "scripts",
    ):
        directory.mkdir(parents=True, exist_ok=True)

    _write_executable(
        resolved_root / "scripts" / "mem0_bridge.py",
        _mem0_stub_template(),
        created=created,
        skipped=skipped,
        force=force,
    )

    config = default_config(
        manifest_path=resolved_root / "SSOT_MANIFEST.toml",
        automation_path=resolved_root / "CHRONICLE_AUTOMATION.toml",
        status_root=resolved_root,
    )
    # `init` is an explicit command, so it may upgrade an existing workspace's
    # schema (after a backup). sqlite3's own context manager only commits, so
    # close the connection explicitly.
    connection = connect(config.db_path)
    try:
        with connection:
            ensure_schema(connection, config, allow_upgrade=True)
    finally:
        connection.close()

    return {
        "status": "ok",
        "root": str(resolved_root),
        "manifest_path": str(resolved_root / "SSOT_MANIFEST.toml"),
        "automation_path": str(resolved_root / "CHRONICLE_AUTOMATION.toml"),
        "db_path": str(config.db_path),
        "timezone": timezone_value,
        "created": created,
        "skipped": skipped,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "next_steps": [
            f"Set CHRONICLE_ROOT={resolved_root}",
            "Review SSOT_MANIFEST.toml and CHRONICLE_AUTOMATION.toml",
            "Run `chronicle status`",
            "Run `chronicle startup --domain global --format json`",
        ],
    }
