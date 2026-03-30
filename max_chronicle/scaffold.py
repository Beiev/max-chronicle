from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .config import default_config
from .db import apply_migrations, connect


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
    runtime_root = root / "runtime"
    workspaces_root = root / "workspaces"
    docs_root = root / "docs"
    return f"""version = 1
title = "Chronicle Workspace"

[settings]
timezone = "{timezone_name}"

[paths]
status_root = "{_path(root)}"
ledger_file = "{_path(root / 'ssot-ledger.jsonl')}"
snapshot_file = "{_path(root / 'chronicle-snapshots.jsonl')}"
chronicle_db = "{_path(root / 'chronicle.db')}"
chronicle_artifact_dir = "{_path(root / 'chronicle-artifacts')}"
chronicle_package_root = "{_path(root)}"
mem0_dump = "{_path(root / 'mem0-dump.json')}"
mem0_bridge = "{_path(root / 'scripts' / 'mem0_bridge.py')}"
env_file = "{_path(root / '.env')}"
workspace_root = "{_path(workspaces_root)}"
openclaw_memory_dir = "{_path(runtime_root / 'openclaw' / 'memory')}"
openclaw_state_json = "{_path(runtime_root / 'openclaw' / 'state.json')}"
openclaw_brief_json = "{_path(runtime_root / 'openclaw' / 'morning-brief.json')}"
openclaw_leads_json = "{_path(runtime_root / 'openclaw' / 'leads.json')}"
openclaw_email_triage_json = "{_path(runtime_root / 'openclaw' / 'email-triage-latest.json')}"
digest_status_json = "{_path(runtime_root / 'digest' / 'last_status.json')}"
digest_synthesis_md = "{_path(runtime_root / 'digest' / 'synthesis.md')}"
digest_previous_summary = "{_path(runtime_root / 'digest' / 'previous_summary.txt')}"
company_intel_json = "{_path(root / 'company-intel.json')}"
portfolio_asset_manifest = "{_path(runtime_root / 'portfolio' / 'asset-manifest.ts')}"
portfolio_repo = "{_path(workspaces_root / 'portfolio')}"
remotion_repo = "{_path(workspaces_root / 'remotion')}"
fcp_sorter_repo = "{_path(workspaces_root / 'fcp-sorter')}"
intel_digest_repo = "{_path(workspaces_root / 'intel-digest')}"

[[lanes]]
id = "work"
label = "Work"
sensitive = false
default_enabled = true

[[lanes]]
id = "agents"
label = "Agents"
sensitive = false
default_enabled = true

[[lanes]]
id = "world"
label = "World"
sensitive = false
default_enabled = true

[[lanes]]
id = "career_market"
label = "Career Market"
sensitive = false
default_enabled = true

[[lanes]]
id = "companies"
label = "Companies"
sensitive = false
default_enabled = true

[[lanes]]
id = "life_admin"
label = "Life Admin"
sensitive = true
default_enabled = false

[[lanes]]
id = "decisions"
label = "Decisions"
sensitive = false
default_enabled = true

[[lanes]]
id = "vectors"
label = "Vectors"
sensitive = false
default_enabled = true

[[runtime_sources]]
id = "portfolio_asset_manifest"
label = "Portfolio Asset Manifest"
path_key = "portfolio_asset_manifest"
lane = "work"
trust_tier = "canonical"
owner = "chronicle"
questions_it_can_answer = ["What asset readiness or missing-media signals exist right now?"]
questions_it_cannot_answer = ["Anything outside the portfolio/media pipeline."]

[[runtime_sources]]
id = "openclaw_state_json"
label = "Agent Runtime State"
path_key = "openclaw_state_json"
lane = "agents"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["What does the latest agent runtime state say?"]
questions_it_cannot_answer = ["Whether the current strategy is correct without corroboration."]

[[runtime_sources]]
id = "openclaw_brief_json"
label = "Agent Morning Brief"
path_key = "openclaw_brief_json"
lane = "agents"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["What was the latest operator-facing brief?"]
questions_it_cannot_answer = ["Canonical truth when Chronicle disagrees."]

[[runtime_sources]]
id = "openclaw_leads_json"
label = "Leads / Queue"
path_key = "openclaw_leads_json"
lane = "career_market"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["Which queued items or leads are active right now?"]
questions_it_cannot_answer = ["Whether a queued item is strategically valuable on its own."]

[[runtime_sources]]
id = "company_intel_json"
label = "Company Intel"
path_key = "company_intel_json"
lane = "companies"
trust_tier = "reference"
owner = "chronicle"
questions_it_can_answer = ["What company-level supporting context exists?"]
questions_it_cannot_answer = ["Canonical truth without corroboration."]

[[runtime_sources]]
id = "digest_status_json"
label = "Digest Status"
path_key = "digest_status_json"
lane = "world"
trust_tier = "canonical"
owner = "chronicle"
questions_it_can_answer = ["Did the external digest run successfully?"]
questions_it_cannot_answer = ["Current project truth."]

[[runtime_sources]]
id = "digest_synthesis_md"
label = "Digest Synthesis"
path_key = "digest_synthesis_md"
lane = "world"
trust_tier = "reference"
owner = "chronicle"
questions_it_can_answer = ["What external developments are currently salient?"]
questions_it_cannot_answer = ["Directly verified first-party truth."]

[[runtime_sources]]
id = "digest_previous_summary"
label = "Digest Previous Summary"
path_key = "digest_previous_summary"
lane = "world"
trust_tier = "reference"
owner = "chronicle"
questions_it_can_answer = ["What was salient in the prior digest window?"]
questions_it_cannot_answer = ["The latest real-time external state."]

[[runtime_sources]]
id = "openclaw_email_triage_json"
label = "Email Triage"
path_key = "openclaw_email_triage_json"
lane = "life_admin"
trust_tier = "canonical"
owner = "system"
enabled = false
questions_it_can_answer = ["What inbox/admin obligations are visible in recent triage output?"]
questions_it_cannot_answer = ["Anything outside explicit opt-in for the life_admin lane."]

[freshness.attach_sources.status]
live_hours = 12
recent_hours = 72
stale_hours = 168

[freshness.attach_sources.priorities]
live_hours = 24
recent_hours = 96
stale_hours = 240

[freshness.attach_sources.job_search]
live_hours = 12
recent_hours = 72
stale_hours = 168

[freshness.attach_sources.openclaw_runbook]
recent_hours = 240
stale_hours = 1440

[freshness.attach_sources.memory_system]
recent_hours = 240
stale_hours = 1440

[freshness.attach_sources.chronicle_protocol]
recent_hours = 240
stale_hours = 1440

[freshness.attach_sources.chronicle_adr]
recent_hours = 720
stale_hours = 2160

[freshness.attach_sources.chronicle_schema]
recent_hours = 720
stale_hours = 2160

[freshness.attach_sources.chronicle_checklist]
recent_hours = 720
stale_hours = 2160

[freshness.semantic_recall.mem0_dump]
live_hours = 18
recent_hours = 30
stale_hours = 54

[freshness.runtime_evidence.portfolio_asset_manifest]
recent_hours = 168
stale_hours = 720

[[sources]]
id = "status"
label = "Status"
path = "{_path(root / 'status.md')}"
kind = "markdown"
role = "ssot"
trust_tier = "operator_curated"
priority = 100
headings = [
  "Active Projects",
  "Decisions Log",
  "Priorities",
  "Blockers / Waiting",
]

[[sources]]
id = "priorities"
label = "Priorities"
path = "{_path(root / 'priorities.md')}"
kind = "markdown"
role = "strategy"
trust_tier = "operator_curated"
priority = 90
headings = [
  "Strategic Context",
  "Priority Stack (ordered)",
  "Anti-Patterns",
  "Key Constraints",
]

[[sources]]
id = "job_search"
label = "Pipeline Status"
path = "{_path(root / 'job-search-status.md')}"
kind = "markdown"
role = "pipeline_metrics"
trust_tier = "operator_curated"
priority = 80

[[sources]]
id = "openclaw_runbook"
label = "Agent Runbook"
path = "{_path(root / 'OPENCLAW-RUNBOOK.md')}"
kind = "markdown"
role = "ops"
trust_tier = "reference"
priority = 70

[[sources]]
id = "memory_system"
label = "Memory System"
path = "{_path(root / 'MEMORY_SYSTEM.md')}"
kind = "markdown"
role = "memory_policy"
trust_tier = "operator_curated"
priority = 65
headings = [
  "1. Truth Model",
  "2. Main Layers",
  "5. How Optional Integrations Work",
  "7. How Activation Works",
]

[[sources]]
id = "chronicle_protocol"
label = "Chronicle Protocol"
path = "{_path(root / 'CHRONICLE_PROTOCOL.md')}"
kind = "markdown"
role = "timeline_protocol"
trust_tier = "reference"
priority = 60
headings = [
  "Command Surface",
  "Data Model",
  "Operational Constraint",
]

[[sources]]
id = "chronicle_adr"
label = "Chronicle ADR 0001"
path = "{_path(docs_root / 'ADR-0001-canonical-memory.md')}"
kind = "markdown"
role = "architecture"
trust_tier = "reference"
priority = 55

[[sources]]
id = "chronicle_schema"
label = "Chronicle Schema v1"
path = "{_path(docs_root / 'SCHEMA_V1.md')}"
kind = "markdown"
role = "schema"
trust_tier = "reference"
priority = 54

[[sources]]
id = "chronicle_checklist"
label = "Chronicle Implementation Checklist"
path = "{_path(docs_root / 'IMPLEMENTATION_CHECKLIST.md')}"
kind = "markdown"
role = "implementation_plan"
trust_tier = "reference"
priority = 53

[[domains]]
id = "global"
label = "Whole System"
source_ids = ["status", "priorities", "job_search", "memory_system", "chronicle_protocol"]
mem0_queries = [
  "current priorities blockers system state",
  "recent durable decisions",
  "active queue or project status",
]

[[domains]]
id = "portfolio"
label = "Shipping / Delivery"
source_ids = ["status", "priorities", "memory_system", "chronicle_protocol"]
mem0_queries = [
  "shipping blockers and missing assets",
  "portfolio proof of work",
  "what is ready to publish",
]

[[domains]]
id = "job_search"
label = "Pipeline / Outreach"
source_ids = ["status", "priorities", "job_search", "openclaw_runbook", "chronicle_protocol"]
mem0_queries = [
  "job search pipeline leads outreach",
  "active queue status",
  "who is ready next",
]

[[domains]]
id = "memory"
label = "Memory Architecture"
source_ids = ["memory_system", "chronicle_protocol", "chronicle_adr", "chronicle_schema", "chronicle_checklist", "status"]
mem0_queries = [
  "memory architecture and constraints",
  "L1 L2 L3 memory rules",
  "durable truth vs derived recall",
]
"""


def _automation_template(root: Path) -> str:
    runtime_root = root / "runtime"
    workspaces_root = root / "workspaces"
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

[minimax]
base_url = "https://api.minimax.io/v1"
model = "MiniMax-M2.7"
temperature = 0.2
max_tokens = 3200
timeout_seconds = 45
max_retries = 3
retry_base_delay = 2.0

[guards]
daybook_per_day = 1
weekly_audit_per_week = 1
backup_interval_days = 2
projection_stale_hours = 36
snapshot_stale_hours = 30
mem0_sync_batch_size = 25
minimax_canary_stale_hours = 36

[[repos]]
slug = "status"
path = "{_path(root)}"

[[repos]]
slug = "intel-digest"
path = "{_path(workspaces_root / 'intel-digest')}"

[[repos]]
slug = "portfolio"
path = "{_path(workspaces_root / 'portfolio')}"

[[repos]]
slug = "remotion"
path = "{_path(workspaces_root / 'remotion')}"

[[repos]]
slug = "fcp-sorter"
path = "{_path(workspaces_root / 'fcp-sorter')}"

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

[jobs.company_intel]
hour = 7
minute = 10
throttle_seconds = 900

[jobs.minimax_canary]
hour = 22
minute = 40
throttle_seconds = 600

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

## Operational Constraint

- Startup/activation should happen before mutating Chronicle from MCP.
- Optional integrations may be absent on a fresh install; Chronicle should still run safely.
"""


def _runbook_template() -> str:
    return """# Agent Runbook

- Add agent-specific procedures here.
- Keep operational rules short and explicit.
- Prefer linking to durable docs over duplicating policy everywhere.
"""


def _adr_template() -> str:
    return """# ADR-0001 Canonical Memory

Chronicle is the canonical truth layer.
Derived recall systems may assist retrieval, but they must not redefine truth.
"""


def _schema_template() -> str:
    return """# Schema v1

- events
- snapshots
- artifacts
- automation_runs
- hook_events
- normalized_entities
- scenario_runs
- lens_runs
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
        resolved_root / "OPENCLAW-RUNBOOK.md": _runbook_template(),
        resolved_root / "docs" / "ADR-0001-canonical-memory.md": _adr_template(),
        resolved_root / "docs" / "SCHEMA_V1.md": _schema_template(),
        resolved_root / "docs" / "IMPLEMENTATION_CHECKLIST.md": _checklist_template(),
        resolved_root / ".gitignore": _gitignore_template(),
        resolved_root / ".env.example": "# Optional secrets for Chronicle integrations.\n# MINIMAX_API_KEY=\n",
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
        resolved_root / "company-intel.json": {"companies": {}, "lead_companies": {"with_intel": [], "without_intel": []}},
        resolved_root / "runtime" / "openclaw" / "state.json": {},
        resolved_root / "runtime" / "openclaw" / "morning-brief.json": {},
        resolved_root / "runtime" / "openclaw" / "leads.json": [],
        resolved_root / "runtime" / "openclaw" / "email-triage-latest.json": {},
        resolved_root / "runtime" / "digest" / "last_status.json": {},
    }
    for path, payload in json_files.items():
        _write_json(path, payload, created=created, skipped=skipped, force=force)

    text_files = {
        resolved_root / "runtime" / "digest" / "synthesis.md": "# Digest Synthesis\n\nPlaceholder.\n",
        resolved_root / "runtime" / "digest" / "previous_summary.txt": "Placeholder.\n",
        resolved_root / "runtime" / "portfolio" / "asset-manifest.ts": "export const assetManifest = {};\n",
    }
    for path, content in text_files.items():
        _write_text(path, content, created=created, skipped=skipped, force=force)

    for directory in (
        resolved_root / "chronicle-artifacts",
        resolved_root / "backups",
        resolved_root / "daybooks",
        resolved_root / "git-hooks",
        resolved_root / "logs" / "launchd",
        resolved_root / "runtime" / "launch-agents",
        resolved_root / "runtime" / "launchd",
        resolved_root / "runtime" / "openclaw" / "memory",
        resolved_root / "workspaces",
        resolved_root / "workspaces" / "portfolio",
        resolved_root / "workspaces" / "remotion",
        resolved_root / "workspaces" / "fcp-sorter",
        resolved_root / "workspaces" / "intel-digest",
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
    with connect(config.db_path) as connection:
        apply_migrations(connection, config)

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
