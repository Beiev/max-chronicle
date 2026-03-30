from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import textwrap

import pytest

from max_chronicle.automation import load_automation_config
from max_chronicle.runtime_context import load_manifest


@dataclass
class ChronicleSandbox:
    root: Path
    status_root: Path
    manifest_path: Path
    automation_path: Path
    digest_repo: Path
    portfolio_repo: Path
    remotion_repo: Path
    fcp_repo: Path
    openclaw_root: Path
    backup_root: Path

    @property
    def chronicle_db(self) -> Path:
        return self.status_root / "chronicle.db"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _json(path: Path, payload: object) -> None:
    _write(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _init_repo(path: Path, *, files: dict[str, str]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(path), "init", "-b", "main"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(path), "config", "user.name", "Chronicle Tests"], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(path), "config", "user.email", "chronicle-tests@example.com"], check=True, capture_output=True, text=True)
    for relative, content in files.items():
        file_path = path / relative
        _write(file_path, content)
    subprocess.run(["git", "-C", str(path), "add", "."], check=True, capture_output=True, text=True)
    subprocess.run(["git", "-C", str(path), "commit", "-m", "initial"], check=True, capture_output=True, text=True)


def _status_sources(status_root: Path) -> None:
    _write(status_root / "HANDOFF_MEMO.md", "# Handoff\n\n## 2. Текущие приоритеты\n- Ship Chronicle.\n")
    _write(
        status_root / "status.md",
        "# Status\n\n## Active Projects\n- OK\n\n## Decisions Log\n- Chronicle native.\n\n## Priorities\n- Ship Chronicle.\n\n## Blockers / Waiting\n- None.\n\n<!-- BEGIN GENERATED:CHRONICLE_STATUS -->\nplaceholder\n<!-- END GENERATED:CHRONICLE_STATUS -->\n",
    )
    _write(
        status_root / "priorities.md",
        "# Priorities\n\n## Strategic Context\n- Chronicle test sandbox.\n\n## Priority Stack (ordered)\n- Chronicle first.\n\n## Anti-Patterns (ADHD Guard Rails)\n- No duplicate docs.\n\n## Key Constraints\n- Keep one contract.\n",
    )
    _write(status_root / "job-search-status.md", "# Job Search Status\n\nPlaceholder.\n")
    _write(status_root / "ECOSYSTEM.md", "# Ecosystem\n\n## Agents & Zones\n- Chronicle only.\n")
    _write(status_root / "OPENCLAW-RUNBOOK.md", "# OpenClaw Runbook\n\nPlaceholder.\n")
    _write(status_root / "MEMORY_SYSTEM.md", "# Memory System\n\n## 1. Truth Model\n- Chronicle is truth.\n\n## 2. Main Layers\n- SSOT is small.\n\n## 5. How Mem0 Works In This System\n- Mem0 is derived.\n\n## 7. How Activation Works\n- Use chronicle activate.\n")
    _write(status_root / "CODEX_MEM0_PROTOCOL.md", "# Archived\n\nDeprecated.\n")
    _write(status_root / "CHRONICLE_PROTOCOL.md", "# Chronicle Protocol\n\n## Command Surface\n- Native CLI.\n\n## Data Model\n- Chronicle is truth.\n\n## Operational Constraint\n- Mem0 can lag.\n")
    _write(status_root / "max_chronicle" / "docs" / "ADR-0001-canonical-memory.md", "# ADR\n")
    _write(status_root / "max_chronicle" / "docs" / "SCHEMA_V1.md", "# Schema\n")
    _write(status_root / "max_chronicle" / "docs" / "IMPLEMENTATION_CHECKLIST.md", "# Checklist\n")
    _write(status_root / "ssot-ledger.jsonl", "")
    _write(status_root / "chronicle-snapshots.jsonl", "")
    _json(status_root / "mem0-dump.json", {"memories": []})
    _json(status_root / "company-intel.json", {"companies": {}, "lead_companies": {"with_intel": [], "without_intel": []}})


def _write_manifest(sandbox: ChronicleSandbox) -> None:
    manifest = f"""
version = 1
title = "Chronicle Test SSOT"

[settings]
timezone = "Europe/Warsaw"

[paths]
status_root = "{sandbox.status_root}"
ledger_file = "{sandbox.status_root / 'ssot-ledger.jsonl'}"
snapshot_file = "{sandbox.status_root / 'chronicle-snapshots.jsonl'}"
chronicle_db = "{sandbox.status_root / 'chronicle.db'}"
chronicle_artifact_dir = "{sandbox.status_root / 'chronicle-artifacts'}"
chronicle_package_root = "/Users/maksymbeiev/Projects/status/max_chronicle"
mem0_dump = "{sandbox.status_root / 'mem0-dump.json'}"
mem0_bridge = "{sandbox.status_root / 'scripts' / 'mem0_bridge.py'}"
env_file = "{sandbox.digest_repo / '.env'}"
workspace_root = "{sandbox.root}"
openclaw_memory_dir = "{sandbox.openclaw_root / 'workspace' / 'memory'}"
openclaw_state_json = "{sandbox.openclaw_root / 'workspace' / 'memory' / 'state.json'}"
openclaw_brief_json = "{sandbox.openclaw_root / 'workspace' / 'memory' / 'morning-brief.json'}"
openclaw_leads_json = "{sandbox.openclaw_root / 'workspace' / 'memory' / 'leads.json'}"
openclaw_email_triage_json = "{sandbox.openclaw_root / 'workspace' / 'memory' / 'email-triage-latest.json'}"
digest_status_json = "{sandbox.digest_repo / 'data' / 'last_status.json'}"
digest_synthesis_md = "{sandbox.digest_repo / 'data' / 'analysis_ua' / 'synthesis.md'}"
digest_previous_summary = "{sandbox.digest_repo / 'data' / 'previous_summary.txt'}"
company_intel_json = "{sandbox.status_root / 'company-intel.json'}"
portfolio_asset_manifest = "{sandbox.portfolio_repo / 'src' / 'data' / 'asset-manifest.ts'}"
portfolio_repo = "{sandbox.portfolio_repo}"
remotion_repo = "{sandbox.remotion_repo}"
fcp_sorter_repo = "{sandbox.fcp_repo}"
intel_digest_repo = "{sandbox.digest_repo}"

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
questions_it_can_answer = ["What portfolio asset gaps exist?"]
questions_it_cannot_answer = ["Anything outside portfolio asset readiness."]

[[runtime_sources]]
id = "openclaw_state_json"
label = "OpenClaw State"
path_key = "openclaw_state_json"
lane = "agents"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["What is the autonomous runtime state?"]
questions_it_cannot_answer = ["Whether strategy is correct without corroboration."]

[[runtime_sources]]
id = "openclaw_brief_json"
label = "OpenClaw Morning Brief"
path_key = "openclaw_brief_json"
lane = "agents"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["What did OpenClaw last brief?"]
questions_it_cannot_answer = ["Durable truth when Chronicle disagrees."]

[[runtime_sources]]
id = "openclaw_leads_json"
label = "OpenClaw Leads"
path_key = "openclaw_leads_json"
lane = "career_market"
trust_tier = "canonical"
owner = "system"
questions_it_can_answer = ["Which leads are in the pipeline?"]
questions_it_cannot_answer = ["Whether a lead is strategically good on its own."]

[[runtime_sources]]
id = "company_intel_json"
label = "Company Intel"
path_key = "company_intel_json"
lane = "companies"
trust_tier = "reference"
owner = "chronicle"
questions_it_can_answer = ["Which company-level supporting context exists?"]
questions_it_cannot_answer = ["Canonical truth without corroboration."]

[[runtime_sources]]
id = "digest_status_json"
label = "Digest Status"
path_key = "digest_status_json"
lane = "world"
trust_tier = "canonical"
owner = "chronicle"
questions_it_can_answer = ["Did the world digest run successfully?"]
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
questions_it_can_answer = ["What inbox/admin obligations are visible?"]
questions_it_cannot_answer = ["Anything outside explicit life_admin opt-in."]

[freshness.runtime_evidence.portfolio_asset_manifest]
recent_hours = 168
stale_hours = 720

[freshness.attach_sources.status]
live_hours = 12
recent_hours = 72
stale_hours = 168

[freshness.attach_sources.memory_system]
recent_hours = 240
stale_hours = 1440

[freshness.attach_sources.chronicle_protocol]
recent_hours = 240
stale_hours = 1440

[freshness.semantic_recall.mem0_dump]
live_hours = 18
recent_hours = 30
stale_hours = 54

[[sources]]
id = "status"
label = "Status"
path = "{sandbox.status_root / 'status.md'}"
kind = "markdown"
role = "ssot"
trust_tier = "operator_curated"
priority = 100
headings = ["Active Projects", "Decisions Log", "Priorities", "Blockers / Waiting"]

[[sources]]
id = "priorities"
label = "Priorities"
path = "{sandbox.status_root / 'priorities.md'}"
kind = "markdown"
role = "strategy"
trust_tier = "operator_curated"
priority = 90
headings = ["Strategic Context", "Priority Stack (ordered)", "Anti-Patterns (ADHD Guard Rails)", "Key Constraints"]

[[sources]]
id = "job_search"
label = "Job Search"
path = "{sandbox.status_root / 'job-search-status.md'}"
kind = "markdown"
role = "pipeline_metrics"
trust_tier = "operator_curated"
priority = 80

[[sources]]
id = "openclaw_runbook"
label = "OpenClaw Runbook"
path = "{sandbox.status_root / 'OPENCLAW-RUNBOOK.md'}"
kind = "markdown"
role = "ops"
trust_tier = "reference"
priority = 70

[[sources]]
id = "memory_system"
label = "Memory System"
path = "{sandbox.status_root / 'MEMORY_SYSTEM.md'}"
kind = "markdown"
role = "memory_policy"
trust_tier = "operator_curated"
priority = 65
headings = ["1. Truth Model", "2. Main Layers", "5. How Mem0 Works In This System", "7. How Activation Works"]

[[sources]]
id = "chronicle_protocol"
label = "Chronicle Protocol"
path = "{sandbox.status_root / 'CHRONICLE_PROTOCOL.md'}"
kind = "markdown"
role = "timeline_protocol"
trust_tier = "reference"
priority = 60
headings = ["Command Surface", "Data Model", "Operational Constraint"]

[[sources]]
id = "chronicle_adr"
label = "Chronicle ADR"
path = "{sandbox.status_root / 'max_chronicle' / 'docs' / 'ADR-0001-canonical-memory.md'}"
kind = "markdown"
role = "architecture"
trust_tier = "reference"
priority = 55

[[sources]]
id = "chronicle_schema"
label = "Chronicle Schema"
path = "{sandbox.status_root / 'max_chronicle' / 'docs' / 'SCHEMA_V1.md'}"
kind = "markdown"
role = "schema"
trust_tier = "reference"
priority = 54

[[sources]]
id = "chronicle_checklist"
label = "Chronicle Checklist"
path = "{sandbox.status_root / 'max_chronicle' / 'docs' / 'IMPLEMENTATION_CHECKLIST.md'}"
kind = "markdown"
role = "implementation_plan"
trust_tier = "reference"
priority = 53

[[domains]]
id = "global"
label = "Whole System"
source_ids = ["status", "priorities", "job_search", "memory_system", "chronicle_protocol"]
mem0_queries = ["chronicle tests"]

[[domains]]
id = "memory"
label = "Memory"
source_ids = ["memory_system", "chronicle_protocol", "chronicle_adr", "chronicle_schema", "chronicle_checklist", "status"]
mem0_queries = ["chronicle memory tests"]
"""
    _write(sandbox.manifest_path, textwrap.dedent(manifest).strip() + "\n")


def _write_automation(sandbox: ChronicleSandbox) -> None:
    content = f"""
version = 1
title = "Chronicle Native Automation Tests"

[paths]
backup_root = "{sandbox.backup_root}"
env_file = "{sandbox.digest_repo / '.env'}"
launch_agent_dir = "{sandbox.root / 'LaunchAgents'}"
launchd_runtime_dir = "{sandbox.status_root / 'runtime' / 'launchd'}"
launchd_log_dir = "{sandbox.status_root / 'logs' / 'launchd'}"
daybook_dir = "{sandbox.status_root / 'daybooks'}"
git_hooks_dir = "{sandbox.status_root / 'git-hooks'}"

[minimax]
base_url = "https://api.minimax.io/v1"
model = "MiniMax-M2.7"
temperature = 0.2
max_tokens = 2000
timeout_seconds = 5
max_retries = 1
retry_base_delay = 0.1

[guards]
daybook_per_day = 1
weekly_audit_per_week = 1
backup_interval_days = 2
projection_stale_hours = 36
snapshot_stale_hours = 30
mem0_sync_batch_size = 10
minimax_canary_stale_hours = 36

[[repos]]
slug = "status"
path = "{sandbox.status_root}"

[[repos]]
slug = "intel-digest"
path = "{sandbox.digest_repo}"

[[repos]]
slug = "rzmrn-portfolio"
path = "{sandbox.portfolio_repo}"

[[repos]]
slug = "fcp-sorter"
path = "{sandbox.fcp_repo}"

[[repos]]
slug = "remotion"
path = "{sandbox.remotion_repo}"

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
    _write(sandbox.automation_path, textwrap.dedent(content).strip() + "\n")


@pytest.fixture()
def chronicle_sandbox(tmp_path: Path) -> ChronicleSandbox:
    sandbox = ChronicleSandbox(
        root=tmp_path,
        status_root=tmp_path / "status",
        manifest_path=tmp_path / "status" / "SSOT_MANIFEST.toml",
        automation_path=tmp_path / "status" / "CHRONICLE_AUTOMATION.toml",
        digest_repo=tmp_path / "intel-digest",
        portfolio_repo=tmp_path / "rzmrn-portfolio",
        remotion_repo=tmp_path / "remotion",
        fcp_repo=tmp_path / "fcp-sorter",
        openclaw_root=tmp_path / ".openclaw",
        backup_root=tmp_path / "Volumes" / "T9" / "RZMRN-Chronicle-Backups",
    )

    _status_sources(sandbox.status_root)

    _init_repo(sandbox.status_root, files={"README.md": "# Status\n"})
    _init_repo(
        sandbox.digest_repo,
        files={
            "README.md": "# Digest\n",
            "data/analysis_ua/synthesis.md": "**1. Test headline**\n",
            "data/previous_summary.txt": "Yesterday summary.\n",
            ".env": "MINIMAX_API_KEY=test-key\nGOOGLE_API_KEY=test-google\n",
        },
    )
    _init_repo(
        sandbox.portfolio_repo,
        files={
            "README.md": "# Portfolio\n",
            "src/data/asset-manifest.ts": 'export const assetManifest = {\n  "case-study": {\n    hero: null, // STATUS: missing\n  },\n};\n',
        },
    )
    _init_repo(sandbox.remotion_repo, files={"README.md": "# Remotion\n"})
    _init_repo(sandbox.fcp_repo, files={"README.md": "# FCP\n"})

    _json(
        sandbox.digest_repo / "data" / "last_status.json",
        {
            "status": "ok",
            "timestamp": "2026-03-15T13:12:46.393298+00:00",
            "sources_ok": 12,
            "sources_total": 12,
            "items": 42,
            "elapsed_seconds": 321.1,
            "deployed": True,
        },
    )
    _write(sandbox.digest_repo / "logs" / "digest_2026-03-15_1312.log", "Digest run log\n")

    _json(
        sandbox.openclaw_root / "workspace" / "memory" / "state.json",
        {
            "jobs_found_today": 3,
            "applications_sent_today": 1,
            "gpt_calls_today": 2,
            "pipeline_counts": {"found": 3, "shortlisted": 2, "cl_written": 1, "applied": 1},
        },
    )
    _json(
        sandbox.openclaw_root / "workspace" / "memory" / "morning-brief.json",
        {"last_scout": "2026-03-15", "last_nightly": "2026-03-15", "last_backup": "2026-03-15"},
    )
    _json(
        sandbox.openclaw_root / "workspace" / "memory" / "leads.json",
        [{"company": "Example", "role": "Producer", "status": "shortlisted"}],
    )
    _json(
        sandbox.openclaw_root / "workspace" / "memory" / "email-triage-latest.json",
        {"items": [{"subject": "Utility bill", "status": "needs-attention"}]},
    )
    _write(sandbox.openclaw_root / "workspace" / "memory" / "2026-03-15.md", "Daily memory log.\n")

    _write(sandbox.status_root / "scripts" / "ssot_hub.py", "#!/usr/bin/env python3\nprint('sync ok')\n")
    _write(sandbox.status_root / "scripts" / "chronicle", "#!/bin/zsh\nexec python3 -m max_chronicle.cli \"$@\"\n")
    (sandbox.status_root / "scripts" / "chronicle").chmod(0o755)
    _write(
        sandbox.status_root / "scripts" / "mem0_bridge.py",
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "args = sys.argv[1:]\n"
        "if args and args[0] == 'add':\n"
        "    print(json.dumps({'id': 'fake-memory', 'status': 'stored', 'text': ' '.join(args[1:])}, ensure_ascii=False))\n"
        "    raise SystemExit(0)\n"
        "if args and args[0] == 'dump':\n"
        "    output = args[args.index('--output') + 1] if '--output' in args else 'mem0-dump.json'\n"
        "    payload = {'total_memories': 1, 'memories': [{'id': 'fake-memory', 'memory': 'Chronicle sandbox memory', 'metadata': {'project': 'status'}}]}\n"
        "    with open(output, 'w', encoding='utf-8') as handle:\n"
        "        json.dump(payload, handle, ensure_ascii=False, indent=2)\n"
        "        handle.write('\\n')\n"
        "    print(json.dumps({'status': 'ok', 'output': output}, ensure_ascii=False))\n"
        "    raise SystemExit(0)\n"
        "raise SystemExit(1)\n",
    )
    _write(
        sandbox.status_root / "git-hooks" / "post-commit",
        "#!/bin/zsh\nexit 0\n",
    )
    (sandbox.status_root / "git-hooks" / "post-commit").chmod(0o755)

    _write_manifest(sandbox)
    _write_automation(sandbox)
    return sandbox


@pytest.fixture()
def loaded_manifest(chronicle_sandbox: ChronicleSandbox) -> dict:
    return load_manifest(chronicle_sandbox.manifest_path)


@pytest.fixture()
def loaded_automation(chronicle_sandbox: ChronicleSandbox):
    return load_automation_config(chronicle_sandbox.automation_path)
