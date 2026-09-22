from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import textwrap

import pytest

# Hermetic by construction: variables exported by an operator's shell must
# never aim a test (or a CLI subprocess inheriting this environment) at a real
# workspace. Drop them before max_chronicle is imported, because the config
# module resolves its defaults at import time. Tests that need them set them
# explicitly via monkeypatch or a subprocess env.
for _name in (
    "CHRONICLE_ROOT",
    "CHRONICLE_MANIFEST",
    "CHRONICLE_AUTOMATION_CONFIG",
    "CHRONICLE_DB",
    "CHRONICLE_REQUIRE_ROOT",
    "CHRONICLE_AUTO_MIGRATE",
):
    os.environ.pop(_name, None)

from max_chronicle.automation import load_automation_config
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import reset_migration_cache

# Anchor on the checkout, not on one machine's absolute path.
CHRONICLE_PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "max_chronicle"


@pytest.fixture(autouse=True)
def _offline_embeddings(monkeypatch):
    """Tests opt into explicit fake vectors; never contact an operator's Ollama."""
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda _: None)


@pytest.fixture(autouse=True)
def _isolated_home_root(tmp_path_factory, monkeypatch):
    """Keep the implicit ~/.max-chronicle fallback inside the test run."""
    monkeypatch.setattr(
        "max_chronicle.config.DEFAULT_HOME_ROOT",
        tmp_path_factory.getbasetemp() / "max-chronicle-home",
    )


@pytest.fixture(autouse=True)
def _reset_migration_cache():
    reset_migration_cache()
    yield
    reset_migration_cache()


@dataclass
class ChronicleSandbox:
    root: Path
    status_root: Path
    manifest_path: Path
    automation_path: Path
    digest_repo: Path
    portfolio_repo: Path
    render_repo: Path
    media_sorter_repo_path: Path
    agenthub_root: Path
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
    _write(status_root / "AGENT-RUNBOOK.md", "# AgentHub Runbook\n\nPlaceholder.\n")
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
chronicle_package_root = "{CHRONICLE_PACKAGE_ROOT}"
mem0_dump = "{sandbox.status_root / 'mem0-dump.json'}"
mem0_bridge = "{sandbox.status_root / 'scripts' / 'mem0_bridge.py'}"
workspace_root = "{sandbox.root}"
company_intel_json = "{sandbox.status_root / 'company-intel.json'}"
portfolio_asset_manifest = "{sandbox.portfolio_repo / 'src' / 'data' / 'asset-manifest.ts'}"
portfolio_repo = "{sandbox.portfolio_repo}"
render_repo = "{sandbox.render_repo}"
media_sorter_repo = "{sandbox.media_sorter_repo_path}"

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
id = "company_intel_json"
label = "Company Intel"
path_key = "company_intel_json"
lane = "companies"
trust_tier = "reference"
owner = "chronicle"
questions_it_can_answer = ["Which company-level supporting context exists?"]
questions_it_cannot_answer = ["Canonical truth without corroboration."]

[freshness.runtime_evidence.portfolio_asset_manifest]
recent_hours = 168
stale_hours = 999999

[freshness.runtime_evidence.company_intel_json]
recent_hours = 999999
stale_hours = 999999

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
env_file = "{sandbox.status_root / '.env'}"
launch_agent_dir = "{sandbox.root / 'LaunchAgents'}"
launchd_runtime_dir = "{sandbox.status_root / 'runtime' / 'launchd'}"
launchd_log_dir = "{sandbox.status_root / 'logs' / 'launchd'}"
daybook_dir = "{sandbox.status_root / 'daybooks'}"
git_hooks_dir = "{sandbox.status_root / 'git-hooks'}"

[guards]
daybook_per_day = 1
weekly_audit_per_week = 1
backup_interval_days = 2
projection_stale_hours = 36
snapshot_stale_hours = 30
mem0_sync_batch_size = 10
jsonl_rotate_mb = 25
artifact_store_warn_gb = 6

[[repos]]
slug = "status"
path = "{sandbox.status_root}"

[[repos]]
slug = "news-digest"
path = "{sandbox.digest_repo}"

[[repos]]
slug = "demo-portfolio"
path = "{sandbox.portfolio_repo}"

[[repos]]
slug = "media-sorter"
path = "{sandbox.media_sorter_repo_path}"

[[repos]]
slug = "renderkit"
path = "{sandbox.render_repo}"

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
    _write(sandbox.automation_path, textwrap.dedent(content).strip() + "\n")


@pytest.fixture()
def chronicle_sandbox(tmp_path: Path) -> ChronicleSandbox:
    sandbox = ChronicleSandbox(
        root=tmp_path,
        status_root=tmp_path / "status",
        manifest_path=tmp_path / "status" / "SSOT_MANIFEST.toml",
        automation_path=tmp_path / "status" / "CHRONICLE_AUTOMATION.toml",
        digest_repo=tmp_path / "news-digest",
        portfolio_repo=tmp_path / "demo-portfolio",
        render_repo=tmp_path / "renderkit",
        media_sorter_repo_path=tmp_path / "media-sorter",
        agenthub_root=tmp_path / ".agenthub",
        backup_root=tmp_path / "Volumes" / "BackupDrive" / "Chronicle-Backups",
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
    _init_repo(sandbox.render_repo, files={"README.md": "# RenderKit\n"})
    _init_repo(sandbox.media_sorter_repo_path, files={"README.md": "# Media Sorter\n"})


    _write(sandbox.status_root / ".env", "# Chronicle test env\n")
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
