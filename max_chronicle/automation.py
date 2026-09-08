from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tomllib
from typing import Any

from .config import default_automation_path


@dataclass(frozen=True)
class RepoConfig:
    slug: str
    path: Path


@dataclass(frozen=True)
class JobSchedule:
    hour: int
    minute: int
    throttle_seconds: int
    weekday: int | None = None


@dataclass(frozen=True)
class GuardSettings:
    daybook_per_day: int
    weekly_audit_per_week: int
    backup_interval_days: int
    projection_stale_hours: int
    snapshot_stale_hours: int
    mem0_sync_batch_size: int
    stale_run_ttl_hours: int
    subprocess_timeout_seconds: float
    backup_space_margin_mb: float
    jsonl_rotate_mb: float
    artifact_store_warn_gb: float


@dataclass(frozen=True)
class AutomationConfig:
    root_path: Path
    backup_root: Path
    env_file: Path
    launch_agent_dir: Path
    launchd_runtime_dir: Path
    launchd_log_dir: Path
    daybook_dir: Path
    git_hooks_dir: Path
    # Installations own their launchd namespace; the package default is generic.
    launchd_label_prefix: str
    guards: GuardSettings
    repos: tuple[RepoConfig, ...]
    jobs: dict[str, JobSchedule]


def _load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def load_automation_config(path: Path | None = None) -> AutomationConfig:
    config_path = path or default_automation_path()
    raw = _load_toml(config_path)
    paths = raw.get("paths", {})
    guards = raw.get("guards", {})
    jobs = raw.get("jobs", {})

    repo_items = tuple(
        RepoConfig(slug=item["slug"], path=Path(item["path"]).expanduser())
        for item in raw.get("repos", [])
    )

    return AutomationConfig(
        root_path=config_path,
        backup_root=Path(paths["backup_root"]).expanduser(),
        env_file=Path(paths["env_file"]).expanduser(),
        launch_agent_dir=Path(paths["launch_agent_dir"]).expanduser(),
        launchd_runtime_dir=Path(paths["launchd_runtime_dir"]).expanduser(),
        launchd_log_dir=Path(paths["launchd_log_dir"]).expanduser(),
        daybook_dir=Path(paths["daybook_dir"]).expanduser(),
        git_hooks_dir=Path(paths["git_hooks_dir"]).expanduser(),
        launchd_label_prefix=str(paths.get("launchd_label_prefix", "com.chronicle")),
        guards=GuardSettings(
            daybook_per_day=int(guards.get("daybook_per_day", 1)),
            weekly_audit_per_week=int(guards.get("weekly_audit_per_week", 1)),
            backup_interval_days=int(guards.get("backup_interval_days", 2)),
            projection_stale_hours=int(guards.get("projection_stale_hours", 36)),
            snapshot_stale_hours=int(guards.get("snapshot_stale_hours", 30)),
            mem0_sync_batch_size=int(guards.get("mem0_sync_batch_size", 25)),
            stale_run_ttl_hours=int(guards.get("stale_run_ttl_hours", 6)),
            subprocess_timeout_seconds=float(guards.get("subprocess_timeout_seconds", 120.0)),
            backup_space_margin_mb=float(guards.get("backup_space_margin_mb", 512.0)),
            jsonl_rotate_mb=float(guards.get("jsonl_rotate_mb", 25.0)),
            artifact_store_warn_gb=float(guards.get("artifact_store_warn_gb", 6.0)),
        ),
        repos=repo_items,
        jobs={
            name: JobSchedule(
                hour=int(job["hour"]),
                minute=int(job["minute"]),
                throttle_seconds=int(job.get("throttle_seconds", 600)),
                weekday=job.get("weekday"),
            )
            for name, job in jobs.items()
        },
    )


def ensure_automation_dirs(config: AutomationConfig) -> None:
    config.launchd_runtime_dir.mkdir(parents=True, exist_ok=True)
    config.launchd_log_dir.mkdir(parents=True, exist_ok=True)
    config.daybook_dir.mkdir(parents=True, exist_ok=True)
    config.git_hooks_dir.mkdir(parents=True, exist_ok=True)


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env

    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        env[key] = value
        os.environ.setdefault(key, value)
    return env


def repo_by_slug(config: AutomationConfig, slug: str) -> RepoConfig:
    for repo in config.repos:
        if repo.slug == slug:
            return repo
    raise KeyError(f"Unknown tracked repo: {slug}")


def repo_slug_for_path(config: AutomationConfig, repo_path: Path) -> str:
    repo_path = repo_path.expanduser().resolve()
    for repo in config.repos:
        if repo.path.resolve() == repo_path:
            return repo.slug
    return repo_path.name
