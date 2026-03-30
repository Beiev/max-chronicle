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
class MiniMaxSettings:
    base_url: str
    model: str
    temperature: float
    max_tokens: int
    timeout_seconds: float
    max_retries: int
    retry_base_delay: float


@dataclass(frozen=True)
class GuardSettings:
    daybook_per_day: int
    weekly_audit_per_week: int
    backup_interval_days: int
    projection_stale_hours: int
    snapshot_stale_hours: int
    mem0_sync_batch_size: int
    minimax_canary_stale_hours: int


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
    minimax: MiniMaxSettings
    guards: GuardSettings
    repos: tuple[RepoConfig, ...]
    jobs: dict[str, JobSchedule]


def _load_toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def load_automation_config(path: Path | None = None) -> AutomationConfig:
    config_path = path or default_automation_path()
    raw = _load_toml(config_path)
    paths = raw.get("paths", {})
    minimax = raw.get("minimax", {})
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
        minimax=MiniMaxSettings(
            base_url=minimax["base_url"],
            model=minimax["model"],
            temperature=float(minimax.get("temperature", 0.2)),
            max_tokens=int(minimax.get("max_tokens", 3200)),
            timeout_seconds=float(minimax.get("timeout_seconds", 45.0)),
            max_retries=int(minimax.get("max_retries", 3)),
            retry_base_delay=float(minimax.get("retry_base_delay", 2.0)),
        ),
        guards=GuardSettings(
            daybook_per_day=int(guards.get("daybook_per_day", 1)),
            weekly_audit_per_week=int(guards.get("weekly_audit_per_week", 1)),
            backup_interval_days=int(guards.get("backup_interval_days", 2)),
            projection_stale_hours=int(guards.get("projection_stale_hours", 36)),
            snapshot_stale_hours=int(guards.get("snapshot_stale_hours", 30)),
            mem0_sync_batch_size=int(guards.get("mem0_sync_batch_size", 25)),
            minimax_canary_stale_hours=int(guards.get("minimax_canary_stale_hours", 36)),
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
