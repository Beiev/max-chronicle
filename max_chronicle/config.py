from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import tomllib


PACKAGE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_ROOT.parent
MIGRATIONS_DIR = PACKAGE_ROOT / "migrations"
DEFAULT_HOME_ROOT = Path.home() / ".max-chronicle"
DEFAULT_TIMEZONE = "Europe/Warsaw"
ACTIVATION_CONTRACT_NAME = "max-chronicle"
ACTIVATION_CONTRACT_VERSION = "2026-03-16.v1"
ENV_CHRONICLE_ROOT = "CHRONICLE_ROOT"
ENV_CHRONICLE_MANIFEST = "CHRONICLE_MANIFEST"
ENV_CHRONICLE_AUTOMATION = "CHRONICLE_AUTOMATION_CONFIG"
ENV_CHRONICLE_DB = "CHRONICLE_DB"
ENV_CHRONICLE_TIMEZONE = "CHRONICLE_TIMEZONE"
DEFAULT_ARTIFACT_MAX_COPY_BYTES = 25 * 1024 * 1024
DEFAULT_ARTIFACT_ALLOWED_EXTENSIONS = (
    "csv",
    "ini",
    "js",
    "json",
    "jsonl",
    "jsx",
    "log",
    "md",
    "py",
    "sh",
    "sql",
    "toml",
    "ts",
    "tsx",
    "tsv",
    "txt",
    "typ",
    "yaml",
    "yml",
)


@dataclass(frozen=True)
class ChronicleConfig:
    status_root: Path
    db_path: Path
    artifact_dir: Path
    ledger_path: Path
    snapshot_path: Path
    manifest_path: Path
    automation_path: Path
    migrations_dir: Path
    timezone: str
    artifact_max_copy_bytes: int
    artifact_allowed_extensions: tuple[str, ...]
    artifact_pointer_only_enabled: bool
    artifact_follow_symlinks: bool


def _expand(path: str | Path) -> Path:
    return Path(path).expanduser()


def resolve_status_root(
    *,
    manifest_path: Path | None = None,
    automation_path: Path | None = None,
) -> Path:
    if os.environ.get(ENV_CHRONICLE_ROOT):
        return _expand(os.environ[ENV_CHRONICLE_ROOT])

    if manifest_path is not None:
        return _expand(manifest_path).parent

    if automation_path is not None:
        return _expand(automation_path).parent

    if os.environ.get(ENV_CHRONICLE_MANIFEST):
        return _expand(os.environ[ENV_CHRONICLE_MANIFEST]).parent

    if os.environ.get(ENV_CHRONICLE_AUTOMATION):
        return _expand(os.environ[ENV_CHRONICLE_AUTOMATION]).parent

    if (REPO_ROOT / "SSOT_MANIFEST.toml").exists():
        return REPO_ROOT

    return DEFAULT_HOME_ROOT


def default_manifest_path(status_root: Path | None = None) -> Path:
    if os.environ.get(ENV_CHRONICLE_MANIFEST):
        return _expand(os.environ[ENV_CHRONICLE_MANIFEST])
    return (status_root or resolve_status_root()) / "SSOT_MANIFEST.toml"


def default_automation_path(status_root: Path | None = None) -> Path:
    if os.environ.get(ENV_CHRONICLE_AUTOMATION):
        return _expand(os.environ[ENV_CHRONICLE_AUTOMATION])
    return (status_root or resolve_status_root()) / "CHRONICLE_AUTOMATION.toml"


def default_db_path(status_root: Path | None = None) -> Path:
    if os.environ.get(ENV_CHRONICLE_DB):
        return _expand(os.environ[ENV_CHRONICLE_DB])
    return (status_root or resolve_status_root()) / "chronicle.db"


DEFAULT_STATUS_ROOT = resolve_status_root()
DEFAULT_DB_PATH = default_db_path(DEFAULT_STATUS_ROOT)
DEFAULT_ARTIFACT_DIR = DEFAULT_STATUS_ROOT / "chronicle-artifacts"
DEFAULT_LEDGER_PATH = DEFAULT_STATUS_ROOT / "ssot-ledger.jsonl"
DEFAULT_SNAPSHOT_PATH = DEFAULT_STATUS_ROOT / "chronicle-snapshots.jsonl"
DEFAULT_MANIFEST_PATH = default_manifest_path(DEFAULT_STATUS_ROOT)
DEFAULT_AUTOMATION_PATH = default_automation_path(DEFAULT_STATUS_ROOT)


def _load_timezone(manifest_path: Path) -> str:
    if not manifest_path.exists():
        return os.environ.get(ENV_CHRONICLE_TIMEZONE, DEFAULT_TIMEZONE)

    data = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    settings = data.get("settings", {})
    timezone = settings.get("timezone")
    return timezone or os.environ.get(ENV_CHRONICLE_TIMEZONE, DEFAULT_TIMEZONE)


def default_config(
    db_path: Path | None = None,
    *,
    manifest_path: Path | None = None,
    automation_path: Path | None = None,
    status_root: Path | None = None,
) -> ChronicleConfig:
    resolved_status_root = status_root or resolve_status_root(
        manifest_path=manifest_path,
        automation_path=automation_path,
    )
    resolved_manifest_path = _expand(manifest_path) if manifest_path is not None else default_manifest_path(resolved_status_root)
    resolved_automation_path = (
        _expand(automation_path) if automation_path is not None else default_automation_path(resolved_status_root)
    )
    resolved_db_path = _expand(db_path) if db_path is not None else default_db_path(resolved_status_root)
    return ChronicleConfig(
        status_root=resolved_status_root,
        db_path=resolved_db_path,
        artifact_dir=resolved_status_root / "chronicle-artifacts",
        ledger_path=resolved_status_root / "ssot-ledger.jsonl",
        snapshot_path=resolved_status_root / "chronicle-snapshots.jsonl",
        manifest_path=resolved_manifest_path,
        automation_path=resolved_automation_path,
        migrations_dir=MIGRATIONS_DIR,
        timezone=_load_timezone(resolved_manifest_path),
        artifact_max_copy_bytes=DEFAULT_ARTIFACT_MAX_COPY_BYTES,
        artifact_allowed_extensions=DEFAULT_ARTIFACT_ALLOWED_EXTENSIONS,
        artifact_pointer_only_enabled=True,
        artifact_follow_symlinks=False,
    )


def ensure_runtime_dirs(config: ChronicleConfig) -> None:
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    config.artifact_dir.mkdir(parents=True, exist_ok=True)
