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
# Launchers set this so a lost environment fails loudly instead of silently
# resolving to the package checkout or ~/.max-chronicle.
ENV_CHRONICLE_REQUIRE_ROOT = "CHRONICLE_REQUIRE_ROOT"
# Opt back into upgrading an existing database's schema on first connect.
# Off by default: upgrades run through `chronicle migrate` or a server started
# with `--migrate`. A server start ignores this variable; only the flag counts.
ENV_CHRONICLE_AUTO_MIGRATE = "CHRONICLE_AUTO_MIGRATE"

# Exit codes shared by `chronicle` and `chronicle-mcp`.
EXIT_CONFIG_ERROR = 2  # which workspace to use is unclear (see CHRONICLE_REQUIRE_ROOT)
EXIT_SCHEMA_ACTION = 3  # the schema needs `chronicle migrate`, or a newer release

# v8 feature flags — toggle Phase 1 surfaces without editing code.
ENV_FEATURE_ENTITY_ALIASES = "CHRONICLE_ENABLE_ENTITY_ALIASES"
ENV_FEATURE_EVENT_HASH_DEDUP = "CHRONICLE_ENABLE_EVENT_HASH_DEDUP"
ENV_FEATURE_MEM0_LIVE_SEARCH = "CHRONICLE_ENABLE_MEM0_LIVE_SEARCH"
ENV_MEM0_LIVE_TIMEOUT_S = "CHRONICLE_MEM0_LIVE_TIMEOUT_S"
ENV_EVENT_DEDUP_WINDOW_HOURS = "CHRONICLE_EVENT_DEDUP_WINDOW_HOURS"

# 15s: a cold `uv run` of the mem0 bridge can resolve/sync an environment and
# blow far past 5s. Since the MCP tool bodies moved off the event loop, a slow
# bridge call no longer blocks other sessions, so a generous budget is safe.
DEFAULT_MEM0_LIVE_TIMEOUT_S = 15.0
# Mem0 collection the outbox targets, and the operator label used in agent
# prompts. Both are per-installation identity, so they live in the manifest
# ([settings] mem0_collection / operator) instead of being baked into code.
DEFAULT_MEM0_COLLECTION = "chronicle_personal"
DEFAULT_OPERATOR_LABEL = "the operator"
DEFAULT_EVENT_DEDUP_WINDOW_HOURS = 24
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


class ChronicleConfigError(RuntimeError):
    """The environment does not say which Chronicle workspace to use."""


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
    mem0_collection: str
    operator_label: str


def _expand(path: str | Path) -> Path:
    return Path(path).expanduser()


def feature_enabled(env_name: str, *, default: bool = True) -> bool:
    """Read a feature flag from the environment.

    Anything in {"1","true","yes","on"} (case-insensitive) means on.
    Anything in {"0","false","no","off"} means off. Unset or garbage
    falls back to `default`.
    """
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def env_float(env_name: str, *, default: float) -> float:
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_int(env_name: str, *, default: int) -> int:
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _explicit_status_root(
    *,
    manifest_path: Path | None = None,
    automation_path: Path | None = None,
) -> Path | None:
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

    return None


def _implicit_status_root() -> Path:
    if (REPO_ROOT / "SSOT_MANIFEST.toml").exists():
        return REPO_ROOT
    return DEFAULT_HOME_ROOT


def resolve_status_root(
    *,
    manifest_path: Path | None = None,
    automation_path: Path | None = None,
) -> Path:
    """Resolve the workspace root: environment and explicit paths first.

    Without either, fall back to a checkout carrying SSOT_MANIFEST.toml, then
    to ~/.max-chronicle. With CHRONICLE_REQUIRE_ROOT set, that fallback raises
    instead: a launcher that lost its environment must fail loudly rather than
    open, or create, some other database.
    """
    explicit = _explicit_status_root(manifest_path=manifest_path, automation_path=automation_path)
    if explicit is not None:
        return explicit
    if feature_enabled(ENV_CHRONICLE_REQUIRE_ROOT, default=False):
        raise ChronicleConfigError(
            f"{ENV_CHRONICLE_REQUIRE_ROOT} is set but no workspace was given; set "
            f"{ENV_CHRONICLE_ROOT} or {ENV_CHRONICLE_MANIFEST} instead of falling back to "
            f"{_implicit_status_root()}"
        )
    return _implicit_status_root()


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


# Import-time defaults never raise: CHRONICLE_REQUIRE_ROOT is enforced when a
# config is actually resolved, so `--help` and plain imports keep working.
DEFAULT_STATUS_ROOT = _explicit_status_root() or _implicit_status_root()
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
        mem0_collection=DEFAULT_MEM0_COLLECTION,
        operator_label=DEFAULT_OPERATOR_LABEL,
    )


def ensure_runtime_dirs(config: ChronicleConfig) -> None:
    config.db_path.parent.mkdir(parents=True, exist_ok=True)
    config.artifact_dir.mkdir(parents=True, exist_ok=True)
