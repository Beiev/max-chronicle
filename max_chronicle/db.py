from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
import os
import random
import re
import sqlite3
import time
import uuid

from .config import ChronicleConfig


MIGRATION_RE = re.compile(r"^(?P<version>\d{4})_(?P<name>.+)\.sql$")
APPLICATION_ID = 0x4348524E  # CHRN
MIGRATION_LOCK_RETRIES = 5
MIGRATION_LOCK_RETRY_BASE_SECONDS = 0.05
CONNECT_WAL_RETRIES = 8
# A pre-migration backup runs while the upgrader holds the write lock; bound
# it so a stuck source cannot hold every other writer forever.
MIGRATION_BACKUP_TIMEOUT_SECONDS = 120.0
MIGRATION_BACKUP_PAGES_PER_STEP = 256


class MigrationError(RuntimeError):
    """The migration files or the database schema are inconsistent."""


class SchemaMigrationRequired(MigrationError):
    """An existing database is behind this code and may not upgrade here."""


class SchemaAheadOfCode(MigrationError):
    """The database carries migrations that this code does not know."""


@dataclass(frozen=True)
class MigrationFile:
    version: int
    name: str
    path: Path


@dataclass(frozen=True)
class SchemaState:
    """Applied migrations of a database against the migration files on disk."""

    applied: frozenset[int]
    known: tuple[MigrationFile, ...]
    has_objects: bool = False
    application_id: int = 0

    @property
    def fresh(self) -> bool:
        """Empty: nothing in the file yet, so initialising it changes no one's data."""
        return not self.applied and not self.has_objects

    @property
    def foreign(self) -> bool:
        """Some other application's database: objects without Chronicle history."""
        if self.application_id not in (0, APPLICATION_ID):
            return True
        return not self.applied and self.has_objects

    @property
    def pending(self) -> tuple[MigrationFile, ...]:
        return tuple(item for item in self.known if item.version not in self.applied)

    @property
    def unknown(self) -> tuple[int, ...]:
        known_versions = {item.version for item in self.known}
        return tuple(sorted(version for version in self.applied if version not in known_versions))

    @property
    def current_version(self) -> int:
        return max(self.applied, default=0)

    @property
    def target_version(self) -> int:
        return max((item.version for item in self.known), default=0)


@dataclass(frozen=True)
class SchemaPreparation:
    """Receipt of ensure_schema: what ran, and where the pre-upgrade copy is."""

    state_before: SchemaState
    applied: tuple[MigrationFile, ...]
    backup_path: Path | None


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    # A brand-new database can pick its auto_vacuum mode for free; an existing
    # one needs a full VACUUM to flip it. INCREMENTAL lets maintenance reclaim
    # space with cheap non-exclusive incremental_vacuum steps instead of a full
    # VACUUM that fights the live MCP daemon for an exclusive lock.
    try:
        if connection.execute("PRAGMA page_count").fetchone()[0] == 0:
            connection.execute("PRAGMA auto_vacuum = INCREMENTAL")
    except sqlite3.OperationalError:
        pass
    # Switching a fresh DB to WAL needs a brief exclusive lock, and SQLite returns
    # SQLITE_BUSY *immediately* (it bypasses the busy handler here to avoid a
    # deadlock) when a peer holds the database open. Two processes cold-starting at
    # the same moment therefore collide. WAL is a persistent property, so retry with
    # jittered backoff until the mode reads back 'wal' (the peer that won the race
    # already set it). Never hard-fail connect over journal mode — a later open
    # will set WAL if every attempt here loses the race.
    for attempt in range(CONNECT_WAL_RETRIES + 1):
        try:
            row = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            if row and str(row[0]).casefold() == "wal":
                break
        except sqlite3.OperationalError as exc:
            message = str(exc).casefold()
            if "locked" not in message and "busy" not in message:
                raise
        if attempt < CONNECT_WAL_RETRIES:
            time.sleep(0.02 * (2 ** attempt) + random.uniform(0.0, 0.03))
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA synchronous = FULL")
    return connection


def ensure_migration_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_utc TEXT NOT NULL
        ) STRICT
        """
    )


def _load_migration_files(config: ChronicleConfig) -> list[MigrationFile]:
    migrations: list[MigrationFile] = []
    seen: dict[int, Path] = {}
    for path in sorted(config.migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        version = int(match.group("version"))
        # Two files sharing a number used to apply the first and silently skip
        # the second, since the version was already recorded as applied.
        if version in seen:
            raise MigrationError(
                f"duplicate migration number {version:04d}: {seen[version].name} and {path.name}"
            )
        seen[version] = path
        migrations.append(
            MigrationFile(
                version=version,
                name=match.group("name").replace("_", " "),
                path=path,
            )
        )
    return migrations


def _prepare_migration_v8(connection: sqlite3.Connection) -> None:
    """Idempotent preconditions for 0008_memory_v3.

    SQLite has no ADD COLUMN IF NOT EXISTS, and some live databases already
    carry events.content_hash from an earlier hotfix path. Checking here
    lets 0008 apply cleanly regardless of which side of that hotfix the DB
    started on.
    """
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(events)")
    }
    if "content_hash" not in columns:
        connection.execute("ALTER TABLE events ADD COLUMN content_hash TEXT")


_MIGRATION_PREHOOKS: dict[int, Callable[[sqlite3.Connection], None]] = {
    8: _prepare_migration_v8,
}


def _begin_immediate_with_retry(connection: sqlite3.Connection) -> None:
    for attempt in range(MIGRATION_LOCK_RETRIES + 1):
        try:
            connection.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).casefold()
            if "locked" not in message and "busy" not in message:
                raise
            if attempt >= MIGRATION_LOCK_RETRIES:
                raise
            time.sleep(MIGRATION_LOCK_RETRY_BASE_SECONDS * (2 ** attempt))


def _statement_has_sql(statement: str) -> bool:
    for line in statement.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("--"):
            return True
    return False


def _execute_sql_script_in_transaction(connection: sqlite3.Connection, sql: str) -> None:
    pending: list[str] = []
    for line in sql.splitlines(keepends=True):
        pending.append(line)
        statement = "".join(pending)
        if sqlite3.complete_statement(statement):
            if _statement_has_sql(statement):
                connection.execute(statement)
            pending = []
    remainder = "".join(pending)
    if _statement_has_sql(remainder):
        raise sqlite3.OperationalError("incomplete SQL migration statement")


def _apply_pending_locked(connection: sqlite3.Connection, config: ChronicleConfig) -> list[MigrationFile]:
    """Apply missing migrations inside the caller's write transaction."""
    applied_now: list[MigrationFile] = []
    ensure_migration_table(connection)
    applied_versions = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations")
    }

    connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")

    for migration in _load_migration_files(config):
        if migration.version in applied_versions:
            continue
        sql = migration.path.read_text(encoding="utf-8")
        prehook = _MIGRATION_PREHOOKS.get(migration.version)
        if prehook is not None:
            prehook(connection)
        _execute_sql_script_in_transaction(connection, sql)
        connection.execute(
            """
            INSERT OR IGNORE INTO schema_migrations(version, name, applied_at_utc)
            VALUES (?, ?, ?)
            """,
            (migration.version, migration.name, utc_now()),
        )
        inserted = int(connection.execute("SELECT changes()").fetchone()[0])
        applied_versions.add(migration.version)
        if inserted:
            applied_now.append(migration)
    latest_version = max(applied_versions, default=0)
    connection.execute(f"PRAGMA user_version = {latest_version}")
    return applied_now


def apply_migrations(connection: sqlite3.Connection, config: ChronicleConfig) -> list[MigrationFile]:
    _begin_immediate_with_retry(connection)
    try:
        applied_now = _apply_pending_locked(connection, config)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return applied_now


def read_schema_state(connection: sqlite3.Connection, config: ChronicleConfig) -> SchemaState:
    """Compare applied migrations with the files on disk, without writing."""
    has_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
    ).fetchone()
    applied = (
        frozenset(int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations"))
        if has_table
        else frozenset()
    )
    has_objects = (
        connection.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").fetchone()
        is not None
    )
    application_id = int(connection.execute("PRAGMA application_id").fetchone()[0])
    return SchemaState(
        applied=applied,
        known=tuple(_load_migration_files(config)),
        has_objects=has_objects,
        application_id=application_id,
    )


def check_schema_policy(
    state: SchemaState,
    config: ChronicleConfig,
    *,
    allow_upgrade: bool,
    allow_init: bool = True,
) -> None:
    """Raise unless this caller may use the database in `state` as it is or bring it current."""
    if state.foreign:
        raise MigrationError(
            f"{config.db_path} is not a Chronicle database (it has other objects or application id "
            f"{state.application_id:#x} and no Chronicle migration history); refusing to modify it."
        )
    if state.unknown:
        raise SchemaAheadOfCode(
            f"{config.db_path} carries migrations this code does not know "
            f"({', '.join(f'{version:04d}' for version in state.unknown)}); it is at "
            f"v{state.current_version} and this code knows up to v{state.target_version}. "
            "Upgrade max-chronicle before using this database."
        )
    if not state.pending:
        return
    if state.fresh:
        if not allow_init:
            raise SchemaMigrationRequired(
                f"{config.db_path} is not initialised. Run `chronicle migrate` (or `chronicle init`)."
            )
        return
    if not allow_upgrade:
        raise SchemaMigrationRequired(
            f"{config.db_path} is at schema v{state.current_version}; this code expects "
            f"v{state.target_version} (pending: "
            f"{', '.join(f'{item.version:04d}' for item in state.pending)}). Run `chronicle migrate`, "
            "which backs the database up first, or restart the server with `chronicle-mcp --migrate`."
        )


def _backup_database(db_path: Path, *, label: str) -> Path:
    """Copy the committed database next to itself through its own connection.

    Called while the upgrader holds the write lock, so no writer can commit
    between this copy and the migration. The name is unique and created
    exclusively: two upgraders can never share, or overwrite, one backup.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    target = db_path.with_name(f"{db_path.name}.bak-{label}-{stamp}-{uuid.uuid4().hex[:8]}")
    os.close(os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
    deadline = time.monotonic() + MIGRATION_BACKUP_TIMEOUT_SECONDS

    def _within_deadline(_status: int, _remaining: int, _total: int) -> None:
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"backup of {db_path} did not finish within {MIGRATION_BACKUP_TIMEOUT_SECONDS:.0f}s"
            )

    source = sqlite3.connect(db_path)
    try:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination, pages=MIGRATION_BACKUP_PAGES_PER_STEP, progress=_within_deadline)
        finally:
            destination.close()
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    finally:
        source.close()
    return target


def ensure_schema(
    connection: sqlite3.Connection,
    config: ChronicleConfig,
    *,
    allow_upgrade: bool,
    allow_init: bool = True,
) -> SchemaPreparation:
    """Bring the schema to this code's version under an explicit policy.

    An empty database initialises (unless allow_init is off). An existing
    database with pending migrations upgrades only when the caller allows it,
    after an online backup; otherwise it raises SchemaMigrationRequired, so a
    stray process (another checkout, an old job, a test aimed at the wrong
    root) cannot change a live schema just by connecting. A database carrying
    migrations this code does not know raises SchemaAheadOfCode, and another
    application's SQLite file is refused outright.

    The decision, the backup and the upgrade all happen under one write lock:
    concurrent upgraders serialise, and the second one finds nothing to do.
    """
    _begin_immediate_with_retry(connection)
    try:
        state = read_schema_state(connection, config)
        check_schema_policy(state, config, allow_upgrade=allow_upgrade, allow_init=allow_init)
        if not state.pending:
            connection.rollback()
            return SchemaPreparation(state_before=state, applied=(), backup_path=None)
        backup_path = None
        if not state.fresh:
            backup_path = _backup_database(
                config.db_path,
                label=f"premigrate-v{state.current_version}-v{state.target_version}",
            )
        applied = _apply_pending_locked(connection, config)
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    return SchemaPreparation(state_before=state, applied=tuple(applied), backup_path=backup_path)


def schema_target_version(config: ChronicleConfig) -> int:
    """The newest migration this code ships for `config`."""
    return max((item.version for item in _load_migration_files(config)), default=0)


def table_count(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def database_summary(connection: sqlite3.Connection) -> dict[str, object]:
    summary: dict[str, object] = {
        "journal_mode": connection.execute("PRAGMA journal_mode").fetchone()[0],
        "foreign_keys": connection.execute("PRAGMA foreign_keys").fetchone()[0],
        "application_id": connection.execute("PRAGMA application_id").fetchone()[0],
        "user_version": connection.execute("PRAGMA user_version").fetchone()[0],
        "migrations": table_count(connection, "schema_migrations"),
        "entities": table_count(connection, "entities"),
        "events": table_count(connection, "events"),
        "snapshots": table_count(connection, "snapshots"),
        "artifacts": table_count(connection, "artifacts"),
        "artifact_links": table_count(connection, "artifact_links"),
        "relations": table_count(connection, "relations"),
        "ingest_runs": table_count(connection, "ingest_runs"),
        "mem0_outbox": table_count(connection, "mem0_outbox"),
        "projection_runs": table_count(connection, "projection_runs"),
        "automation_runs": table_count(connection, "automation_runs"),
        "backup_runs": table_count(connection, "backup_runs"),
        "hook_events": table_count(connection, "hook_events"),
        "curation_runs": table_count(connection, "curation_runs"),
        "event_external_refs": table_count(connection, "event_external_refs"),
        "normalized_entities": table_count(connection, "normalized_entities"),
        "situation_models": table_count(connection, "situation_models"),
        "lens_runs": table_count(connection, "lens_runs"),
        "scenario_runs": table_count(connection, "scenario_runs"),
        "forecast_reviews": table_count(connection, "forecast_reviews"),
    }

    latest_event = connection.execute(
        """
        SELECT occurred_at_utc, event_type, category, entity_id, text
        FROM events
        ORDER BY occurred_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    latest_snapshot = connection.execute(
        """
        SELECT captured_at_utc, domain, COALESCE(title, label, summary_text) AS summary
        FROM snapshots
        ORDER BY captured_at_utc DESC
        LIMIT 1
        """
    ).fetchone()

    summary["latest_event"] = dict(latest_event) if latest_event else None
    summary["latest_snapshot"] = dict(latest_snapshot) if latest_snapshot else None
    return summary
