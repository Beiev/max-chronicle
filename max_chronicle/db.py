from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3

from .config import ChronicleConfig


MIGRATION_RE = re.compile(r"^(?P<version>\d{4})_(?P<name>.+)\.sql$")
APPLICATION_ID = 0x4348524E  # CHRN


@dataclass(frozen=True)
class MigrationFile:
    version: int
    name: str
    path: Path


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA busy_timeout = 5000")
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
    for path in sorted(config.migrations_dir.glob("*.sql")):
        match = MIGRATION_RE.match(path.name)
        if not match:
            continue
        migrations.append(
            MigrationFile(
                version=int(match.group("version")),
                name=match.group("name").replace("_", " "),
                path=path,
            )
        )
    return migrations


def apply_migrations(connection: sqlite3.Connection, config: ChronicleConfig) -> list[MigrationFile]:
    ensure_migration_table(connection)
    applied_versions = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migrations")
    }
    applied_now: list[MigrationFile] = []

    connection.execute(f"PRAGMA application_id = {APPLICATION_ID}")

    for migration in _load_migration_files(config):
        if migration.version in applied_versions:
            continue
        sql = migration.path.read_text(encoding="utf-8")
        with connection:
            connection.executescript(sql)
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
