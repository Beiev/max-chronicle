"""Schema changes happen on purpose, never as a side effect of connecting.

A process that merely opens a database (a job, a hook, another checkout, a
test aimed at the wrong root) must not upgrade a live schema. Upgrades run
through `chronicle migrate` or server start, after an online backup.
"""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import json
import logging
from pathlib import Path
import shutil
import sqlite3
import sys

import pytest

from max_chronicle import cli
import max_chronicle.mcp_server as mcp_server_module
from max_chronicle.config import MIGRATIONS_DIR, ChronicleConfigError, default_config, resolve_status_root
from max_chronicle.db import (
    MigrationError,
    SchemaAheadOfCode,
    SchemaMigrationRequired,
    connect,
    ensure_schema,
    read_schema_state,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import config_from_manifest, open_connection, prepare_database, reset_migration_cache

LATEST_VERSION = max(int(path.name[:4]) for path in MIGRATIONS_DIR.glob("*.sql"))


def _config(db_path: Path, migrations_dir: Path = MIGRATIONS_DIR):
    return replace(default_config(db_path), migrations_dir=migrations_dir)


def _migrations_up_to(tmp_path: Path, last_version: int) -> Path:
    """The migration set an older release shipped."""
    target = tmp_path / f"migrations-v{last_version}"
    target.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= last_version:
            shutil.copy2(path, target / path.name)
    return target


def _older_database(tmp_path: Path) -> Path:
    """A database built by the previous release: every migration but the last."""
    db_path = tmp_path / "chronicle.db"
    with open_connection(_config(db_path, _migrations_up_to(tmp_path, LATEST_VERSION - 1))):
        pass
    reset_migration_cache()
    return db_path


def _schema_version(db_path: Path) -> int:
    with closing(sqlite3.connect(db_path)) as connection:
        return int(connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0])


def _backups(db_path: Path) -> list[Path]:
    return sorted(db_path.parent.glob(f"{db_path.name}.bak-*"))


def _run_cli(monkeypatch, capsys, *argv: str) -> dict:
    monkeypatch.setattr(sys, "argv", ["chronicle", *argv])
    assert cli.main() == 0
    return json.loads(capsys.readouterr().out)


def test_new_database_initialises_on_first_connect(tmp_path) -> None:
    config = _config(tmp_path / "chronicle.db")

    with open_connection(config) as connection:
        state = read_schema_state(connection, config)

    assert state.pending == ()
    assert state.current_version == LATEST_VERSION
    assert _backups(config.db_path) == []


def test_database_behind_this_code_is_not_upgraded_by_connecting(tmp_path) -> None:
    db_path = _older_database(tmp_path)

    with pytest.raises(SchemaMigrationRequired) as excinfo:
        with open_connection(_config(db_path)):
            pass

    assert "chronicle migrate" in str(excinfo.value)
    assert _schema_version(db_path) == LATEST_VERSION - 1
    assert _backups(db_path) == []


def test_auto_migrate_opt_in_upgrades_after_an_online_backup(tmp_path, monkeypatch) -> None:
    db_path = _older_database(tmp_path)
    monkeypatch.setenv("CHRONICLE_AUTO_MIGRATE", "1")

    with open_connection(_config(db_path)):
        pass

    assert _schema_version(db_path) == LATEST_VERSION
    [backup] = _backups(db_path)
    assert f"premigrate-v{LATEST_VERSION - 1}-v{LATEST_VERSION}" in backup.name
    assert _schema_version(backup) == LATEST_VERSION - 1


def test_migrate_command_upgrades_and_reports_the_backup(tmp_path, monkeypatch, capsys) -> None:
    db_path = _older_database(tmp_path)

    payload = _run_cli(monkeypatch, capsys, "--db", str(db_path), "migrate")

    assert [item["version"] for item in payload["applied"]] == [LATEST_VERSION]
    assert payload["backup_path"] == str(_backups(db_path)[0])
    assert _schema_version(Path(payload["backup_path"])) == LATEST_VERSION - 1
    assert payload["summary"]["user_version"] == LATEST_VERSION


def test_status_reports_a_pending_schema_without_migrating(tmp_path, monkeypatch, capsys) -> None:
    db_path = _older_database(tmp_path)

    payload = _run_cli(monkeypatch, capsys, "--db", str(db_path), "status")

    assert payload["schema"]["pending"] == [LATEST_VERSION]
    assert payload["schema"]["current_version"] == LATEST_VERSION - 1
    assert payload["summary"] is None
    assert "chronicle migrate" in payload["message"]
    assert _schema_version(db_path) == LATEST_VERSION - 1


def test_database_newer_than_this_code_is_refused(tmp_path) -> None:
    db_path = tmp_path / "chronicle.db"
    with open_connection(_config(db_path)):
        pass
    reset_migration_cache()
    older_code = _config(db_path, _migrations_up_to(tmp_path, LATEST_VERSION - 1))

    with pytest.raises(SchemaAheadOfCode):
        with open_connection(older_code):
            pass
    with pytest.raises(SchemaAheadOfCode):
        prepare_database(older_code, allow_upgrade=True)
    assert _schema_version(db_path) == LATEST_VERSION


def test_duplicate_migration_numbers_are_rejected(tmp_path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_first.sql").write_text("CREATE TABLE a(x INTEGER);", encoding="utf-8")
    (migrations_dir / "0001_second.sql").write_text("CREATE TABLE b(x INTEGER);", encoding="utf-8")
    config = _config(tmp_path / "chronicle.db", migrations_dir)

    with closing(connect(config.db_path)) as connection:
        with pytest.raises(MigrationError) as excinfo:
            ensure_schema(connection, config, allow_upgrade=True)

    assert "0001_first.sql" in str(excinfo.value)
    assert "0001_second.sql" in str(excinfo.value)


def test_server_start_upgrades_but_a_read_only_start_only_checks(tmp_path) -> None:
    db_path = _older_database(tmp_path)
    config = _config(db_path)

    with pytest.raises(SchemaMigrationRequired):
        prepare_database(config, allow_upgrade=False)
    prepared = prepare_database(config, allow_upgrade=True)

    assert [item.version for item in prepared.applied] == [LATEST_VERSION]
    assert prepared.state_before.current_version == LATEST_VERSION - 1
    assert prepared.backup_path is not None and prepared.backup_path.exists()
    assert _schema_version(db_path) == LATEST_VERSION


def test_server_refuses_to_start_against_a_newer_schema(chronicle_sandbox, monkeypatch, caplog) -> None:
    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, applied_at_utc) VALUES (9999, 'future', '2099-01-01T00:00:00Z')"
        )
        connection.commit()
    reset_migration_cache()
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path)])

    with caplog.at_level(logging.ERROR, logger="max_chronicle.mcp"):
        exit_code = mcp_server_module._main()

    assert exit_code == 1
    assert "SchemaAheadOfCode" in caplog.text


def test_require_root_refuses_the_implicit_fallback(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_REQUIRE_ROOT", "1")

    with pytest.raises(ChronicleConfigError):
        resolve_status_root()
    assert resolve_status_root(manifest_path=tmp_path / "SSOT_MANIFEST.toml") == tmp_path
    monkeypatch.setenv("CHRONICLE_ROOT", str(tmp_path))
    assert resolve_status_root() == tmp_path


def test_the_test_run_cannot_reach_a_real_workspace(tmp_path_factory) -> None:
    root = resolve_status_root()

    assert root == tmp_path_factory.getbasetemp() / "max-chronicle-home"
