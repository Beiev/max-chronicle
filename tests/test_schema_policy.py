"""Schema changes happen on purpose, never as a side effect of connecting.

A process that merely opens a database (a job, a hook, another checkout, a
test aimed at the wrong root) must not upgrade a live schema. Upgrades run
through `chronicle migrate` or `chronicle-mcp --migrate`, after an online
backup taken under the same write lock as the upgrade.
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
import threading

import pytest

from max_chronicle import cli
import max_chronicle.db as db_module
import max_chronicle.mcp_server as mcp_server_module
from max_chronicle.config import (
    EXIT_CONFIG_ERROR,
    EXIT_SCHEMA_ACTION,
    MIGRATIONS_DIR,
    ChronicleConfigError,
    default_config,
    resolve_status_root,
)
from max_chronicle.db import (
    MigrationError,
    SchemaAheadOfCode,
    SchemaMigrationRequired,
    connect,
    ensure_schema,
    read_schema_state,
)
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import (
    config_from_manifest,
    open_connection,
    prepare_database,
    reset_migration_cache,
    set_read_only_process,
)

LATEST_VERSION = max(int(path.name[:4]) for path in MIGRATIONS_DIR.glob("*.sql"))


def _config(db_path: Path, migrations_dir: Path = MIGRATIONS_DIR):
    return replace(default_config(db_path), migrations_dir=migrations_dir)


def _migrations_up_to(tmp_path: Path, last_version: int) -> Path:
    """The migration set an older release shipped."""
    target = tmp_path / f"migrations-v{last_version}"
    target.mkdir(exist_ok=True)
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= last_version:
            shutil.copy2(path, target / path.name)
    return target


def _older_database(tmp_path: Path, db_path: Path | None = None) -> Path:
    """A database built by the previous release: every migration but the last."""
    db_path = db_path or tmp_path / "chronicle.db"
    with open_connection(_config(db_path, _migrations_up_to(tmp_path, LATEST_VERSION - 1))):
        pass
    reset_migration_cache()
    return db_path


def _schema_version(db_path: Path) -> int:
    with closing(sqlite3.connect(db_path)) as connection:
        return int(connection.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0])


def _backups(db_path: Path) -> list[Path]:
    return sorted(db_path.parent.glob(f"{db_path.name}.bak-*"))


def _journal_mode(db_path: Path) -> str:
    with closing(sqlite3.connect(db_path)) as connection:
        return str(connection.execute("PRAGMA journal_mode").fetchone()[0])


def _in_rollback_journal_mode(db_path: Path) -> Path:
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("PRAGMA journal_mode = DELETE")
    return db_path


def _run_cli(monkeypatch, capsys, *argv: str) -> tuple[int, dict]:
    monkeypatch.setattr(sys, "argv", ["chronicle", *argv])
    exit_code = cli.main()
    return exit_code, json.loads(capsys.readouterr().out)


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

    exit_code, payload = _run_cli(monkeypatch, capsys, "--db", str(db_path), "migrate")

    assert exit_code == 0
    assert [item["version"] for item in payload["applied"]] == [LATEST_VERSION]
    assert payload["backup_path"] == str(_backups(db_path)[0])
    assert _schema_version(Path(payload["backup_path"])) == LATEST_VERSION - 1
    assert payload["summary"]["user_version"] == LATEST_VERSION


def test_status_reports_a_pending_schema_without_migrating(tmp_path, monkeypatch, capsys) -> None:
    db_path = _older_database(tmp_path)

    exit_code, payload = _run_cli(monkeypatch, capsys, "--db", str(db_path), "status")

    assert exit_code == cli.EXIT_SCHEMA_ACTION
    assert payload["schema"]["pending"] == [LATEST_VERSION]
    assert payload["schema"]["current_version"] == LATEST_VERSION - 1
    assert payload["summary"] is None
    assert "chronicle migrate" in payload["message"]
    assert _schema_version(db_path) == LATEST_VERSION - 1


def test_concurrent_upgraders_serialise_and_share_one_backup(tmp_path) -> None:
    db_path = _older_database(tmp_path)
    config = _config(db_path)
    barrier = threading.Barrier(2)
    results: list[object] = []
    errors: list[BaseException] = []

    def upgrader() -> None:
        connection = connect(db_path)
        try:
            barrier.wait()
            results.append(ensure_schema(connection, config, allow_upgrade=True))
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)
        finally:
            connection.close()

    threads = [threading.Thread(target=upgrader) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert errors == []
    applied = [[item.version for item in result.applied] for result in results]
    assert sorted(applied) == [[], [LATEST_VERSION]]
    [backup] = _backups(db_path)
    assert _schema_version(backup) == LATEST_VERSION - 1
    assert _schema_version(db_path) == LATEST_VERSION


def test_backup_names_never_collide(tmp_path) -> None:
    db_path = _older_database(tmp_path)

    first = db_module._backup_database(db_path, label="premigrate-v9-v10")
    second = db_module._backup_database(db_path, label="premigrate-v9-v10")

    assert first != second
    assert _schema_version(first) == _schema_version(second) == LATEST_VERSION - 1


def test_another_applications_sqlite_file_is_left_untouched(tmp_path) -> None:
    db_path = tmp_path / "notes.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE notes(body TEXT)")
        connection.commit()
    config = _config(db_path)
    size = db_path.stat().st_size

    with closing(connect(db_path)) as connection:
        with pytest.raises(MigrationError, match="not a Chronicle database"):
            ensure_schema(connection, config, allow_upgrade=True)

    with closing(sqlite3.connect(db_path)) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        application_id = connection.execute("PRAGMA application_id").fetchone()[0]
    assert tables == {"notes"}
    assert application_id == 0
    assert _journal_mode(db_path) == "delete"  # connecting did not switch it to WAL
    assert db_path.stat().st_size == size and not Path(f"{db_path}-wal").exists()


def test_a_foreign_file_without_migration_history_is_refused_even_at_the_current_version(tmp_path) -> None:
    db_path = tmp_path / "chronicle.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE notes(body TEXT)")
        connection.execute(f"PRAGMA user_version = {LATEST_VERSION}")
        connection.commit()

    with pytest.raises(MigrationError, match="not a Chronicle database"):
        with open_connection(_config(db_path)):
            pass


@pytest.mark.parametrize("read_only", [False, True], ids=["writer", "read-only"])
def test_an_empty_migration_history_is_refused_at_the_current_version(tmp_path, read_only) -> None:
    db_path = tmp_path / "chronicle.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE unrelated(body TEXT)")
        connection.execute("CREATE TABLE schema_migrations(version INTEGER, name TEXT, applied_at_utc TEXT)")
        connection.execute(f"PRAGMA user_version = {LATEST_VERSION}")
        connection.commit()
    set_read_only_process(read_only)

    with pytest.raises(MigrationError, match="not a Chronicle database"):
        with open_connection(_config(db_path)):
            pass


def test_every_connection_enforces_foreign_keys(tmp_path) -> None:
    config = _config(tmp_path / "chronicle.db")

    with open_connection(config) as connection:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_a_read_only_process_never_creates_upgrades_or_writes(tmp_path) -> None:
    current = _config(tmp_path / "current.db")
    with open_connection(current) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS probe(x INTEGER)")
        connection.commit()
    older = _config(_older_database(tmp_path, tmp_path / "older.db"))
    missing = _config(tmp_path / "missing.db")
    set_read_only_process(True)

    with open_connection(current) as connection:
        assert connection.execute("SELECT count(*) FROM probe").fetchone()[0] == 0
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("INSERT INTO probe VALUES (1)")
    with pytest.raises(SchemaMigrationRequired):
        with open_connection(older):
            pass
    with pytest.raises(SchemaMigrationRequired, match="never creates"):
        with open_connection(missing):
            pass

    assert not missing.db_path.exists()
    assert _schema_version(older.db_path) == LATEST_VERSION - 1


def test_a_foreign_file_with_a_matching_version_is_refused(tmp_path) -> None:
    db_path = tmp_path / "chronicle.db"
    with closing(sqlite3.connect(db_path)) as connection:
        connection.execute("CREATE TABLE schema_migrations(version INTEGER, name TEXT, applied_at_utc TEXT)")
        connection.execute("INSERT INTO schema_migrations VALUES (1, 'theirs', '2026-01-01T00:00:00Z')")
        connection.execute(f"PRAGMA user_version = {LATEST_VERSION}")
        connection.execute("PRAGMA application_id = 123")
        connection.commit()

    with pytest.raises(MigrationError, match="not a Chronicle database"):
        with open_connection(_config(db_path)):
            pass


def test_a_swapped_older_file_is_caught_on_the_next_connection(tmp_path) -> None:
    db_path = tmp_path / "chronicle.db"
    config = _config(db_path)
    with open_connection(config):
        pass
    older = _older_database(tmp_path, tmp_path / "older.db")
    shutil.copy2(older, db_path)  # e.g. a restore, while this process keeps running

    with pytest.raises(SchemaMigrationRequired):
        with open_connection(config):
            pass


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


def test_server_start_upgrades_only_when_asked(tmp_path) -> None:
    db_path = _older_database(tmp_path)
    config = _config(db_path)

    with pytest.raises(SchemaMigrationRequired):
        prepare_database(config, allow_upgrade=False)
    prepared = prepare_database(config, allow_upgrade=True)

    assert [item.version for item in prepared.applied] == [LATEST_VERSION]
    assert prepared.state_before.current_version == LATEST_VERSION - 1
    assert prepared.backup_path is not None and prepared.backup_path.exists()
    assert _schema_version(db_path) == LATEST_VERSION


def test_read_only_start_never_creates_or_changes_a_database(tmp_path) -> None:
    missing = _config(tmp_path / "missing.db")
    with pytest.raises(SchemaMigrationRequired, match="never creates"):
        prepare_database(missing, allow_upgrade=True, read_only=True)
    assert not missing.db_path.exists()

    db_path = _in_rollback_journal_mode(_older_database(tmp_path))
    with pytest.raises(SchemaMigrationRequired):
        prepare_database(_config(db_path), allow_upgrade=True, read_only=True)
    assert _schema_version(db_path) == LATEST_VERSION - 1
    assert _backups(db_path) == []
    assert _journal_mode(db_path) == "delete"

    empty = tmp_path / "empty.db"
    empty.touch()
    with pytest.raises(SchemaMigrationRequired):
        prepare_database(_config(empty), allow_upgrade=True, read_only=True)
    assert empty.stat().st_size == 0 and not Path(f"{empty}-wal").exists()


def _outdated_sandbox_database(chronicle_sandbox, tmp_path) -> Path:
    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    for suffix in ("", "-wal", "-shm"):
        Path(f"{config.db_path}{suffix}").unlink(missing_ok=True)
    return _older_database(tmp_path, config.db_path)


def test_server_without_migrate_refuses_an_outdated_schema(chronicle_sandbox, tmp_path, monkeypatch, caplog) -> None:
    db_path = _outdated_sandbox_database(chronicle_sandbox, tmp_path)
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path)])

    with caplog.at_level(logging.ERROR, logger="max_chronicle.mcp"):
        exit_code = mcp_server_module._main()

    assert exit_code == EXIT_SCHEMA_ACTION
    assert "SchemaMigrationRequired" in caplog.text
    assert "--migrate" in caplog.text
    assert _schema_version(db_path) == LATEST_VERSION - 1


def test_server_refuses_to_start_against_a_newer_schema(chronicle_sandbox, monkeypatch, caplog) -> None:
    config = config_from_manifest(load_manifest(chronicle_sandbox.manifest_path))
    with open_connection(config) as connection:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, applied_at_utc) VALUES (9999, 'future', '2099-01-01T00:00:00Z')"
        )
        connection.commit()
    reset_migration_cache()
    monkeypatch.setattr(
        sys, "argv", ["chronicle-mcp", "--manifest", str(chronicle_sandbox.manifest_path), "--migrate"]
    )

    with caplog.at_level(logging.ERROR, logger="max_chronicle.mcp"):
        exit_code = mcp_server_module._main()

    assert exit_code == EXIT_SCHEMA_ACTION
    assert "SchemaAheadOfCode" in caplog.text


def test_require_root_refuses_the_implicit_fallback(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_REQUIRE_ROOT", "1")

    with pytest.raises(ChronicleConfigError):
        resolve_status_root()
    assert resolve_status_root(manifest_path=tmp_path / "SSOT_MANIFEST.toml") == tmp_path
    monkeypatch.setenv("CHRONICLE_ROOT", str(tmp_path))
    assert resolve_status_root() == tmp_path


def test_a_server_without_a_workspace_exits_like_the_cli(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_REQUIRE_ROOT", "1")
    monkeypatch.setattr(sys, "argv", ["chronicle-mcp"])

    assert mcp_server_module._main() == EXIT_CONFIG_ERROR


def test_require_root_keeps_help_and_explicit_flags_working(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("CHRONICLE_REQUIRE_ROOT", "1")

    for argv, main in ((["chronicle", "--help"], cli.main), (["chronicle-mcp", "--help"], mcp_server_module._main)):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as excinfo:
            main()
        assert excinfo.value.code == 0
    capsys.readouterr()

    exit_code, payload = _run_cli(
        monkeypatch,
        capsys,
        "--db",
        str(tmp_path / "chronicle.db"),
        "--manifest",
        str(tmp_path / "SSOT_MANIFEST.toml"),
        "status",
    )
    assert exit_code == 0 and payload["exists"] is False

    exit_code, payload = _run_cli(monkeypatch, capsys, "status")
    assert exit_code == cli.EXIT_CONFIG_ERROR
    assert payload["error_type"] == "ChronicleConfigError"


def test_the_test_run_cannot_reach_a_real_workspace(tmp_path_factory) -> None:
    root = resolve_status_root()

    assert root == tmp_path_factory.getbasetemp() / "max-chronicle-home"


@pytest.mark.parametrize("given", ["flag", "environment"])
def test_migrate_and_status_open_the_database_the_manifest_names(tmp_path, monkeypatch, capsys, given) -> None:
    named = _older_database(tmp_path, tmp_path / "named.db")
    manifest = tmp_path / "SSOT_MANIFEST.toml"
    manifest.write_text(f'version = 1\n\n[paths]\nchronicle_db = "{named}"\n', encoding="utf-8")
    monkeypatch.setenv("CHRONICLE_ROOT", str(tmp_path))
    if given == "flag":
        argv: tuple[str, ...] = ("--manifest", str(manifest))
    else:
        monkeypatch.setenv("CHRONICLE_MANIFEST", str(manifest))
        argv = ()

    status_code, status = _run_cli(monkeypatch, capsys, *argv, "status")
    migrate_code, migrated = _run_cli(monkeypatch, capsys, *argv, "migrate")

    assert (status_code, status["db_path"]) == (cli.EXIT_SCHEMA_ACTION, str(named))
    assert (migrate_code, migrated["db_path"]) == (0, str(named))
    assert _schema_version(named) == LATEST_VERSION
    assert not (tmp_path / "chronicle.db").exists()
