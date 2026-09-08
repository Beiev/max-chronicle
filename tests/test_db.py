from __future__ import annotations

from dataclasses import replace
import threading
import time

import max_chronicle.db as db_module
from max_chronicle.config import default_config
from max_chronicle.db import apply_migrations, connect


def test_apply_migrations_does_not_double_apply_under_concurrency(tmp_path, monkeypatch) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_counter.sql").write_text(
        """
        CREATE TABLE IF NOT EXISTS migration_counter(value INTEGER NOT NULL);
        INSERT INTO migration_counter(value) VALUES (1);
        """,
        encoding="utf-8",
    )
    config = replace(default_config(tmp_path / "chronicle.db"), migrations_dir=migrations_dir)
    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def slow_pre_hook(connection) -> None:
        time.sleep(0.1)

    monkeypatch.setitem(db_module._MIGRATION_PREHOOKS, 1, slow_pre_hook)

    def worker() -> None:
        try:
            connection = connect(config.db_path)
        except BaseException as exc:  # pragma: no cover - test helper
            errors.append(exc)
            barrier.abort()  # never leave the peer blocked on the barrier
            return
        try:
            barrier.wait()
            apply_migrations(connection, config)
        except BaseException as exc:  # pragma: no cover - test helper
            errors.append(exc)
        finally:
            connection.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads), "migration workers deadlocked"

    assert errors == []
    with connect(config.db_path) as connection:
        migration_rows = connection.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0]
        side_effect_rows = connection.execute("SELECT COUNT(*) FROM migration_counter").fetchone()[0]
    assert migration_rows == 1
    assert side_effect_rows == 1
