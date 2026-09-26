"""One active value per fact slot (W8): the order of a replacement and the unique index of 0015."""

from __future__ import annotations

from dataclasses import replace
import sqlite3

import pytest

from max_chronicle import service
from max_chronicle.memory import task_context
from max_chronicle.store import config_from_manifest, open_connection


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")


def _record(manifest: dict, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields})


def _insert_fact(connection: sqlite3.Connection, fact_id: str, group_id: str | None, value: str,
                 recorded_at: str) -> None:
    tx = connection.execute(
        "INSERT INTO fact_transactions(domain,operation,recorded_at_utc) VALUES ('global','record_episode',?)",
        (recorded_at,),
    ).lastrowid
    connection.execute(
        """INSERT INTO facts(id,domain,group_id,relation,fact_text,fact_hash,recorded_at_utc,created_tx_id,
        slot_key,value_key,cardinality,attributes_json) VALUES (?,'global',?,'deploy.target',?,?,?,?,'deploy.target',?,'single','{}')""",
        (fact_id, group_id, f"deploy.target: {value}", f"hash-{fact_id}", recorded_at, tx, value),
    )


@pytest.mark.parametrize("group_id", ['["atlas","ship"]', '["atlas",null]', "[null,null]", None])
def test_a_slot_takes_no_second_active_value(loaded_manifest, group_id) -> None:
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        _insert_fact(connection, "first", group_id, "staging", "2026-09-01T00:00:00.000Z")
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_fact(connection, "second", group_id, "production", "2026-09-02T00:00:00.000Z")


def test_replacing_through_an_old_spelling_retires_before_it_inserts(loaded_manifest) -> None:
    # Before the registry, "atlas" and "old-atlas" were separate slots. The
    # canonical one is older, so the new canonical value lands on its key.
    _record(loaded_manifest, "Atlas deploys to staging", project="atlas",
            fact={"slot": "deploy.target", "value": "staging", "kind": "decision"})
    newest = _record(loaded_manifest, "Old atlas deploys to production", project="old-atlas",
                     fact={"slot": "deploy.target", "value": "production", "kind": "decision"})["fact_id"]
    manifest = {**loaded_manifest, "projects": [{"id": "atlas", "aliases": ["old-atlas"]}]}

    _record(manifest, "Atlas deploys to canary", project="atlas",
            fact={"slot": "deploy.target", "value": "canary", "kind": "decision", "supersedes": newest})

    facts = task_context(manifest, domain="global", project="atlas", task_id=None)["current_facts"]
    assert [fact["value"] for fact in facts if fact["slot"] == "deploy.target"] == ["canary"]


def _before_0015(config, tmp_path):
    """*config* on the migrations before 0015."""
    from max_chronicle.config import MIGRATIONS_DIR

    early = tmp_path / "migrations"
    early.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= 14:
            (early / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return replace(config, migrations_dir=early)


def test_the_migration_retires_all_but_the_newest_active_value(chronicle_sandbox, loaded_manifest, tmp_path) -> None:
    from max_chronicle.store import prepare_database

    config = config_from_manifest(loaded_manifest)
    with open_connection(_before_0015(config, tmp_path)) as connection:
        _insert_fact(connection, "oldest", '["atlas",null]', "staging", "2026-09-01T00:00:00.000Z")
        _insert_fact(connection, "older", '["atlas", null]', "canary", "2026-09-02T00:00:00.000Z")
        _insert_fact(connection, "newest", '["atlas",null]', "production", "2026-09-03T00:00:00.000Z")
        _insert_fact(connection, "global", "[null,null]", "local", "2026-09-01T00:00:00.000Z")
        _insert_fact(connection, "task", '["atlas","ship"]', "preview", "2026-09-01T00:00:00.000Z")
        connection.commit()

    prepare_database(config, allow_upgrade=True)

    with open_connection(config) as connection:
        status = dict(connection.execute("SELECT id, status FROM facts"))
        retired = connection.execute(
            """SELECT f.id, t.operation, t.reason, l.action FROM facts f JOIN fact_transactions t ON t.id = f.expired_tx_id
            JOIN fact_mutation_log l ON l.fact_id = f.id ORDER BY f.id""").fetchall()
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_fact(connection, "again", '["atlas",null]', "again", "2026-09-04T00:00:00.000Z")

    assert status == {"oldest": "retired", "older": "retired", "newest": "active", "global": "active", "task": "active"}
    assert [(row[0], row[1], row[3]) for row in retired] == [("older", "retire", "retire"), ("oldest", "retire", "retire")]
    assert all("0015" in row[2] for row in retired)


def test_the_migration_takes_a_fact_without_group_as_the_widest_scope(loaded_manifest, tmp_path) -> None:
    from max_chronicle.store import prepare_database

    config = config_from_manifest(loaded_manifest)
    with open_connection(_before_0015(config, tmp_path)) as connection:
        _insert_fact(connection, "older", None, "staging", "2026-09-01T00:00:00.000Z")
        _insert_fact(connection, "newer", None, "production", "2026-09-02T00:00:00.000Z")
        connection.commit()

    prepare_database(config, allow_upgrade=True)

    with open_connection(config) as connection:
        status = dict(connection.execute("SELECT id, status FROM facts"))
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            _insert_fact(connection, "again", None, "again", "2026-09-03T00:00:00.000Z")
    assert status == {"older": "retired", "newer": "active"}


def test_the_migration_is_one_pass(loaded_manifest, tmp_path) -> None:
    import time

    from max_chronicle.store import prepare_database

    config = config_from_manifest(loaded_manifest)
    with open_connection(_before_0015(config, tmp_path)) as connection:
        for index in range(8000):  # 4000 slots of two values; a nested scan took 16 s here
            _insert_fact(connection, f"fact-{index}", f'["project-{index // 2}",null]', f"value-{index}",
                         f"2026-09-01T00:00:{index % 2:02d}.000Z")
        connection.commit()

    started = time.monotonic()
    prepare_database(config, allow_upgrade=True)
    elapsed = time.monotonic() - started

    with open_connection(config) as connection:
        active = connection.execute("SELECT count(*) FROM facts WHERE status = 'active'").fetchone()[0]
    assert active == 4000 and elapsed < 3
