"""One active value per fact slot (W8): the order of a replacement and the unique index of 0015."""

from __future__ import annotations

from dataclasses import replace
import json
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
                 recorded_at: str, domain: str = "global") -> None:
    """A fact as the code of the schema at hand writes it: the group_id pair before 0016, columns since."""
    tx = connection.execute(
        "INSERT INTO fact_transactions(domain,operation,recorded_at_utc) VALUES (?,'record_episode',?)",
        (domain, recorded_at),
    ).lastrowid
    project, task_id = json.loads(group_id) if group_id else (None, None)
    attributes = json.dumps({"project": project, "task_id": task_id, "kind": "decision"})
    if "group_id" in {row[1] for row in connection.execute("PRAGMA table_info(facts)")}:
        scope_columns, scope = "group_id,relation", (group_id, "deploy.target")
    else:
        scope_columns, scope = "project,task_id,kind", (project, task_id, "decision")
    connection.execute(
        f"""INSERT INTO facts(id,domain,{scope_columns},fact_text,fact_hash,recorded_at_utc,created_tx_id,
        slot_key,value_key,cardinality,attributes_json)
        VALUES (?,?,{",".join("?" for _ in scope)},?,?,?,?,'deploy.target',?,'single',?)""",
        (fact_id, domain, *scope, f"deploy.target: {value}", f"hash-{fact_id}", recorded_at, tx, value, attributes),
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


def _upto(config, tmp_path, version: int):
    """*config* on the migrations up to *version*."""
    from max_chronicle.config import MIGRATIONS_DIR

    early = tmp_path / f"migrations-v{version}"
    early.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= version:
            (early / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    return replace(config, migrations_dir=early)


def _before_0015(config, tmp_path):
    """*config* on the migrations before 0015."""
    return _upto(config, tmp_path, 14)


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


# Two values per slot: 4,000 slots of one domain took 16 s with a nested scan,
# 16,000 slots of their own domains 4.4 s with an unindexed lookup of the retiring transaction.
@pytest.mark.parametrize(("slots", "own_domains"), [(4000, False), (16000, True)])
def test_the_migration_is_one_pass(loaded_manifest, tmp_path, slots, own_domains) -> None:
    import time

    from max_chronicle.store import prepare_database

    config = config_from_manifest(loaded_manifest)
    with open_connection(_before_0015(config, tmp_path)) as connection:
        for index in range(slots * 2):
            slot = index // 2
            _insert_fact(connection, f"fact-{index}", f'["project-{slot}",null]', f"value-{index}",
                         f"2026-09-01T00:00:{index % 2:02d}.000Z", f"domain-{slot}" if own_domains else "global")
        connection.commit()

    through_0015 = _upto(config, tmp_path, 15)  # 0015 alone: the later migrations are not its cost
    started = time.monotonic()
    prepare_database(through_0015, allow_upgrade=True)
    elapsed = time.monotonic() - started

    with open_connection(through_0015) as connection:
        active = connection.execute("SELECT count(*) FROM facts WHERE status = 'active'").fetchone()[0]
    assert active == slots and elapsed < 3
