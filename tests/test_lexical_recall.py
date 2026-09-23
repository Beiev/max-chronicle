"""Lexical recall for natural questions: function words, ё, Unicode letters, stems."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from pathlib import Path
import shutil

import pytest

from max_chronicle import embeddings, service
from max_chronicle.config import MIGRATIONS_DIR, default_config
from max_chronicle.db import apply_migrations, connect
from max_chronicle.lexical import covers, query_terms, relaxed_query, strict_query
from max_chronicle.store import config_from_manifest, search_events, search_fact_events


@pytest.mark.parametrize(
    ("query", "terms"),
    [
        ("Почему отказались от Kubernetes?", ["отказались", "kubernetes"]),
        ("отчёт за июль", ["отчет", "июль"]),
        ("Звіт про тестування відео", ["звіт", "тестування", "відео"]),
        ("why did we drop gpt-image-2?", ["drop", "gpt-image-2"]),
        ("v0.11.0 release notes, release", ["v0.11.0", "release", "notes"]),
        ("что это?", []),
    ],
)
def test_query_terms_keep_content_words_only(query: str, terms: list[str]) -> None:
    assert query_terms(query) == terms


def test_strict_needs_every_term_and_relaxed_matches_stems() -> None:
    terms = ["отказались", "api", "gpt-image-2"]

    assert strict_query(terms) == '"отказались" AND "api" AND "gpt-image-2"'
    assert relaxed_query(terms) == '"отказали"* OR "api" OR "gpt-image-2"'


def test_relaxed_evidence_must_cover_two_thirds_of_the_terms() -> None:
    terms = query_terms("какой фреймворк выбрали для внутреннего API")  # 4 terms, 3 needed

    assert covers(terms, "Выбрали FastAPI для внутреннего API вместо Flask.")
    assert not covers(terms, "Выбрали Linear для задач.")
    assert covers(["перешли", "linear"], "Перешли с Jira на Linear.")


@pytest.fixture()
def lexical_only(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    monkeypatch.setattr(embeddings, "embed_text", lambda *args, **kwargs: None)


def _record(manifest: dict, text: str, **fields) -> str:
    return service.record_event(
        manifest, {"agent": "agent-a", "project": "chronicle", "text": text, **fields}
    )["id"]


def _found(manifest: dict, query: str, **options) -> list[str]:
    return [hit["id"] for hit in search_events(config_from_manifest(manifest), query=query, **options)]


def test_either_spelling_finds_ye_and_yo(loaded_manifest, lexical_only) -> None:
    report = _record(loaded_manifest, "Отчёт за июль отправлен заказчику.")
    backups = _record(loaded_manifest, "Еще раз проверили бэкапы.")

    assert _found(loaded_manifest, "отчет") == [report]
    assert _found(loaded_manifest, "ещё раз бэкапы") == [backups]


def test_the_project_slug_is_not_searchable_text(loaded_manifest, lexical_only) -> None:
    _record(loaded_manifest, "Chose SQLite for the local store.")

    assert _found(loaded_manifest, "chronicle") == []


def test_folding_survives_an_update(loaded_manifest, lexical_only) -> None:
    event_id = _record(loaded_manifest, "Placeholder")
    config = config_from_manifest(loaded_manifest)
    with closing(connect(config.db_path)) as connection, connection:
        connection.execute("UPDATE events SET text = 'Ёлка стоит в холле' WHERE id = ?", (event_id,))

    assert _found(loaded_manifest, "елка холле") == [event_id]


def test_fact_values_are_folded_too(loaded_manifest, lexical_only) -> None:
    event_id = _record(
        loaded_manifest,
        "Reporting decided.",
        task_id="ship",
        fact={"slot": "reporting", "value": "Отчётность ведём в SQLite", "kind": "decision"},
    )

    assert search_fact_events(config_from_manifest(loaded_manifest), query="отчетность") == [event_id]


def test_recall_flags_a_relaxed_match(loaded_manifest, lexical_only) -> None:
    event_id = _record(loaded_manifest, "Выбрали FastAPI для внутреннего API вместо Flask.")

    strict = service.query_memory(loaded_manifest, query="внутреннего API")
    relaxed = service.query_memory(loaded_manifest, query="какой фреймворк выбрали для внутреннего API")
    nothing = service.query_memory(loaded_manifest, query="какую базу данных выбрали для аналитики")

    assert [hit["event_id"] for hit in strict["results"]] == [event_id] and strict["relaxed"] is False
    assert [hit["event_id"] for hit in relaxed["results"]] == [event_id] and relaxed["relaxed"] is True
    assert nothing["results"] == [] and nothing["relaxed"] is False


def test_the_migration_refolds_existing_rows(tmp_path) -> None:
    older = tmp_path / "migrations-v10"
    older.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= 10:
            shutil.copy2(path, older / path.name)
    db_path = tmp_path / "chronicle.db"
    with closing(connect(db_path)) as connection:
        apply_migrations(connection, replace(default_config(db_path), migrations_dir=older))
        connection.execute(
            """INSERT INTO events(id, occurred_at_utc, timezone, recorded_at_utc, event_type, title, text)
            VALUES ('e1', '2026-07-31T16:00:00Z', 'UTC', '2026-07-31T16:00:00Z', 'decision', 'chronicle',
                    'Отчёт за июль отправлен.')"""
        )
        connection.commit()
        config = replace(default_config(db_path), migrations_dir=Path(MIGRATIONS_DIR))
        apply_migrations(connection, config)
        rows = connection.execute("SELECT event_id FROM events_fts WHERE events_fts MATCH '\"отчет\"'").fetchall()

    assert [row[0] for row in rows] == ["e1"]
