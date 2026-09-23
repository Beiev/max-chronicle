"""Lexical recall for natural questions: function words, ё, Unicode letters, stems."""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import replace
from pathlib import Path
import shutil

import pytest

from max_chronicle import embeddings, recall, service, store
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
        ("почему не работает бэкап?", ["не", "работает", "бэкап"]),
        ("what did we decide in May?", ["decide", "may"]),
        ("Е\u0308лка в холле", ["елка", "холле"]),
        ("отчёт за маи\u0306", ["отчет", "маи"]),
        ("Straße plan", ["straße", "plan"]),
        ("что это?", ["что", "это"]),
    ],
    ids=[
        "ru-question",
        "yo",
        "uk",
        "en-question",
        "joiners",
        "negation",
        "month",
        "decomposed-yo",
        "decomposed-short-i",
        "sharp-s",
        "only-function-words",
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


def test_coverage_reads_words_as_the_index_does() -> None:
    assert covers(query_terms("dropped gpt_image_2"), "We dropped gpt-image-2 in July.")
    assert covers(query_terms("cafe menu"), "Café menu updated.")
    assert covers(query_terms("straße plan"), "Straße plan approved.")
    assert not covers(query_terms("gpt-image dropped today"), "We dropped the gptimage model.")
    assert not covers(query_terms("viet budget missing"), "Việt budget")  # two accents stay


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


def test_project_and_domain_slugs_are_not_searchable_text(loaded_manifest, lexical_only) -> None:
    _record(loaded_manifest, "Chose SQLite for the local store.", domain="global")

    assert _found(loaded_manifest, "chronicle") == []
    assert _found(loaded_manifest, "global") == []


def test_a_decomposed_query_finds_decomposed_text(loaded_manifest, lexical_only) -> None:
    event_id = _record(loaded_manifest, "Отчёт за маи\u0306 отправлен.")

    assert _found(loaded_manifest, "маи\u0306") == [event_id]


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


def test_a_strict_fact_match_keeps_recall_strict(loaded_manifest, lexical_only) -> None:
    decided = _record(
        loaded_manifest,
        "Reporting decided.",
        task_id="ship",
        fact={"slot": "reporting", "value": "Отчётность ведём в SQLite и Grafana", "kind": "decision"},
    )
    _record(loaded_manifest, "Отчётность и SQLite обсудили на созвоне.")

    result = service.query_memory(loaded_manifest, query="отчетность sqlite grafana")

    assert [hit["event_id"] for hit in result["results"]] == [decided] and result["relaxed"] is False


def _rare_term_corpus(manifest: dict) -> str:
    """Rows holding one rare term outrank the one row holding most terms."""
    for number in range(20):
        _record(manifest, f"Alpha note {number}." if number % 2 else f"Beta note {number}.")
    for number in range(6):
        _record(manifest, f"Gamma gamma gamma, sample {number}.")
    return _record(manifest, "Alpha and beta agree.")


def test_relaxed_search_reads_past_rows_that_hold_one_rare_term(loaded_manifest, lexical_only) -> None:
    target = _rare_term_corpus(loaded_manifest)

    found = _found(loaded_manifest, "alpha beta gamma", limit=1, relaxed=True)

    assert found == [target]


def test_relaxed_pages_read_one_snapshot(loaded_manifest, lexical_only, monkeypatch) -> None:
    target = _rare_term_corpus(loaded_manifest)
    config = config_from_manifest(loaded_manifest)
    original = store.open_connection
    first_page: list[str] = []

    class Cursor:
        def __init__(self, cursor):
            self.cursor = cursor

        def fetchall(self):
            rows = self.cursor.fetchall()
            if not first_page and rows and "rank" in rows[0].keys():
                first_page.extend(row["id"] for row in rows)
                # Another agent rewrites those rows before the next page is read.
                with original(config) as writer, writer:
                    writer.executemany("UPDATE events SET text = 'unrelated note' WHERE id = ?", [(i,) for i in first_page])
            return rows

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def execute(self, *args, **kwargs):
            return Cursor(self.connection.execute(*args, **kwargs))

    @contextmanager
    def interleaved(active_config):
        with original(active_config) as connection:
            yield Connection(connection)

    monkeypatch.setattr(store, "open_connection", interleaved)

    found = [hit["id"] for hit in store.search_events(config, query="alpha beta gamma", limit=1, relaxed=True)]

    assert first_page and found == [target]


def test_a_fact_recorded_after_the_pool_does_not_block_relaxed_recall(
    loaded_manifest, lexical_only, monkeypatch
) -> None:
    target = _record(loaded_manifest, "Alpha and beta agree on the alternative.")
    original = recall.fetch_recall_pool

    def pool_then_fact(*args, **kwargs):
        pool = original(*args, **kwargs)
        _record(
            loaded_manifest,
            "Decision recorded.",
            task_id="ship",
            fact={"slot": "platform", "value": "alpha beta gamma", "kind": "decision"},
        )
        return pool

    monkeypatch.setattr(recall, "fetch_recall_pool", pool_then_fact)

    result = service.query_memory(loaded_manifest, query="alpha beta gamma")

    assert [hit["event_id"] for hit in result["results"]] == [target] and result["relaxed"] is True


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
