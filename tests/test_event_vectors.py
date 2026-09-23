"""Event vectors keyed by embedding profile: model choice, Ollama calls, coexisting indexes."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
import io
import json
import math
import shutil
import sqlite3

import pytest

from max_chronicle import embeddings, service
from max_chronicle.config import MIGRATIONS_DIR, default_config
from max_chronicle.db import apply_migrations, connect
from max_chronicle.embeddings import active_profile, embed_document, embed_query, pack_vector
from max_chronicle.store import (
    config_from_manifest,
    fetch_all_event_embeddings,
    fetch_event_ids_without_embedding,
    open_connection,
    store_event_embedding,
)

QWEN = "qwen3-embedding:0.6b"
NOMIC = "nomic-embed-text"
# conftest replaces embed_text with an offline stub for every test; keep the real one.
_REAL_EMBED_TEXT = embeddings.embed_text


def _unit(seed: float, dim: int) -> list[float]:
    raw = [math.sin(seed + i * 0.1) for i in range(dim)]
    size = math.sqrt(sum(x * x for x in raw))
    return [x / size for x in raw]


def _record(manifest: dict, text: str, **fields) -> dict:
    return service.record_event(manifest, {"agent": "agent-a", "domain": "global", "text": text, **fields})


@pytest.fixture()
def without_embedding(monkeypatch):
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    yield
    monkeypatch.delenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS")


# ---------------------------------------------------------------------------
# Profiles and the Ollama call
# ---------------------------------------------------------------------------


def test_the_default_profile_is_multilingual_and_instructs_queries(monkeypatch) -> None:
    monkeypatch.delenv("CHRONICLE_EMBED_MODEL", raising=False)

    profile = active_profile()

    assert profile.model == QWEN and profile.key == QWEN
    assert profile.query_prefix.startswith("Instruct: ") and profile.query_prefix.endswith("\nQuery:")
    assert profile.document_prefix == ""


def test_an_unknown_model_gets_a_plain_profile(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", "bge-m3")

    assert active_profile() == embeddings.EmbeddingProfile(key="bge-m3", model="bge-m3")


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def test_embed_text_asks_ollama_to_truncate_and_returns_the_first_vector(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    sent = {}

    def urlopen(request, timeout):
        sent.update(url=request.full_url, payload=json.loads(request.data), timeout=timeout)
        return _Response(json.dumps({"embeddings": [[0.0, 3.0, 4.0]]}).encode())

    monkeypatch.setattr(embeddings.urllib.request, "urlopen", urlopen)

    vector = _REAL_EMBED_TEXT("Отчёт за июль", timeout=7.0)

    assert vector == [0.0, 3.0, 4.0]
    assert sent["url"].endswith("/api/embed") and sent["timeout"] == 7.0
    assert sent["payload"] == {"model": QWEN, "input": "Отчёт за июль", "truncate": True}


@pytest.mark.parametrize(
    "body",
    [b"not json", b'{"embeddings": []}', b'{"embeddings": [[0.0, 0.0]]}', b'{"error": "model not found"}'],
    ids=["garbage", "empty", "zero", "error"],
)
def test_embed_text_returns_none_on_a_bad_answer(monkeypatch, body: bytes) -> None:
    monkeypatch.setattr(embeddings.urllib.request, "urlopen", lambda request, timeout: _Response(body))

    assert _REAL_EMBED_TEXT("anything") is None


def test_queries_carry_the_instruction_and_documents_do_not(monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    seen = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: seen.append(text) or [1.0])

    embed_query("почему отказались от kubernetes?")
    embed_document("Отказались от Kubernetes: один сервер.")

    assert seen[0] == active_profile().query_prefix + "почему отказались от kubernetes?"
    assert seen[1] == "Отказались от Kubernetes: один сервер."


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def test_migration_0012_keeps_existing_vectors_under_their_model(tmp_path) -> None:
    older = tmp_path / "migrations-v11"
    older.mkdir()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name[:4]) <= 11:
            shutil.copy2(path, older / path.name)
    db_path = tmp_path / "chronicle.db"
    vector = pack_vector(_unit(1.0, 768))
    with closing(connect(db_path)) as connection:
        apply_migrations(connection, replace(default_config(db_path), migrations_dir=older))
        connection.execute(
            """INSERT INTO events(id, occurred_at_utc, timezone, recorded_at_utc, event_type, title, text)
            VALUES ('e1', '2026-07-31T16:00:00Z', 'UTC', '2026-07-31T16:00:00Z', 'decision', 'demo', 'Chose SQLite.')"""
        )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = OFF")  # lets "gone" outlive its event
        for event_id in ("e1", "gone"):
            connection.execute(
                "INSERT INTO event_embeddings(event_id, model, dim, vector, created_at_utc) VALUES (?, ?, 768, ?, '2026-07-31T16:00:00Z')",
                (event_id, NOMIC, vector),
            )
        connection.commit()
        connection.execute("PRAGMA foreign_keys = ON")
        apply_migrations(connection, replace(default_config(db_path), migrations_dir=MIGRATIONS_DIR))
        rows = connection.execute("SELECT event_id, model_key, dim, vector FROM event_vectors").fetchall()
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert [tuple(row) for row in rows] == [("e1", NOMIC, 768, vector)]
    assert "event_embeddings" not in tables


def test_record_event_stores_the_vector_under_the_active_key_at_its_own_size(loaded_manifest, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(2.0, 1024))

    event = _record(loaded_manifest, "Chose Qwen3 embeddings for Russian recall.")

    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        rows = connection.execute("SELECT model_key, dim FROM event_vectors WHERE event_id = ?", (event["id"],)).fetchall()
    assert [tuple(row) for row in rows] == [(QWEN, 1024)]


@pytest.mark.parametrize("value", [math.nan, 1e39], ids=["nan", "beyond-float32"])
def test_a_vector_that_cannot_be_stored_does_not_lose_the_event(loaded_manifest, value: float) -> None:
    event = _record(loaded_manifest, "Event whose vector is malformed", _embed_fn=lambda _text: [value] * 8)

    assert event["chronicle_status"] == "stored"
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        stored = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (event["id"],)).fetchone()[0]
        vectors = connection.execute("SELECT COUNT(*) FROM event_vectors WHERE event_id = ?", (event["id"],)).fetchone()[0]
    assert (stored, vectors) == (1, 0)


def test_an_index_failure_that_rolls_back_a_whole_transaction_keeps_the_event(loaded_manifest, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(2.0, 1024))

    def disk_full(config, event_id, vector, model_key, dim, *, connection=None, **options):
        if connection is not None:  # on SQLITE_FULL, SQLite rolls back the whole transaction
            connection.execute("ROLLBACK")
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(service, "store_event_embedding", disk_full)

    event = _record(loaded_manifest, "Recorded while the disk is nearly full")

    assert event["chronicle_status"] == "stored"
    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        stored = connection.execute("SELECT COUNT(*) FROM events WHERE id = ?", (event["id"],)).fetchone()[0]
        vectors = connection.execute("SELECT COUNT(*) FROM event_vectors WHERE event_id = ?", (event["id"],)).fetchone()[0]
    assert (stored, vectors) == (1, 0)


def test_a_vector_of_another_dimension_is_incompatible_and_replaced(
    loaded_manifest, monkeypatch, without_embedding
) -> None:
    config = config_from_manifest(loaded_manifest)
    old = _record(loaded_manifest, "Indexed before the model behind the key changed")
    new = _record(loaded_manifest, "Indexed after the change")
    store_event_embedding(config, old["id"], _unit(0.0, 768), QWEN, 768)
    store_event_embedding(config, new["id"], _unit(0.0, 1024), QWEN, 1024)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(0.0, 1024))

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert [hit["event_id"] for hit in result["results"]] == [new["id"]] and result["degraded"] is False
    assert result["vector_coverage"]["missing_or_incompatible"] == 1

    report = service.embed_backfill(loaded_manifest)

    assert (report["dim"], report["embedded"]) == (1024, 1)
    assert {len(vector) for _, vector in fetch_all_event_embeddings(config)} == {1024}


def test_an_index_of_another_dimension_only_is_empty(loaded_manifest, monkeypatch, without_embedding) -> None:
    event = _record(loaded_manifest, "Indexed before the model behind the key changed")
    store_event_embedding(config_from_manifest(loaded_manifest), event["id"], _unit(0.0, 768), QWEN, 768)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(0.0, 1024))

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert result["degraded"] is True and result["channel_errors"]["vector"] == "embedding_index_empty"


def test_a_capture_never_replaces_a_vector_a_backfill_stored_meanwhile(loaded_manifest, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(1.0, 768))  # the old dimension
    real_store = service.store_event_embedding

    def backfill_lands_first(config, event_id, vector, model_key, dim, **options):
        real_store(config, event_id, _unit(1.0, 1024), model_key, 1024)
        real_store(config, event_id, vector, model_key, dim, **options)

    monkeypatch.setattr(service, "store_event_embedding", backfill_lands_first)

    event = _record(loaded_manifest, "Recorded while the model behind the key changed")

    with open_connection(config_from_manifest(loaded_manifest)) as connection:
        dim = connection.execute("SELECT dim FROM event_vectors WHERE event_id = ?", (event["id"],)).fetchone()[0]
    assert dim == 1024


def test_backfill_stops_at_once_when_the_backend_is_unavailable(loaded_manifest, monkeypatch, without_embedding) -> None:
    for n in range(3):
        _record(loaded_manifest, f"Event {n}")
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    calls = []
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: calls.append(text))

    report = service.embed_backfill(loaded_manifest)

    assert len(calls) == 1  # the dimension probe, not one timeout per event
    assert report["backend_unavailable"] is True and (report["embedded"], report["failed"]) == (0, 3)


def test_two_indexes_coexist_and_backfill_fills_only_the_active_one(
    loaded_manifest, monkeypatch, without_embedding
) -> None:
    config = config_from_manifest(loaded_manifest)
    first = _record(loaded_manifest, "First decision")
    second = _record(loaded_manifest, "Second decision")
    for event in (first, second):
        store_event_embedding(config, event["id"], _unit(3.0, 768), NOMIC, 768)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(4.0, 1024))

    assert set(fetch_event_ids_without_embedding(config)) == {first["id"], second["id"]}
    report = service.embed_backfill(loaded_manifest)

    assert report["model_key"] == QWEN and report["embedded"] == 2
    assert fetch_event_ids_without_embedding(config) == []
    assert {len(vector) for _, vector in fetch_all_event_embeddings(config)} == {1024}
    assert {len(vector) for _, vector in fetch_all_event_embeddings(config, model_key=NOMIC)} == {768}


# ---------------------------------------------------------------------------
# Recall
# ---------------------------------------------------------------------------


def test_recall_ranks_with_the_active_index_only(loaded_manifest, monkeypatch, without_embedding) -> None:
    config = config_from_manifest(loaded_manifest)
    target = _record(loaded_manifest, "Выбрали SQLite для локального хранилища.")
    other = _record(loaded_manifest, "Обновили README.")
    store_event_embedding(config, target["id"], _unit(0.0, 1024), QWEN, 1024)
    store_event_embedding(config, other["id"], [-x for x in _unit(0.0, 1024)], QWEN, 1024)
    store_event_embedding(config, other["id"], _unit(0.0, 768), NOMIC, 768)  # a stale index would pick this one
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(0.0, 1024))

    result = service.query_memory(loaded_manifest, query="какое хранилище выбрали")

    assert [hit["event_id"] for hit in result["results"]] == [target["id"]]
    assert result["degraded"] is False and "vector" in result["channels_used"]
    assert result["vector_coverage"] == {"model_key": QWEN, "eligible": 2, "compatible": 2, "missing_or_incompatible": 0}


def test_a_partial_index_is_disclosed_not_degraded(loaded_manifest, monkeypatch, without_embedding) -> None:
    config = config_from_manifest(loaded_manifest)
    indexed = _record(loaded_manifest, "Indexed decision")
    _record(loaded_manifest, "Not yet indexed")
    store_event_embedding(config, indexed["id"], _unit(0.0, 1024), QWEN, 1024)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(0.0, 1024))

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert result["degraded"] is False
    assert result["vector_coverage"]["missing_or_incompatible"] == 1


def test_an_empty_index_for_the_active_model_is_degraded(loaded_manifest, monkeypatch, without_embedding) -> None:
    config = config_from_manifest(loaded_manifest)
    event = _record(loaded_manifest, "Indexed by the old model only")
    store_event_embedding(config, event["id"], _unit(0.0, 768), NOMIC, 768)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: _unit(0.0, 1024))

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert result["degraded"] is True and result["channel_errors"]["vector"] == "embedding_index_empty"


def test_each_profile_brings_its_own_similarity_floor(loaded_manifest, monkeypatch, without_embedding) -> None:
    config = config_from_manifest(loaded_manifest)
    event = _record(loaded_manifest, "Ordinary project update")
    query = [1.0] + [0.0] * 1023
    stored = [0.55, math.sqrt(1 - 0.55**2)] + [0.0] * 1022  # cosine 0.55 with the query
    store_event_embedding(config, event["id"], stored, QWEN, 1024)
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.delenv("CHRONICLE_VECTOR_MIN_SIMILARITY", raising=False)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: query)

    passes_qwen_floor = service.query_memory(loaded_manifest, query="zzqx")["results"]
    monkeypatch.setenv("CHRONICLE_VECTOR_MIN_SIMILARITY", "0.6")
    fails_explicit_floor = service.query_memory(loaded_manifest, query="zzqx")["results"]

    assert [hit["event_id"] for hit in passes_qwen_floor] == [event["id"]]
    assert fails_explicit_floor == []
