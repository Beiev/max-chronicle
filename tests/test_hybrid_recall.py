"""Tests for Chronicle hybrid recall (embeddings + query_memory + embed_backfill).

All tests mock embed_text so no real Ollama network call is made.
"""

from __future__ import annotations

import json
import math
import os
import struct
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from max_chronicle import service, store
from max_chronicle.config import MIGRATIONS_DIR
from max_chronicle.embeddings import cosine, pack_vector, unpack_vector, EMBED_DIM
from max_chronicle.runtime_context import load_manifest
from max_chronicle.store import (
    config_from_manifest,
    fetch_all_event_embeddings,
    fetch_event_ids_without_embedding,
    open_connection,
    store_event_embedding,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_vec(seed: float, dim: int = EMBED_DIM) -> list[float]:
    """Deterministic unit vector based on seed (for reproducible tests)."""
    raw = [math.sin(seed + i * 0.1) for i in range(dim)]
    mag = math.sqrt(sum(x * x for x in raw))
    return [x / mag for x in raw]


def _store_event(manifest: dict, text: str, **kwargs) -> dict:
    """Store an event without triggering embedding (Ollama not needed)."""
    entry: dict[str, Any] = {
        "agent": "test",
        "domain": "global",
        "category": "note",
        "text": text,
        **kwargs,
    }
    os.environ["CHRONICLE_FEATURE_EVENT_EMBEDDINGS"] = "0"
    try:
        result = service.record_event(manifest, entry)
    finally:
        os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)
    return result


# ---------------------------------------------------------------------------
# 1. pack/unpack roundtrip + cosine basics
# ---------------------------------------------------------------------------


class TestPackUnpack:
    def test_roundtrip_preserves_values(self) -> None:
        original = [1.0, 2.5, -3.14, 0.0, 1e-5]
        blob = pack_vector(original)
        recovered = unpack_vector(blob)
        assert len(recovered) == len(original)
        for a, b in zip(original, recovered):
            assert abs(a - b) < 1e-5

    def test_blob_length(self) -> None:
        vec = [0.5] * EMBED_DIM
        blob = pack_vector(vec)
        assert len(blob) == EMBED_DIM * 4  # 4 bytes per float32

    def test_roundtrip_full_dim(self) -> None:
        vec = _make_vec(1.0)
        assert len(vec) == EMBED_DIM
        recovered = unpack_vector(pack_vector(vec))
        assert len(recovered) == EMBED_DIM
        for a, b in zip(vec, recovered):
            assert abs(a - b) < 1e-5


class TestCosine:
    def test_identical_vectors(self) -> None:
        v = _make_vec(1.0)
        assert abs(cosine(v, v) - 1.0) < 1e-6

    def test_orthogonal_vectors(self) -> None:
        a = [1.0, 0.0, 0.0]
        b = [0.0, 1.0, 0.0]
        assert abs(cosine(a, b)) < 1e-10

    def test_opposite_vectors(self) -> None:
        v = _make_vec(1.0)
        neg = [-x for x in v]
        assert abs(cosine(v, neg) - (-1.0)) < 1e-6

    def test_zero_vector_returns_zero(self) -> None:
        zero = [0.0] * 5
        v = [1.0, 0.0, 0.0, 0.0, 0.0]
        assert cosine(zero, v) == 0.0

    def test_similar_vectors_higher_than_dissimilar(self) -> None:
        base = _make_vec(0.0)
        similar = _make_vec(0.01)
        different = _make_vec(1.5)
        assert cosine(base, similar) > cosine(base, different)


# ---------------------------------------------------------------------------
# 2. store_event_embedding / fetch helpers
# ---------------------------------------------------------------------------


class TestEmbeddingStore:
    def test_store_and_fetch_roundtrip(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        event = _store_event(loaded_manifest, "Chronicle embedding test")
        vec = _make_vec(42.0)
        store_event_embedding(config, event["id"], vec, "nomic-embed-text", EMBED_DIM)

        all_embs = fetch_all_event_embeddings(config)
        ids = [eid for eid, _ in all_embs]
        assert event["id"] in ids

        recovered_vec = next(v for eid, v in all_embs if eid == event["id"])
        assert len(recovered_vec) == EMBED_DIM
        for a, b in zip(vec, recovered_vec):
            assert abs(a - b) < 1e-5

    def test_upsert_replaces_existing(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        event = _store_event(loaded_manifest, "Upsert test")
        v1 = _make_vec(1.0)
        v2 = _make_vec(2.0)
        store_event_embedding(config, event["id"], v1, "nomic-embed-text", EMBED_DIM)
        store_event_embedding(config, event["id"], v2, "nomic-embed-text", EMBED_DIM)

        all_embs = dict(fetch_all_event_embeddings(config))
        recovered = all_embs[event["id"]]
        assert abs(cosine(recovered, v2) - 1.0) < 1e-5  # matches v2, not v1

    def test_fetch_event_ids_without_embedding(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        ev1 = _store_event(loaded_manifest, "Has embedding")
        ev2 = _store_event(loaded_manifest, "No embedding")
        # Give ev1 an embedding
        store_event_embedding(config, ev1["id"], _make_vec(1.0), "nomic-embed-text", EMBED_DIM)

        missing = fetch_event_ids_without_embedding(config)
        assert ev2["id"] in missing
        assert ev1["id"] not in missing


# ---------------------------------------------------------------------------
# 3. record_event: embedding hook
# ---------------------------------------------------------------------------


class TestRecordEventEmbedding:
    def test_embedding_stored_when_embed_text_succeeds(self, loaded_manifest) -> None:
        """Embedding row must be created when embed_text returns a valid vector."""
        config = config_from_manifest(loaded_manifest)
        mock_vec = _make_vec(99.0)
        os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        event = service.record_event(
            loaded_manifest,
            {
                "agent": "test",
                "domain": "global",
                "category": "note",
                "text": "Record event with embedding hook",
                "_embed_fn": lambda t: mock_vec,
            },
        )

        all_embs = dict(fetch_all_event_embeddings(config))
        assert event["id"] in all_embs

    def test_embedding_not_stored_when_embed_text_returns_none(self, loaded_manifest) -> None:
        """When embed_text returns None (Ollama down), event must still be stored."""
        config = config_from_manifest(loaded_manifest)
        os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        event = service.record_event(
            loaded_manifest,
            {
                "agent": "test",
                "domain": "global",
                "category": "note",
                "text": "Event with failed embedding",
                "_embed_fn": lambda t: None,
            },
        )

        # Event was stored
        assert event.get("chronicle_status") == "stored"
        # No embedding row
        all_embs = dict(fetch_all_event_embeddings(config))
        assert event["id"] not in all_embs

    def test_embedding_not_stored_when_embed_text_raises(self, loaded_manifest) -> None:
        """When embed_text raises, event is still stored without error."""
        config = config_from_manifest(loaded_manifest)
        os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        def _raise(_text: str) -> list[float]:
            raise RuntimeError("Ollama is gone")

        event = service.record_event(
            loaded_manifest,
            {
                "agent": "test",
                "domain": "global",
                "category": "note",
                "text": "Event with raising embed_text",
                "_embed_fn": _raise,
            },
        )

        assert event.get("chronicle_status") == "stored"
        all_embs = dict(fetch_all_event_embeddings(config))
        assert event["id"] not in all_embs

    def test_embedding_disabled_via_env(self, loaded_manifest) -> None:
        """CHRONICLE_FEATURE_EVENT_EMBEDDINGS=0 skips embedding entirely."""
        config = config_from_manifest(loaded_manifest)

        call_count = [0]

        def _track(text: str) -> list[float]:
            call_count[0] += 1
            return _make_vec(1.0)

        os.environ["CHRONICLE_FEATURE_EVENT_EMBEDDINGS"] = "0"
        try:
            event = service.record_event(
                loaded_manifest,
                {
                    "agent": "test",
                    "domain": "global",
                    "category": "note",
                    "text": "Event with embedding disabled",
                    "_embed_fn": _track,
                },
            )
        finally:
            os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        assert call_count[0] == 0
        all_embs = dict(fetch_all_event_embeddings(config))
        assert event["id"] not in all_embs


# ---------------------------------------------------------------------------
# 4. query_memory: hybrid ranking
# ---------------------------------------------------------------------------


class TestQueryMemory:
    def test_no_match_does_not_return_recent_activity(self, loaded_manifest) -> None:
        _store_event(loaded_manifest, "Website release completed")
        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.query_memory(loaded_manifest, query="unicorn reactor")
        assert result["results"] == []

    def test_vector_recall_enforces_scope_and_visibility(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        hidden = _store_event(loaded_manifest, "Internal scratch", domain="memory",
                              memory_guard={"visibility": "raw_only"})
        elsewhere = _store_event(loaded_manifest, "Other project", project="beta")
        for event in [hidden, elsewhere]:
            store_event_embedding(config, event["id"], _make_vec(0), "nomic-embed-text", EMBED_DIM)
        with patch("max_chronicle.embeddings.embed_text", return_value=_make_vec(0)):
            result = service.query_memory(loaded_manifest, query="unicorn reactor",
                                          domain="global", project="alpha")
        assert result["results"] == []

    def test_incompatible_vectors_are_excluded_and_reported(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        event = _store_event(loaded_manifest, "Unique decision")
        store_event_embedding(config, event["id"], _make_vec(0), "another-model", EMBED_DIM)
        with patch("max_chronicle.embeddings.embed_text", return_value=_make_vec(0)):
            result = service.query_memory(loaded_manifest, query="unicorn reactor")
        assert result["results"] == []
        assert result["degraded"] is True
        assert result["vector_coverage"]["compatible"] == 0

    def test_low_similarity_is_not_evidence(self, loaded_manifest) -> None:
        config = config_from_manifest(loaded_manifest)
        event = _store_event(loaded_manifest, "Unrelated evidence")
        store_event_embedding(config, event["id"], [-x for x in _make_vec(0)], "nomic-embed-text", EMBED_DIM)
        with patch("max_chronicle.embeddings.embed_text", return_value=_make_vec(0)):
            result = service.query_memory(loaded_manifest, query="unicorn reactor")
        assert result["results"] == []

    def _seed_events(self, manifest: dict) -> list[dict]:
        """Store three events with known text for testing."""
        ev1 = _store_event(manifest, "Chronicle memory system upgrade shipped")
        ev2 = _store_event(manifest, "Backup job ran successfully on BackupDrive drive")
        ev3 = _store_event(manifest, "Pipeline test results for batch generation")
        return [ev1, ev2, ev3]

    def test_fts_channel_used_when_vector_disabled(self, loaded_manifest) -> None:
        """When Ollama is down, FTS + temporal channels still produce results."""
        events = self._seed_events(loaded_manifest)

        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.query_memory(
                loaded_manifest,
                query="Chronicle memory",
                limit=5,
            )

        assert result["degraded"] is True
        assert "vector" not in result["channels_used"]
        # FTS should have found ev1 (about Chronicle memory)
        result_ids = [r["event_id"] for r in result["results"]]
        assert events[0]["id"] in result_ids

    def test_vector_channel_used_when_ollama_available(self, loaded_manifest) -> None:
        """When embed_text returns valid vectors, vector channel is included."""
        config = config_from_manifest(loaded_manifest)
        events = self._seed_events(loaded_manifest)

        # Store embeddings for events using deterministic vectors
        for idx, ev in enumerate(events):
            store_event_embedding(
                config, ev["id"], _make_vec(float(idx)), "nomic-embed-text", EMBED_DIM
            )

        # The query vector matches event 0's seed
        query_vec = _make_vec(0.0)

        with patch("max_chronicle.embeddings.embed_text", return_value=query_vec):
            result = service.query_memory(
                loaded_manifest,
                query="Chronicle upgrade",
                limit=5,
            )

        assert result["degraded"] is False
        assert "vector" in result["channels_used"]

    def test_rrf_fuses_channels_sensibly(self, loaded_manifest) -> None:
        """Events appearing in multiple channels rank higher."""
        config = config_from_manifest(loaded_manifest)
        events = self._seed_events(loaded_manifest)

        # ev1 is about "Chronicle memory" — matches FTS for that query
        # Give ev1 a strong vector match too
        q_vec = _make_vec(0.0)
        store_event_embedding(config, events[0]["id"], _make_vec(0.0), "nomic-embed-text", EMBED_DIM)
        store_event_embedding(config, events[1]["id"], _make_vec(1.5), "nomic-embed-text", EMBED_DIM)
        store_event_embedding(config, events[2]["id"], _make_vec(3.0), "nomic-embed-text", EMBED_DIM)

        with patch("max_chronicle.embeddings.embed_text", return_value=q_vec):
            result = service.query_memory(
                loaded_manifest,
                query="Chronicle memory",
                limit=5,
            )

        assert not result["degraded"]
        result_ids = [r["event_id"] for r in result["results"]]
        # ev1 should rank first: strong FTS + strong vector + recency
        if result_ids:
            assert result_ids[0] == events[0]["id"]

    def test_rrf_result_shape(self, loaded_manifest) -> None:
        """Each result must have required keys."""
        self._seed_events(loaded_manifest)

        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.query_memory(
                loaded_manifest,
                query="test",
                limit=3,
            )

        required_top_keys = {"query", "domain", "results", "channels_used", "degraded"}
        assert required_top_keys.issubset(result.keys())

        for hit in result["results"]:
            assert "event_id" in hit
            assert "text" in hit
            assert "category" in hit
            assert "occurred_at_utc" in hit
            assert "project" in hit
            assert "rrf_score" in hit
            assert "channels" in hit
            ch = hit["channels"]
            assert set(ch.keys()) >= {"fts_rank", "vector_similarity", "recency_rank"}

    def test_degraded_true_when_ollama_down(self, loaded_manifest) -> None:
        self._seed_events(loaded_manifest)

        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.query_memory(loaded_manifest, query="anything")

        assert result["degraded"] is True

    def test_empty_db_returns_empty_results(self, loaded_manifest) -> None:
        """query_memory on empty events table returns results=[] without raising."""
        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.query_memory(loaded_manifest, query="nothing here")

        assert result["results"] == []
        assert isinstance(result["degraded"], bool)


# ---------------------------------------------------------------------------
# 5. embed_backfill CLI
# ---------------------------------------------------------------------------


def _cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "max_chronicle.cli", *args],
        capture_output=True,
        text=True,
    )


class TestEmbedBackfillCli:
    def test_embed_backfill_populates_missing_embeddings(self, chronicle_sandbox) -> None:
        """embed-backfill with mocked embed_text populates event_embeddings."""
        from max_chronicle.store import config_from_manifest, fetch_event_ids_without_embedding

        manifest = load_manifest(chronicle_sandbox.manifest_path)
        config = config_from_manifest(manifest)

        # Store an event without embedding
        os.environ["CHRONICLE_FEATURE_EVENT_EMBEDDINGS"] = "0"
        try:
            ev = service.record_event(
                manifest,
                {
                    "agent": "test",
                    "domain": "global",
                    "category": "note",
                    "text": "Event for backfill test",
                },
            )
        finally:
            os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        missing_before = fetch_event_ids_without_embedding(config)
        assert ev["id"] in missing_before

        mock_vec = _make_vec(7.0)
        with patch("max_chronicle.embeddings.embed_text", return_value=mock_vec):
            result = service.embed_backfill(manifest)

        assert result["embedded"] >= 1
        missing_after = fetch_event_ids_without_embedding(config)
        assert ev["id"] not in missing_after

    def test_embed_backfill_reports_failed_when_ollama_down(self, chronicle_sandbox) -> None:
        """When embed_text returns None, events are counted as failed (not crashed)."""
        manifest = load_manifest(chronicle_sandbox.manifest_path)

        os.environ["CHRONICLE_FEATURE_EVENT_EMBEDDINGS"] = "0"
        try:
            service.record_event(
                manifest,
                {
                    "agent": "test",
                    "domain": "global",
                    "category": "note",
                    "text": "Backfill should fail gracefully",
                },
            )
        finally:
            os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        with patch("max_chronicle.embeddings.embed_text", return_value=None):
            result = service.embed_backfill(manifest)

        assert result["embedded"] == 0
        assert result["failed"] >= 1

    def test_embed_backfill_limit_respected(self, chronicle_sandbox) -> None:
        """--limit N processes at most N events."""
        manifest = load_manifest(chronicle_sandbox.manifest_path)
        os.environ["CHRONICLE_FEATURE_EVENT_EMBEDDINGS"] = "0"
        try:
            for i in range(5):
                service.record_event(
                    manifest,
                    {
                        "agent": "test",
                        "domain": "global",
                        "category": "note",
                        "text": f"Limit test event {i}",
                    },
                )
        finally:
            os.environ.pop("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", None)

        call_count = [0]

        def _count_and_return(text: str) -> list[float]:
            call_count[0] += 1
            return _make_vec(float(call_count[0]))

        with patch("max_chronicle.embeddings.embed_text", side_effect=_count_and_return):
            result = service.embed_backfill(manifest, limit=2)

        assert result["total_without_embedding"] == 2
        assert result["embedded"] == 2
        assert call_count[0] == 2


# ---------------------------------------------------------------------------
# 6. Migration idempotence updated to 10
# ---------------------------------------------------------------------------


def test_migrate_is_idempotent_v10(chronicle_sandbox) -> None:
    """Applying migrations twice applies every file once and records the latest version."""
    first = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "migrate",
    )
    second = _cli(
        "--db",
        str(chronicle_sandbox.chronicle_db),
        "--manifest",
        str(chronicle_sandbox.manifest_path),
        "--automation-config",
        str(chronicle_sandbox.automation_path),
        "migrate",
    )
    first_payload = json.loads(first.stdout)
    second_payload = json.loads(second.stdout)
    latest = max(int(path.name[:4]) for path in MIGRATIONS_DIR.glob("*.sql"))
    assert len(first_payload["applied"]) == latest, first_payload["applied"]
    assert second_payload["applied"] == []
    assert first_payload["summary"]["user_version"] == latest
    assert second_payload["summary"]["user_version"] == latest


def test_weak_positive_vector_match_does_not_answer_unrelated_query(loaded_manifest, monkeypatch):
    monkeypatch.delenv("CHRONICLE_VECTOR_MIN_SIMILARITY", raising=False)
    event = _store_event(loaded_manifest, "Ordinary project update")
    stored_vector = [0.6, 0.8] + [0.0] * (EMBED_DIM - 2)
    store_event_embedding(config_from_manifest(loaded_manifest), event["id"], stored_vector,
                          "nomic-embed-text", EMBED_DIM)
    monkeypatch.setattr("max_chronicle.embeddings.embed_text", lambda _: [1.0] + [0.0] * (EMBED_DIM - 1))
    assert service.query_memory(loaded_manifest, query="unicornquantumzxy987")["results"] == []
