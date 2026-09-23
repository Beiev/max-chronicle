"""Recall ranks by relevance, lets recency break ties, and says when nothing is a confident match."""

from __future__ import annotations

import math

import pytest

from max_chronicle import embeddings, service
from max_chronicle.evals import GoldenCase, run_eval, summarize
from max_chronicle.recall import NO_CONFIDENT_MATCH_HINT
from max_chronicle.store import config_from_manifest, store_event_embedding

QWEN = "qwen3-embedding:0.6b"
NOMIC = "nomic-embed-text"
DIM = 8
QUERY = [1.0] + [0.0] * (DIM - 1)


def _at_cosine(similarity: float) -> list[float]:
    """A unit vector whose cosine with QUERY is exactly *similarity*."""
    return [similarity, math.sqrt(1 - similarity**2)] + [0.0] * (DIM - 2)


@pytest.fixture()
def qwen(monkeypatch):
    """Vectors are stored by hand; every query embeds to QUERY under the Qwen3 profile."""
    monkeypatch.setenv("CHRONICLE_FEATURE_EVENT_EMBEDDINGS", "0")
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", QWEN)
    monkeypatch.delenv("CHRONICLE_VECTOR_MIN_SIMILARITY", raising=False)
    monkeypatch.delenv("CHRONICLE_VECTOR_CONFIDENT_SIMILARITY", raising=False)
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: QUERY)


def _record(manifest: dict, text: str, *, at: str, similarity: float | None = None, key: str = QWEN) -> str:
    event = service.record_event(
        manifest, {"agent": "agent-a", "domain": "global", "text": text, "recorded_at": at}
    )
    if similarity is not None:
        store_event_embedding(config_from_manifest(manifest), event["id"], _at_cosine(similarity), key, DIM)
    return event["id"]


def _ids(result: dict) -> list[str]:
    return [hit["event_id"] for hit in result["results"]]


# ---------------------------------------------------------------------------
# Recency breaks ties only (FR-1)
# ---------------------------------------------------------------------------


def test_a_more_relevant_older_event_outranks_a_newer_one(loaded_manifest, qwen) -> None:
    older = _record(loaded_manifest, "Deploy target chosen", at="2026-07-01T10:00:00Z", similarity=0.9)
    newer = _record(loaded_manifest, "Unrelated update", at="2026-09-01T10:00:00Z", similarity=0.7)

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert _ids(result) == [older, newer]


# Five events, so that random event ids agree with their age by chance only once in 120 runs.
MONTHS = ("05", "06", "07", "08", "09")


def test_recency_orders_equally_relevant_events(loaded_manifest, qwen) -> None:
    oldest_first = [
        _record(loaded_manifest, f"Note {month}", at=f"2026-{month}-01T10:00:00Z", similarity=0.8) for month in MONTHS
    ]

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert _ids(result) == oldest_first[::-1]


def test_a_lexical_tie_ranks_the_newer_event_first(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: None)
    oldest_first = [
        _record(loaded_manifest, "Deploy target is the staging cluster", at=f"2026-{month}-01T10:00:00Z")
        for month in MONTHS
    ]

    result = service.query_memory(loaded_manifest, query="staging cluster")

    assert _ids(result) == oldest_first[::-1]


# ---------------------------------------------------------------------------
# no_confident_match (FR-2)
# ---------------------------------------------------------------------------


def test_a_match_on_every_term_is_confident_without_vectors(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: None)
    event = _record(loaded_manifest, "Выбрали FastAPI для внутреннего API вместо Flask.", at="2026-07-01T10:00:00Z")

    result = service.query_memory(loaded_manifest, query="внутреннего API")

    assert _ids(result) == [event]
    assert result["no_confident_match"] is False and "hint" not in result
    assert result["degraded"] is True  # the vector channel was skipped, and says so


def test_a_relaxed_match_alone_is_not_confident(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: None)
    event = _record(loaded_manifest, "Выбрали FastAPI для внутреннего API вместо Flask.", at="2026-07-01T10:00:00Z")

    result = service.query_memory(loaded_manifest, query="какой фреймворк выбрали для внутреннего API")

    assert _ids(result) == [event] and result["relaxed"] is True
    assert result["no_confident_match"] is True and result["hint"] == NO_CONFIDENT_MATCH_HINT


@pytest.mark.parametrize(
    ("similarity", "confident"),
    [(0.65, True), (0.55, False)],
    ids=["above", "between-floors"],
)
def test_a_vector_match_is_confident_from_the_profile_floor(loaded_manifest, qwen, similarity, confident) -> None:
    event = _record(loaded_manifest, "Some decision", at="2026-07-01T10:00:00Z", similarity=similarity)

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert _ids(result) == [event]  # returned either way: a weak match is still a lead
    assert result["no_confident_match"] is (not confident)
    assert result["similarity_floors"] == {"min": 0.5, "confident": 0.6}


def test_an_empty_result_is_not_a_confident_match(loaded_manifest, qwen) -> None:
    _record(loaded_manifest, "Some decision", at="2026-07-01T10:00:00Z", similarity=0.3)

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert result["results"] == [] and result["no_confident_match"] is True


def test_a_profile_without_a_confident_floor_needs_a_lexical_match(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setenv("CHRONICLE_EMBED_MODEL", NOMIC)
    event = _record(loaded_manifest, "Some decision", at="2026-07-01T10:00:00Z", similarity=0.95, key=NOMIC)

    result = service.query_memory(loaded_manifest, query="zzqx")

    assert _ids(result) == [event] and result["no_confident_match"] is True
    assert result["similarity_floors"] == {"min": 0.65, "confident": None}


def test_the_environment_overrides_the_confident_floor(loaded_manifest, qwen, monkeypatch) -> None:
    _record(loaded_manifest, "Some decision", at="2026-07-01T10:00:00Z", similarity=0.55)
    monkeypatch.setenv("CHRONICLE_VECTOR_CONFIDENT_SIMILARITY", "0.5")

    assert service.query_memory(loaded_manifest, query="zzqx")["no_confident_match"] is False

    monkeypatch.setenv("CHRONICLE_VECTOR_CONFIDENT_SIMILARITY", "1.5")
    with pytest.raises(ValueError, match="CHRONICLE_VECTOR_CONFIDENT_SIMILARITY"):
        service.query_memory(loaded_manifest, query="zzqx")


def test_every_term_counts_even_below_the_capped_full_text_list(loaded_manifest, qwen) -> None:
    for n in range(40):  # short, so BM25 ranks them all above the long one
        _record(loaded_manifest, f"rollout canary {n}", at=f"2026-07-01T10:{n:02d}:00Z")
    long_one = _record(loaded_manifest, "The rollout went to one canary region first " + "and waited " * 20,
                       at="2026-09-01T10:00:00Z", similarity=0.55)

    result = service.query_memory(loaded_manifest, query="rollout canary", limit=1)

    assert _ids(result) == [long_one] and result["results"][0]["channels"]["fts_rank"] is None
    assert result["no_confident_match"] is False


def test_a_similarity_below_the_admission_floor_still_counts_toward_confidence(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: QUERY)
    event = _record(loaded_manifest, "Выбрали FastAPI для внутреннего API вместо Flask.", at="2026-07-01T10:00:00Z",
                    similarity=0.7)
    _record(loaded_manifest, "Unrelated, found by vector only", at="2026-07-02T10:00:00Z", similarity=0.7)
    monkeypatch.setenv("CHRONICLE_VECTOR_MIN_SIMILARITY", "0.8")

    result = service.query_memory(loaded_manifest, query="какой фреймворк выбрали для внутреннего API")

    assert _ids(result) == [event] and result["relaxed"] is True  # the vector-only event stays out
    assert result["results"][0]["channels"]["vector_similarity"] == pytest.approx(0.7, abs=1e-6)
    assert result["no_confident_match"] is False  # 0.7 reaches the confident floor of 0.6


def test_a_tie_between_facts_ranks_the_newer_event_first(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: None)
    oldest_first = [
        service.record_event(loaded_manifest, {
            "agent": "agent-a", "domain": "global", "text": "Store chosen", "project": "alpha", "task_id": f"t{month}",
            "recorded_at": f"2026-{month}-01T10:00:00Z",
            "fact": {"slot": "store", "value": "Postgres", "kind": "decision"},
        })["id"]
        for month in MONTHS
    ]

    result = service.query_memory(loaded_manifest, query="Postgres", project="alpha")

    assert _ids(result) == oldest_first[::-1]


def test_a_focused_startup_reports_the_recall_signals(loaded_manifest, qwen, monkeypatch) -> None:
    monkeypatch.setattr(embeddings, "embed_text", lambda text, **options: None)
    _record(loaded_manifest, "Выбрали FastAPI для внутреннего API вместо Flask.", at="2026-07-01T10:00:00Z")

    bundle = service.build_startup_bundle(loaded_manifest, focus="какой фреймворк выбрали для внутреннего API")

    status = bundle["recall_status"]
    assert status["relaxed"] is True and status["no_confident_match"] is True
    assert status["hint"] == NO_CONFIDENT_MATCH_HINT


# ---------------------------------------------------------------------------
# Eval: the cost of abstaining is measured, not hidden
# ---------------------------------------------------------------------------


def test_eval_counts_answerable_questions_called_unconfident() -> None:
    def recall(manifest, *, query, limit, **scope):
        return {"results": [{"event_id": "a"}], "no_confident_match": query == "weak"}

    cases = [
        GoldenCase(id="weak", query="weak", category="fact", expected=("event:a",)),
        GoldenCase(id="strong", query="strong", category="fact", expected=("event:a",)),
    ]

    metrics = summarize(run_eval({}, cases, recall=recall))

    assert metrics["hit@1"] == 1.0  # a flagged result still counts as found
    assert metrics["abstained_on_answerable"] == 0.5
