"""Chronicle embeddings — local Ollama-backed text embeddings with pure-Python cosine.

All public functions degrade gracefully when Ollama is unavailable.  NEVER raise
on network or parsing errors — callers treat None as "skip this channel".

Each vector is stored under the key of the profile that produced it. Recall
compares a query only with vectors of the active profile, so an index built by
one model is never ranked against another model's query, and a new model can be
backfilled while the old index still serves.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import math
import os
import struct
import urllib.error
import urllib.request
from typing import Any

# ---------------------------------------------------------------------------
# Profiles
# ---------------------------------------------------------------------------

ENV_EMBED_MODEL = "CHRONICLE_EMBED_MODEL"
DEFAULT_EMBED_MODEL = "qwen3-embedding:0.6b"
DOCUMENT_TIMEOUT_S = 3.0  # on the write path, outside the write lock
BACKFILL_TIMEOUT_S = 30.0  # a long event on a cold model
FLOAT32_MAX = 3.4028234663852886e38  # vectors are stored as float32
DIMENSION_PROBE_TEXT = "dimension probe"  # embedded once per backfill to learn the model's dimension

_DEFAULT_OLLAMA_URL = "http://localhost:11434"


@dataclass(frozen=True)
class EmbeddingProfile:
    """How one model embeds documents and queries."""

    key: str  # stored with every vector; change it when stored vectors stop being comparable
    model: str  # Ollama model tag
    query_prefix: str = ""
    document_prefix: str = ""
    # Cosine floor for a candidate found by the vector channel alone. Similarity
    # scales differ by model, so each profile carries its own.
    min_similarity: float = 0.65
    # Cosine at which a vector match is confident evidence (FR-2), or None when
    # this model's similarity does not tell relevant from unrelated text.
    confident_similarity: float | None = None


# Qwen3-Embedding is instruction-aware on the query side; documents are raw.
_QWEN3_QUERY = (
    "Instruct: Given a question about past work, decisions and events, "
    "retrieve the memory entries that answer it\nQuery:"
)
# Floors were measured with `chronicle eval` on 64 real questions (54 answerable,
# 10 that memory cannot answer). qwen3-embedding:0.6b: ranking peaks at a 0.45-0.50
# floor; 0.60 flags 8 of 10 unanswerable questions and 15 of 54 answerable ones
# as unconfident. nomic-embed-text: its top similarity does not separate the two
# groups at all (AUC 0.50), so only a lexical match makes it confident.
PROFILES = {
    profile.model: profile
    for profile in (
        # Multilingual (Russian, Ukrainian, English), 32K context, 1024 dimensions.
        EmbeddingProfile(key="qwen3-embedding:0.6b", model="qwen3-embedding:0.6b", query_prefix=_QWEN3_QUERY,
                         min_similarity=0.5, confident_similarity=0.6),
        # Not yet measured: calibrate both floors with `chronicle eval` before relying on it.
        EmbeddingProfile(key="qwen3-embedding:4b", model="qwen3-embedding:4b", query_prefix=_QWEN3_QUERY,
                         min_similarity=0.5),
        # English only; kept so an existing index stays usable. Its vectors were
        # stored without the search_document prefix, so none is added now.
        EmbeddingProfile(key="nomic-embed-text", model="nomic-embed-text", min_similarity=0.65),
    )
}


def active_profile() -> EmbeddingProfile:
    """The profile named by CHRONICLE_EMBED_MODEL, or a plain one for an unknown model."""
    model = os.environ.get(ENV_EMBED_MODEL, "").strip() or DEFAULT_EMBED_MODEL
    return PROFILES.get(model) or EmbeddingProfile(key=model, model=model)


def _ollama_base_url() -> str:
    return os.environ.get("OLLAMA_URL", _DEFAULT_OLLAMA_URL).rstrip("/")


# ---------------------------------------------------------------------------
# Vector utilities
# ---------------------------------------------------------------------------


def pack_vector(vec: list[float]) -> bytes:
    """Pack a list of float32 values into a raw BLOB."""
    return struct.pack(f"{len(vec)}f", *vec)


def unpack_vector(blob: bytes) -> list[float]:
    """Unpack a raw BLOB back to a list of float32 values."""
    n = len(blob) // 4
    return list(struct.unpack(f"{n}f", blob))


_sumprod = getattr(math, "sumprod", None)  # C speed on Python 3.12+


def _dot(a: list[float], b: list[float]) -> float:
    return _sumprod(a, b) if _sumprod is not None else sum(x * y for x, y in zip(a, b))


def similarity_scorer(query: list[float]) -> Callable[[list[float]], float]:
    """Cosine similarity to *query* for many vectors, the query's norm computed once.

    A vector of another dimension or with a non-finite value raises ValueError;
    a zero vector scores 0.0.
    """
    query_norm = math.sqrt(_dot(query, query))
    if not math.isfinite(query_norm):
        raise ValueError("Embedding contains non-finite values")

    def score(vector: list[float]) -> float:
        if len(vector) != len(query):
            raise ValueError("Embedding dimensions do not match")
        dot = _dot(query, vector)
        norm = math.sqrt(_dot(vector, vector))
        if not (math.isfinite(dot) and math.isfinite(norm)):
            raise ValueError("Embedding contains non-finite values")
        if norm == 0.0 or query_norm == 0.0:
            return 0.0
        return dot / (query_norm * norm)

    return score


def cosine(a: list[float], b: list[float]) -> float:
    """Return the cosine similarity between two equal-length vectors.

    Returns 0.0 when either vector is zero-length (degenerate embedding).
    """
    return similarity_scorer(a)(b)


# ---------------------------------------------------------------------------
# Ollama embedding call
# ---------------------------------------------------------------------------


def event_embedding_text(event: dict[str, Any]) -> str:
    parts = [event.get(key) for key in ("project", "text", "why")]
    if event.get("fact"):
        parts.extend(event["fact"].get(key) for key in ("slot", "value"))
    if event.get("checkpoint"):
        parts.append(event["checkpoint"].get("goal"))
    return " ".join(part for part in parts if part).strip()


def _request_embeddings(inputs: str | list[str], *, timeout: float) -> list[list[float]] | None:
    """One Ollama /api/embed call for a text or a list; None unless every vector is usable. NEVER raises."""
    expected = 1 if isinstance(inputs, str) else len(inputs)
    try:
        payload = {"model": active_profile().model, "input": inputs, "truncate": True}
        req = urllib.request.Request(
            f"{_ollama_base_url()}/api/embed",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
        embeddings = json.loads(body).get("embeddings")
        if not isinstance(embeddings, list) or len(embeddings) != expected:
            return None
        vectors = []
        for item in embeddings:
            if not isinstance(item, list):
                return None
            vector = [float(x) for x in item]
            if not vector or not all(math.isfinite(x) for x in vector) or not any(vector):
                return None
            vectors.append(vector)
        return vectors
    except Exception:  # noqa: BLE001 — intentional blanket catch
        return None


def embed_text(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed *text* as given with the active profile's model.

    Uses Ollama's /api/embed with truncation, so a text longer than the model's
    context is embedded from its start instead of failing. The dimension is the
    model's own. Returns None on ANY failure (connection error, timeout, bad
    response). NEVER raises.
    """
    vectors = _request_embeddings(text, timeout=timeout)
    return vectors[0] if vectors else None


def embed_documents(texts: list[str], *, timeout: float = BACKFILL_TIMEOUT_S) -> list[list[float]] | None:
    """Embed stored texts (note chunks) in one call; None on any failure. NEVER raises."""
    if not texts:
        return []
    return _request_embeddings([active_profile().document_prefix + text for text in texts], timeout=timeout)


def embed_query(query: str) -> list[float] | None:
    """Embed a search query the way the active profile expects."""
    return embed_text(active_profile().query_prefix + query)


def embed_document(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed stored text (an event) the way the active profile expects."""
    return embed_text(active_profile().document_prefix + text, timeout=timeout)
