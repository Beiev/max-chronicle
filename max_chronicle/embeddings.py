"""Chronicle embeddings — local Ollama-backed text embeddings with pure-Python cosine.

All public functions degrade gracefully when Ollama is unavailable.  NEVER raise
on network or parsing errors — callers treat None as "skip this channel".

Each vector is stored under the key of the profile that produced it. Recall
compares a query only with vectors of the active profile, so an index built by
one model is never ranked against another model's query, and a new model can be
backfilled while the old index still serves.
"""

from __future__ import annotations

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


# Qwen3-Embedding is instruction-aware on the query side; documents are raw.
_QWEN3_QUERY = (
    "Instruct: Given a question about past work, decisions and events, "
    "retrieve the memory entries that answer it\nQuery:"
)
PROFILES = {
    profile.model: profile
    for profile in (
        # Multilingual (Russian, Ukrainian, English), 32K context, 1024 dimensions.
        EmbeddingProfile(key="qwen3-embedding:0.6b", model="qwen3-embedding:0.6b", query_prefix=_QWEN3_QUERY,
                         min_similarity=0.5),
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


def cosine(a: list[float], b: list[float]) -> float:
    """Return the cosine similarity between two equal-length vectors.

    Returns 0.0 when either vector is zero-length (degenerate embedding).
    """
    if len(a) != len(b):
        raise ValueError("Embedding dimensions do not match")
    if not all(math.isfinite(x) for x in (*a, *b)):
        raise ValueError("Embedding contains non-finite values")
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(y * y for y in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


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


def embed_text(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed *text* as given with the active profile's model.

    Uses Ollama's /api/embed with truncation, so a text longer than the model's
    context is embedded from its start instead of failing. The dimension is the
    model's own. Returns None on ANY failure (connection error, timeout, bad
    response). NEVER raises.
    """
    try:
        payload = {"model": active_profile().model, "input": text, "truncate": True}
        req = urllib.request.Request(
            f"{_ollama_base_url()}/api/embed",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
        embeddings = json.loads(body).get("embeddings")
        if not isinstance(embeddings, list) or not embeddings or not isinstance(embeddings[0], list):
            return None
        vector = [float(x) for x in embeddings[0]]
        if not vector or not all(math.isfinite(x) for x in vector) or not any(vector):
            return None
        return vector
    except Exception:  # noqa: BLE001 — intentional blanket catch
        return None


def embed_query(query: str) -> list[float] | None:
    """Embed a search query the way the active profile expects."""
    return embed_text(active_profile().query_prefix + query)


def embed_document(text: str, *, timeout: float = DOCUMENT_TIMEOUT_S) -> list[float] | None:
    """Embed stored text (an event) the way the active profile expects."""
    return embed_text(active_profile().document_prefix + text, timeout=timeout)
