"""Chronicle embeddings — local Ollama-backed text embeddings with pure-Python cosine.

All public functions degrade gracefully when Ollama is unavailable.  NEVER raise
on network or parsing errors — callers treat None as "skip this channel".
"""

from __future__ import annotations

import json
import math
import os
import struct
import urllib.error
import urllib.request
from typing import Any

# ---------------------------------------------------------------------------
# Constants (overridable via env)
# ---------------------------------------------------------------------------

EMBED_MODEL: str = os.environ.get("CHRONICLE_EMBED_MODEL", "nomic-embed-text")
EMBED_DIM: int = 768

_DEFAULT_OLLAMA_URL = "http://localhost:11434"


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
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = math.sqrt(sum(x * x for x in a))
    mag_b = math.sqrt(sum(y * y for y in b))
    if mag_a == 0.0 or mag_b == 0.0:
        return 0.0
    return dot / (mag_a * mag_b)


# ---------------------------------------------------------------------------
# Ollama embedding call
# ---------------------------------------------------------------------------


def embed_text(text: str, *, timeout: float = 3.0) -> list[float] | None:
    """Embed *text* via Ollama and return a 768-dim float list.

    Returns None on ANY failure (connection error, timeout, bad response).
    NEVER raises.

    The Ollama base URL and model are read from OLLAMA_URL and
    CHRONICLE_EMBED_MODEL env vars with defaults ``http://localhost:11434``
    and ``nomic-embed-text``.
    """
    try:
        url = f"{_ollama_base_url()}/api/embeddings"
        payload: dict[str, Any] = {
            "model": EMBED_MODEL,
            "prompt": text,
        }
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
        parsed = json.loads(body)
        embedding = parsed.get("embedding")
        if not isinstance(embedding, list) or len(embedding) == 0:
            return None
        return [float(x) for x in embedding]
    except Exception:  # noqa: BLE001 — intentional blanket catch
        return None
